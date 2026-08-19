<?php

use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Redis;
use Keepsuit\LaravelOpenTelemetry\Instrumentation\HorizonInstrumentation;
use Keepsuit\LaravelOpenTelemetry\Instrumentation\Support\Horizon\JobMemoryStore;
use OpenTelemetry\SDK\Metrics\Data\Metric;

beforeEach(function () {
    config()->set('cache.default', 'redis');
    Cache::store('redis')->lock(HorizonInstrumentation::LOCK_NAME)->forceRelease();

    Redis::connection()->del(JobMemoryStore::RUNNING_KEY);

    foreach (Redis::connection()->keys(JobMemoryStore::PREFIX.'*') as $key) {
        Redis::connection()->del($key);
    }
});

function fakeJob(string $name, string $queue, string $id): object
{
    return new class($name, $queue, $id)
    {
        public function __construct(
            protected string $name,
            protected string $queue,
            protected string $id,
        ) {}

        public function resolveName(): string
        {
            return $this->name;
        }

        public function getQueue(): string
        {
            return $this->queue;
        }

        public function getJobId(): string
        {
            return $this->id;
        }

        public function payload(): array
        {
            return ['displayName' => $this->name, 'uuid' => $this->id];
        }
    };
}

function runFakeJob(string $name, string $queue, string $id = 'j1', bool $failed = false): void
{
    $job = fakeJob($name, $queue, $id);

    event(new JobProcessing('redis', $job));

    $keepAllocated = str_repeat('x', 2 * 1024 * 1024);

    $failed
        ? event(new JobFailed('redis', $job, new RuntimeException('boom')))
        : event(new JobProcessed('redis', $job));

    unset($keepAllocated);
}

function memoryGauge(string $name): ?Metric
{
    return getRecordedMetrics()->firstWhere('name', $name);
}

function pointsByJob(?Metric $metric): array
{
    return collect($metric?->data->dataPoints ?? [])
        ->mapWithKeys(fn ($point) => [
            $point->attributes->get('queue').'|'.$point->attributes->get('job_name') => $point->value,
        ])
        ->all();
}

it('records peak memory, added memory and a completion count per job and queue', function () {
    registerInstrumentation(HorizonInstrumentation::class);

    runFakeJob('App\\Jobs\\Import', 'import-older');
    runFakeJob('App\\Jobs\\Import', 'import-older', 'j2');

    $store = new JobMemoryStore;
    $rows = $store->readCompletedBucket(HorizonInstrumentation::DEFAULT_INTERVAL);

    $current = invade($store)->bucketKey(HorizonInstrumentation::DEFAULT_INTERVAL, 0);
    $raw = Redis::connection()->hgetall($current);

    expect((int) $raw['import-older|App\\Jobs\\Import|count'])->toBe(2)
        ->and((int) $raw['import-older|App\\Jobs\\Import|max_peak'])->toBeGreaterThan(0)
        ->and((int) $raw['import-older|App\\Jobs\\Import|sum_peak'])
        ->toBeGreaterThanOrEqual((int) $raw['import-older|App\\Jobs\\Import|max_peak']);
});

it('counts a failed job too', function () {
    registerInstrumentation(HorizonInstrumentation::class);

    runFakeJob('App\\Jobs\\Flaky', 'live', 'f1', failed: true);

    $store = new JobMemoryStore;
    $raw = Redis::connection()->hgetall(invade($store)->bucketKey(HorizonInstrumentation::DEFAULT_INTERVAL, 0));

    expect((int) $raw['live|App\\Jobs\\Flaky|count'])->toBe(1);
});

it('tracks in-flight jobs and releases them on completion', function () {
    registerInstrumentation(HorizonInstrumentation::class);

    $job = fakeJob('App\\Jobs\\Slow', 'live', 's1');
    event(new JobProcessing('redis', $job));

    $store = new JobMemoryStore;
    $running = collect($store->readRunning())->firstWhere('job', 'App\\Jobs\\Slow');
    expect($running['running'])->toBe(1);

    event(new JobProcessed('redis', $job));

    $running = collect($store->readRunning())->firstWhere('job', 'App\\Jobs\\Slow');
    expect($running['running'])->toBe(0);
});

it('never reports a negative in-flight count', function () {
    Redis::connection()->hset(JobMemoryStore::RUNNING_KEY, 'live|App\\Jobs\\Ghost', -3);

    $running = collect((new JobMemoryStore)->readRunning())->firstWhere('job', 'App\\Jobs\\Ghost');

    expect($running['running'])->toBe(0);
});

it('publishes averages and the max from the completed bucket', function () {

    $store = new JobMemoryStore;
    $previous = invade($store)->bucketKey(HorizonInstrumentation::DEFAULT_INTERVAL, -1);

    Redis::connection()->hmset($previous, [
        'import-older|App\\Jobs\\Import|count' => 4,
        'import-older|App\\Jobs\\Import|sum_peak' => 400,
        'import-older|App\\Jobs\\Import|max_peak' => 250,
        'import-older|App\\Jobs\\Import|sum_added' => 80,
    ]);

    registerInstrumentation(HorizonInstrumentation::class);

    expect(pointsByJob(memoryGauge(HorizonInstrumentation::METRIC_JOB_MEMORY_PEAK_AVG)))
        ->toBe(['import-older|App\\Jobs\\Import' => 100])
        ->and(pointsByJob(memoryGauge(HorizonInstrumentation::METRIC_JOB_MEMORY_PEAK_MAX)))
        ->toBe(['import-older|App\\Jobs\\Import' => 250])
        ->and(pointsByJob(memoryGauge(HorizonInstrumentation::METRIC_JOB_MEMORY_ADDED_AVG)))
        ->toBe(['import-older|App\\Jobs\\Import' => 20])
        ->and(pointsByJob(memoryGauge(HorizonInstrumentation::METRIC_JOB_PROCESSED)))
        ->toBe(['import-older|App\\Jobs\\Import' => 4]);
});

it('publishes nothing for a window in which no job ran', function () {
    registerInstrumentation(HorizonInstrumentation::class);

    expect(memoryGauge(HorizonInstrumentation::METRIC_JOB_MEMORY_PEAK_AVG)->data->dataPoints)->toBeEmpty();
});

it('labels the queue so it joins with the fleet metrics', function () {
    expect(JobMemoryStore::normalizeQueue('queues:import-older'))->toBe('import-older')
        ->and(JobMemoryStore::normalizeQueue('import-older:supervisor-1'))->toBe('import-older')
        ->and(JobMemoryStore::normalizeQueue('queues:live:supervisor-2'))->toBe('live')
        ->and(JobMemoryStore::normalizeQueue(null))->toBe('default')
        ->and(JobMemoryStore::normalizeQueue(''))->toBe('default');
});
