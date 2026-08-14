<?php

use Illuminate\Support\Facades\Cache;
use Keepsuit\LaravelOpenTelemetry\Instrumentation\HorizonInstrumentation;
use Laravel\Horizon\Contracts\JobRepository;
use Laravel\Horizon\Contracts\MasterSupervisorRepository;
use Laravel\Horizon\Contracts\MetricsRepository;
use Laravel\Horizon\Contracts\WorkloadRepository;
use OpenTelemetry\SDK\Metrics\Data\Gauge;
use OpenTelemetry\SDK\Metrics\Data\Metric;

/**
 * Horizon's repositories are bound to fakes: what is under test is the mapping from Horizon's shape
 * to metric points and the single-writer election, not Horizon's own Redis behaviour.
 *
 * The lock store is `redis`, because the whole point of the election is that it is shared between
 * processes — an `array` store would elect every one of them.
 */
beforeEach(function () {
    config()->set('cache.default', 'redis');
    Cache::store('redis')->lock(HorizonInstrumentation::LOCK_NAME)->forceRelease();
});

function fakeHorizon(array $queues = [], int $perMinute = 0, int $recent = 0, int $failed = 0, array $masters = []): void
{
    app()->bind(WorkloadRepository::class, fn () => new class($queues)
    {
        public function __construct(protected array $queues) {}

        public function get(): array
        {
            return $this->queues;
        }
    });

    app()->bind(MetricsRepository::class, fn () => new class($perMinute)
    {
        public function __construct(protected int $perMinute) {}

        public function jobsProcessedPerMinute(): int
        {
            return $this->perMinute;
        }
    });

    app()->bind(JobRepository::class, fn () => new class($recent, $failed)
    {
        public function __construct(protected int $recent, protected int $failed) {}

        public function countRecent(): int
        {
            return $this->recent;
        }

        public function countRecentlyFailed(): int
        {
            return $this->failed;
        }
    });

    app()->bind(MasterSupervisorRepository::class, fn () => new class($masters)
    {
        public function __construct(protected array $masters) {}

        public function all(): array
        {
            return $this->masters;
        }
    });
}

function horizonGauge(string $name): ?Metric
{
    return getRecordedMetrics()->firstWhere('name', $name);
}

it('records queue depth, wait time and processes per queue', function () {
    fakeHorizon(queues: [
        ['name' => 'default', 'length' => 12, 'wait' => 4.5, 'processes' => 3],
        ['name' => 'emails', 'length' => 0, 'wait' => 0, 'processes' => 1],
    ]);
    registerInstrumentation(HorizonInstrumentation::class);

    $length = horizonGauge(HorizonInstrumentation::METRIC_QUEUE_LENGTH);

    expect($length)->toBeInstanceOf(Metric::class)
        ->and($length->data)->toBeInstanceOf(Gauge::class);

    $byQueue = collect($length->data->dataPoints)
        ->mapWithKeys(fn ($point) => [$point->attributes->get('queue') => $point->value]);

    expect($byQueue['default'])->toBe(12)
        // An empty queue must still report 0 rather than vanish: an absent series and an empty
        // queue look the same on a dashboard, and only one of them is fine.
        ->and($byQueue['emails'])->toBe(0);

    expect(horizonGauge(HorizonInstrumentation::METRIC_QUEUE_WAIT)->data->dataPoints)->toHaveCount(2)
        ->and(horizonGauge(HorizonInstrumentation::METRIC_QUEUE_PROCESSES)->data->dataPoints)->toHaveCount(2);
});

it('reports one series per queue rather than one per supervisor', function () {
    // Horizon names supervisor-scoped queues `queue:supervisor`. Keeping the supervisor in the label
    // would split one queue's depth across however many supervisors serve it, so a queue served by
    // two would read as two half-depth queues.
    fakeHorizon(queues: [
        ['name' => 'default:supervisor-1', 'length' => 5, 'wait' => 1, 'processes' => 2],
        ['name' => 'default:supervisor-2', 'length' => 7, 'wait' => 2, 'processes' => 2],
    ]);
    registerInstrumentation(HorizonInstrumentation::class);

    $queues = collect(horizonGauge(HorizonInstrumentation::METRIC_QUEUE_LENGTH)->data->dataPoints)
        ->map(fn ($point) => $point->attributes->get('queue'))
        ->unique()
        ->values();

    expect($queues->all())->toBe(['default']);
});

it('records throughput, failures and supervisors', function () {
    fakeHorizon(perMinute: 42, recent: 100, failed: 7, masters: [(object) ['status' => 'running']]);
    registerInstrumentation(HorizonInstrumentation::class);

    expect(horizonGauge(HorizonInstrumentation::METRIC_JOBS_PER_MINUTE)->data->dataPoints[0]->value)->toBe(42)
        ->and(horizonGauge(HorizonInstrumentation::METRIC_JOBS_RECENT)->data->dataPoints[0]->value)->toBe(100)
        ->and(horizonGauge(HorizonInstrumentation::METRIC_JOBS_FAILED_RECENT)->data->dataPoints[0]->value)->toBe(7)
        ->and(horizonGauge(HorizonInstrumentation::METRIC_SUPERVISORS)->data->dataPoints[0]->value)->toBe(1);
});

it('publishes nothing while another process holds the election lock', function () {
    // THE POINT OF THE ELECTION. Every worker registers this callback, so without the lock each one
    // would publish an identical copy of every fleet-wide number and run its own Redis workload
    // scan. A process that loses must contribute no data points at all — not zeros.
    fakeHorizon(queues: [['name' => 'default', 'length' => 3, 'wait' => 0, 'processes' => 1]]);

    Cache::store('redis')->lock(HorizonInstrumentation::LOCK_NAME, 60)->get();

    registerInstrumentation(HorizonInstrumentation::class);

    // The INSTRUMENT is still exported — it was registered — so what must be empty is its data
    // points. Asserting the metric itself is absent would pass only by accident of SDK behaviour.
    $metric = horizonGauge(HorizonInstrumentation::METRIC_QUEUE_LENGTH);

    expect($metric?->data->dataPoints ?? [])->toBe([]);
});

it('does not read horizon at all when it loses the election', function () {
    // Losing must be cheap: the repositories are the expensive part (a Redis scan per queue), so a
    // fleet of workers that all lose must not each pay for a reading they will discard.
    fakeHorizon();

    $reads = new class
    {
        public int $count = 0;
    };

    app()->bind(WorkloadRepository::class, fn () => new class($reads)
    {
        public function __construct(protected object $reads) {}

        public function get(): array
        {
            $this->reads->count++;

            return [];
        }
    });

    Cache::store('redis')->lock(HorizonInstrumentation::LOCK_NAME, 60)->get();

    registerInstrumentation(HorizonInstrumentation::class);
    getRecordedMetrics();

    expect($reads->count)->toBe(0);
});

it('registers nothing when horizon is not installed', function () {
    // The package must stay usable in applications without Horizon; the instrumentation is opt-in
    // but a stray `true` in config should degrade rather than fatal on a missing interface.
    expect(fn () => registerInstrumentation(HorizonInstrumentation::class))->not->toThrow(Throwable::class);
})->skip(interface_exists(WorkloadRepository::class), 'Horizon is installed in the test suite.');
