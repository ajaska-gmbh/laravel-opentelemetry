<?php

namespace Keepsuit\LaravelOpenTelemetry\Instrumentation;

use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Str;
use Keepsuit\LaravelOpenTelemetry\Facades\Meter;
use Keepsuit\LaravelOpenTelemetry\Instrumentation\Support\Horizon\JobMemoryStore;
use Laravel\Horizon\Contracts\JobRepository;
use Laravel\Horizon\Contracts\MasterSupervisorRepository;
use Laravel\Horizon\Contracts\MetricsRepository;
use Laravel\Horizon\Contracts\WorkloadRepository;
use OpenTelemetry\API\Metrics\ObservableCallbackInterface;
use OpenTelemetry\API\Metrics\ObserverInterface;

/**
 * Fleet-level metrics for Laravel Horizon: how deep the queues are, how long work waits, and how
 * much is flowing through.
 *
 * WHY THIS IS NOT COVERED BY {@see QueueInstrumentation}. That one measures jobs as they pass
 * through a worker — spans, and counters. None of it can answer "how many jobs are waiting", because
 * queue depth is a property of the queue at an instant, not of any job that ran. A queue that has
 * stopped being consumed emits no job events at all, so from that signal alone a wedged fleet is
 * indistinguishable from an idle one. That is precisely the case an on-call dashboard exists to show.
 *
 * ONE WRITER, ELECTED BY A LOCK. These values are fleet-wide: emitting them from every worker
 * process would publish one identical copy of each number per process and run a Redis workload scan
 * per process per collection. So the measurement lives in a single OBSERVABLE batch callback that
 * first tries to take a short lock; the process that wins takes the reading, everyone else observes
 * nothing and contributes no data points. The lock is never released — it is left to expire, so its
 * TTL *is* the publish interval.
 *
 * OBSERVABLE, not synchronous, gauges — because this runs inside long-lived workers. A synchronous
 * gauge keeps its last value and would be re-exported at every collection tick as though it were a
 * fresh reading, so a fleet whose numbers had stopped updating would look perfectly steady. An
 * observable callback that declines to observe simply produces nothing, which is the honest shape.
 *
 * DETECTING THAT HORIZON IS DOWN IS THE ONE THING THIS CANNOT DO, and it is worth being explicit:
 * the publisher lives inside the workers, so if Horizon stops there is nobody left to report it.
 * A total outage therefore shows up as the metrics going ABSENT, not as a zero. Alert on
 * `absent_over_time(laravel_horizon_queue_length[10m])` rather than on a status gauge — a gauge that
 * could only ever report "healthy" would be worse than none, because somebody would inevitably
 * build an alert on a value it can never take.
 */
class HorizonInstrumentation implements Instrumentation
{
    public const METRIC_QUEUE_LENGTH = 'laravel.horizon.queue.length';

    public const METRIC_QUEUE_WAIT = 'laravel.horizon.queue.wait_time';

    public const METRIC_QUEUE_PROCESSES = 'laravel.horizon.queue.processes';

    public const METRIC_JOBS_PER_MINUTE = 'laravel.horizon.jobs_per_minute';

    public const METRIC_JOBS_RECENT = 'laravel.horizon.jobs.recent';

    public const METRIC_JOBS_FAILED_RECENT = 'laravel.horizon.jobs.failed_recent';

    public const METRIC_SUPERVISORS = 'laravel.horizon.supervisors';

    public const METRIC_JOB_MEMORY_PEAK_AVG = 'laravel.horizon.job.memory.peak.avg';

    public const METRIC_JOB_MEMORY_PEAK_MAX = 'laravel.horizon.job.memory.peak.max';

    public const METRIC_JOB_MEMORY_ADDED_AVG = 'laravel.horizon.job.memory.added.avg';

    public const METRIC_JOB_PROCESSED = 'laravel.horizon.job.processed';

    public const METRIC_JOB_RUNNING = 'laravel.horizon.job.running';

    /** Seconds between publishes, fleet-wide. Also the TTL of the election lock. */
    public const DEFAULT_INTERVAL = 60;

    public const LOCK_NAME = 'otel-horizon-metrics';

    /**
     * The live callback, so registering twice in one process replaces rather than stacks.
     *
     * `batchObserve()` ADDS a callback; it does not replace one. Without this, a runtime that boots
     * the application more than once in a process — Octane, or a test suite — would end up with one
     * callback per boot, all reading the same numbers, and the meter provider outlives the
     * application container that owns them.
     */
    protected static ?ObservableCallbackInterface $callback = null;

    protected static array $baselines = [];

    public function register(array $options): void
    {
        if (! interface_exists(WorkloadRepository::class)) {
            return;
        }

        self::$callback?->detach();

        $interval = max(1, (int) ($options['interval'] ?? self::DEFAULT_INTERVAL));
        $store = $options['lock_store'] ?? null;
        $memory = ($options['job_memory'] ?? true) !== false;
        $windowBuckets = max(1, (int) ($options['job_memory_window'] ?? JobMemoryStore::WINDOW_BUCKETS));
        $memoryStore = new JobMemoryStore($options['redis_connection'] ?? null);

        if ($memory) {
            $this->recordJobMemory($memoryStore, $interval);
        }

        self::$callback = Meter::batchObserve(
            [
                Meter::observableGauge(self::METRIC_QUEUE_LENGTH, '{job}', 'Jobs currently waiting on the queue.'),
                Meter::observableGauge(self::METRIC_QUEUE_WAIT, 's', 'Estimated time the oldest job on the queue has been waiting.'),
                Meter::observableGauge(self::METRIC_QUEUE_PROCESSES, '{process}', 'Worker processes currently assigned to the queue.'),
                Meter::observableGauge(self::METRIC_JOBS_PER_MINUTE, '{job}', 'Jobs processed per minute, as measured by Horizon.'),
                Meter::observableGauge(self::METRIC_JOBS_RECENT, '{job}', 'Jobs processed in Horizon\'s recent window.'),
                Meter::observableGauge(self::METRIC_JOBS_FAILED_RECENT, '{job}', 'Jobs that failed in Horizon\'s recent window.'),
                Meter::observableGauge(self::METRIC_SUPERVISORS, '{supervisor}', 'Master supervisors currently registered.'),
                Meter::observableGauge(self::METRIC_JOB_MEMORY_PEAK_AVG, 'By', 'Mean process memory high-water mark while running this job.'),
                Meter::observableGauge(self::METRIC_JOB_MEMORY_PEAK_MAX, 'By', 'Largest process memory high-water mark while running this job.'),
                Meter::observableGauge(self::METRIC_JOB_MEMORY_ADDED_AVG, 'By', 'Mean memory the job allocated above the memory already resident in the worker.'),
                Meter::observableGauge(self::METRIC_JOB_PROCESSED, '{job}', 'Jobs of this class completed on this queue in the last window.'),
                Meter::observableGauge(self::METRIC_JOB_RUNNING, '{job}', 'Jobs of this class currently executing on this queue.'),
            ],
            function (
                ObserverInterface $length,
                ObserverInterface $wait,
                ObserverInterface $processes,
                ObserverInterface $perMinute,
                ObserverInterface $recent,
                ObserverInterface $failed,
                ObserverInterface $supervisors,
                ObserverInterface $memoryPeakAvg,
                ObserverInterface $memoryPeakMax,
                ObserverInterface $memoryAddedAvg,
                ObserverInterface $jobProcessed,
                ObserverInterface $jobRunning,
            ) use ($interval, $store, $memory, $memoryStore, $windowBuckets): void {
                if (! $this->winsElection($interval, $store)) {
                    return;
                }

                // A callback registered against one application container can outlive it — Octane
                // rebuilds the container while the meter provider persists, and so does a test
                // suite. If Horizon is no longer resolvable, observing nothing is the only safe
                // move: an exception here does not just lose these gauges, it aborts the whole
                // collection and takes every other instrumentation's metrics with it.
                try {
                    $this->observeWorkload($length, $wait, $processes);
                    $this->observeThroughput($perMinute, $recent, $failed);

                    $supervisors->observe(count(app(MasterSupervisorRepository::class)->all()));
                } catch (\Throwable) {
                }

                try {
                    if ($memory) {
                        $this->observeJobMemory(
                            $memoryStore,
                            $interval,
                            $windowBuckets,
                            $memoryPeakAvg,
                            $memoryPeakMax,
                            $memoryAddedAvg,
                            $jobProcessed,
                            $jobRunning,
                        );
                    }
                } catch (\Throwable) {
                    // Swallowed, and deliberately NOT reported: this package installs a `reportable`
                    // hook that records any reported exception on the ACTIVE SPAN and marks it
                    // errored. Reporting from inside a metric collection would therefore stamp an
                    // unrelated request's or job's span with an error it had nothing to do with.
                }
            }
        );
    }

    /**
     * Whether this process is the one to publish this interval.
     *
     * The lock is acquired and deliberately NOT released, so it lapses on its own after $interval
     * seconds. That turns a mutex into a fleet-wide rate limiter: the first collection after the
     * lapse publishes, and every other process in between observes nothing.
     */
    protected function winsElection(int $interval, ?string $store): bool
    {
        $underlying = Cache::store($store)->getStore();

        // Locks live on the STORE, not on the repository — and not every store has them. Declining
        // when the store cannot lock is the safe direction: no data beats one identical copy of
        // every fleet-wide number per worker process.
        if (! $underlying instanceof LockProvider) {
            return false;
        }

        return $underlying->lock(self::LOCK_NAME, $interval)->get();
    }

    protected function observeWorkload(ObserverInterface $length, ObserverInterface $wait, ObserverInterface $processes): void
    {
        foreach (app(WorkloadRepository::class)->get() as $workload) {
            // Horizon reports supervisor-scoped queues as `queue:supervisor`; the queue itself is the
            // useful dimension, and keeping the supervisor would fragment one queue's depth across
            // however many supervisors happen to serve it.
            $attributes = ['queue' => Str::before((string) ($workload['name'] ?? 'unknown'), ':')];

            $length->observe((int) ($workload['length'] ?? 0), $attributes);
            $wait->observe((float) ($workload['wait'] ?? 0), $attributes);
            $processes->observe((int) ($workload['processes'] ?? 0), $attributes);
        }
    }

    protected function recordJobMemory(JobMemoryStore $store, int $interval): void
    {
        $events = app('events');

        $events->listen(JobProcessing::class, function (JobProcessing $event) use ($store, $interval): void {
            $key = $this->jobKey($event->job);

            self::$baselines[$key] = memory_get_usage(true);

            if (function_exists('memory_reset_peak_usage')) {
                memory_reset_peak_usage();
            }

            $store->adjustRunning(
                JobMemoryStore::normalizeQueue($event->job->getQueue()),
                $event->job->resolveName(),
                1,
                $interval
            );
        });

        $finish = function (JobProcessed|JobFailed $event) use ($store, $interval): void {
            $key = $this->jobKey($event->job);
            $baseline = self::$baselines[$key] ?? memory_get_usage(true);
            unset(self::$baselines[$key]);

            $peak = memory_get_peak_usage(true);
            $queue = JobMemoryStore::normalizeQueue($event->job->getQueue());
            $job = $event->job->resolveName();

            $store->record($queue, $job, $peak, $peak > $baseline ? $peak - $baseline : 0, $interval);
            $store->adjustRunning($queue, $job, -1, $interval);
        };

        $events->listen(JobProcessed::class, $finish);
        $events->listen(JobFailed::class, $finish);
    }

    protected function jobKey(object $job): string
    {
        if (! method_exists($job, 'getJobId')) {
            return spl_object_hash($job);
        }

        $id = $job->getJobId();

        return is_string($id) && $id !== '' ? $id : spl_object_hash($job);
    }

    protected function observeJobMemory(
        JobMemoryStore $store,
        int $interval,
        int $windowBuckets,
        ObserverInterface $peakAvg,
        ObserverInterface $peakMax,
        ObserverInterface $addedAvg,
        ObserverInterface $processed,
        ObserverInterface $running,
    ): void {
        foreach ($store->readWindow($interval, $windowBuckets) as $row) {
            $attributes = ['queue' => $row['queue'], 'job_name' => $row['job']];
            $count = max(1, (int) ($row['count'] ?? 0));

            $peakAvg->observe(intdiv((int) ($row['sum_peak'] ?? 0), $count), $attributes);
            $peakMax->observe((int) ($row['max_peak'] ?? 0), $attributes);
            $addedAvg->observe(intdiv((int) ($row['sum_added'] ?? 0), $count), $attributes);
            $processed->observe((int) ($row['count'] ?? 0), $attributes);
        }

        foreach ($store->readRunning() as $row) {
            $running->observe($row['running'], ['queue' => $row['queue'], 'job_name' => $row['job']]);
        }
    }

    protected function observeThroughput(ObserverInterface $perMinute, ObserverInterface $recent, ObserverInterface $failed): void
    {
        $perMinute->observe((int) app(MetricsRepository::class)->jobsProcessedPerMinute());

        $jobs = app(JobRepository::class);

        $recent->observe((int) $jobs->countRecent());
        $failed->observe((int) $jobs->countRecentlyFailed());
    }
}
