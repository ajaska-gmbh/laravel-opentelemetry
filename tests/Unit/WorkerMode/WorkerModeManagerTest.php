<?php

use Keepsuit\LaravelOpenTelemetry\Facades\Meter;
use Keepsuit\LaravelOpenTelemetry\Facades\OpenTelemetry;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors\LongRunningCommandWorkerModeDetector;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors\QueueWorkerModeDetector;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\PeriodicWorkerModeDetectorInterface;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeDetectorInterface;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeManager;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerStoppingDetectorInterface;
use Spatie\TestTime\TestTime;

function hookDetector(): object
{
    return new class implements PeriodicWorkerModeDetectorInterface, WorkerModeDetectorInterface, WorkerStoppingDetectorInterface
    {
        public ?Closure $iteration = null;

        public ?Closure $tick = null;

        public ?Closure $stopping = null;

        public function detect(): bool
        {
            return true;
        }

        public function onIterationEnded(Closure $callback): void
        {
            $this->iteration = $callback;
        }

        public function onTick(Closure $callback): void
        {
            $this->tick = $callback;
        }

        public function onWorkerStopping(Closure $callback): void
        {
            $this->stopping = $callback;
        }
    };
}

test('a tick collects metrics only when the interval is due', function () {
    TestTime::freezeAtSecond();
    Meter::shouldReceive('collect')->once();

    $detector = hookDetector();
    new WorkerModeManager(metricsExportInterval: 60, detectors: [$detector]);

    ($detector->tick)();
    TestTime::addSeconds(59);
    ($detector->tick)();
    TestTime::addSeconds(1);
    ($detector->tick)();
    ($detector->tick)();
});

test('a tick never flushes, even when flushing after each iteration', function () {
    TestTime::freezeAtSecond();
    OpenTelemetry::shouldReceive('flush')->once();
    Meter::shouldReceive('collect')->once();

    $detector = hookDetector();
    new WorkerModeManager(flushAfterEachIteration: true, metricsExportInterval: 60, detectors: [$detector]);

    ($detector->tick)();
    ($detector->iteration)();
    TestTime::addSeconds(60);
    ($detector->tick)();
});

test('worker stopping flushes everything regardless of the interval', function () {
    TestTime::freezeAtSecond();
    OpenTelemetry::shouldReceive('flush')->once();

    $detector = hookDetector();
    new WorkerModeManager(metricsExportInterval: 60, detectors: [$detector]);

    ($detector->stopping)();
});

test('an export failure never reaches the worker', function () {
    TestTime::freezeAtSecond();
    OpenTelemetry::shouldReceive('flush')->once()->andThrow(new RuntimeException('collector down'));

    $detector = hookDetector();
    new WorkerModeManager(metricsExportInterval: 60, detectors: [$detector]);

    $log = tempnam(sys_get_temp_dir(), 'otel');
    $previous = ini_set('error_log', $log);

    try {
        expect(fn () => ($detector->stopping)())->not->toThrow(RuntimeException::class)
            ->and(file_get_contents($log))->toContain('worker mode metrics export failed: collector down');
    } finally {
        ini_set('error_log', $previous ?: '');
        unlink($log);
    }
});

test('an export cannot start another export', function () {
    TestTime::freezeAtSecond();

    $detector = hookDetector();
    Meter::shouldReceive('collect')->once()->andReturnUsing(function () use ($detector) {
        // e.g. an observable gauge callback that queries the database
        TestTime::addSeconds(120);
        ($detector->tick)();

        return true;
    });

    new WorkerModeManager(metricsExportInterval: 60, detectors: [$detector]);

    TestTime::addSeconds(60);
    ($detector->tick)();
});

test('the provider adds the long-running command detector when the published config lacks the key', function () {
    $ORIGINAL_SERVER = $_SERVER;

    try {
        $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
        $_SERVER['argv'] = ['artisan', 'dialer:run'];
        // An app config published before the key existed: its own detector list, no long_running_commands.
        config()->set('opentelemetry.worker_mode', [
            'flush_after_each_iteration' => false,
            'metrics_collect_interval' => 60,
            'detectors' => [QueueWorkerModeDetector::class],
        ]);
        putenv('OTEL_WORKER_MODE_LONG_RUNNING_COMMANDS=app:run, dialer:run');

        app()->forgetInstance(WorkerModeManager::class);
        $manager = app(WorkerModeManager::class);

        $detectors = (fn () => $this->detectors)->call($manager);
        $longRunning = array_values(array_filter($detectors, fn ($d) => $d instanceof LongRunningCommandWorkerModeDetector));

        expect($longRunning)->toHaveCount(1)
            ->and((fn () => $this->commands)->call($longRunning[0]))->toBe(['app:run', 'dialer:run']);
    } finally {
        putenv('OTEL_WORKER_MODE_LONG_RUNNING_COMMANDS');
        $_SERVER = $ORIGINAL_SERVER;
    }
});
