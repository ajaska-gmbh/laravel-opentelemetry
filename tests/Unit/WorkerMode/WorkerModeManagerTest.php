<?php

use Keepsuit\LaravelOpenTelemetry\Facades\Meter;
use Keepsuit\LaravelOpenTelemetry\Facades\OpenTelemetry;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors\LongRunningCommandWorkerModeDetector;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeDetectorInterface;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeManager;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerStoppingDetectorInterface;
use Spatie\TestTime\TestTime;

function stoppingDetector(): object
{
    return new class implements WorkerModeDetectorInterface, WorkerStoppingDetectorInterface
    {
        public ?Closure $iteration = null;

        public ?Closure $stopping = null;

        public function detect(): bool
        {
            return true;
        }

        public function onIterationEnded(Closure $callback): void
        {
            $this->iteration = $callback;
        }

        public function onWorkerStopping(Closure $callback): void
        {
            $this->stopping = $callback;
        }
    };
}

test('collects on worker stopping regardless of the interval', function () {
    TestTime::freezeAtSecond();
    Meter::shouldReceive('collect')->once();

    $detector = stoppingDetector();
    new WorkerModeManager(metricsExportInterval: 60, detectors: [$detector]);

    ($detector->stopping)();
});

test('an export failure never reaches the worker', function () {
    TestTime::freezeAtSecond();
    Meter::shouldReceive('collect')->once()->andThrow(new RuntimeException('collector down'));

    $detector = stoppingDetector();
    new WorkerModeManager(metricsExportInterval: 60, detectors: [$detector]);

    $log = tempnam(sys_get_temp_dir(), 'otel');
    $previous = ini_set('error_log', $log);

    expect(fn () => ($detector->stopping)())->not->toThrow(RuntimeException::class)
        ->and(file_get_contents($log))->toContain('worker mode metrics export failed: collector down');

    ini_set('error_log', $previous ?: '');
    unlink($log);
});

test('a long-running command only collects on the interval, even when flushing every iteration', function () {
    TestTime::freezeAtSecond();
    $ORIGINAL_SERVER = $_SERVER;
    $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
    $_SERVER['argv'] = ['artisan', 'dialer:run'];

    OpenTelemetry::shouldReceive('flush')->never();
    Meter::shouldReceive('collect')->once();

    $detector = new class(['dialer:run']) extends LongRunningCommandWorkerModeDetector
    {
        public ?Closure $callback = null;

        public function onIterationEnded(Closure $callback): void
        {
            $this->callback = $callback;
        }
    };

    new WorkerModeManager(flushAfterEachIteration: true, metricsExportInterval: 60, detectors: [$detector]);

    ($detector->callback)();
    TestTime::addSeconds(30);
    ($detector->callback)();
    TestTime::addSeconds(31);
    ($detector->callback)();

    $_SERVER = $ORIGINAL_SERVER;
});

test('the provider adds the long-running command detector from the environment', function () {
    $ORIGINAL_SERVER = $_SERVER;
    $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
    $_SERVER['argv'] = ['artisan', 'dialer:run'];
    config()->set('opentelemetry.worker_mode.long_running_commands', null);
    putenv('OTEL_WORKER_MODE_LONG_RUNNING_COMMANDS=app:run, dialer:run');
    $_ENV['OTEL_WORKER_MODE_LONG_RUNNING_COMMANDS'] = 'app:run, dialer:run';

    app()->forgetInstance(WorkerModeManager::class);
    $manager = app(WorkerModeManager::class);

    $detectors = (fn () => $this->detectors)->call($manager);
    $longRunning = array_values(array_filter($detectors, fn ($d) => $d instanceof LongRunningCommandWorkerModeDetector));

    expect($longRunning)->toHaveCount(1)
        ->and((fn () => $this->commands)->call($longRunning[0]))->toBe(['app:run', 'dialer:run']);

    putenv('OTEL_WORKER_MODE_LONG_RUNNING_COMMANDS');
    unset($_ENV['OTEL_WORKER_MODE_LONG_RUNNING_COMMANDS']);
    $_SERVER = $ORIGINAL_SERVER;
});
