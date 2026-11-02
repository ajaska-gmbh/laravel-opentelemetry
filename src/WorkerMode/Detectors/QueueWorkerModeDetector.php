<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors;

use Closure;
use Illuminate\Queue\Events\JobAttempted;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Events\WorkerStopping;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeDetectorInterface;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerStoppingDetectorInterface;

/**
 * Detects Laravel Queue worker mode
 */
class QueueWorkerModeDetector implements WorkerModeDetectorInterface, WorkerStoppingDetectorInterface
{
    public function detect(): bool
    {
        return app()->runningConsoleCommand('queue:work')
            || app()->runningConsoleCommand('horizon:work');
    }

    public function onIterationEnded(Closure $callback): void
    {
        app('events')->listen(JobAttempted::class, $callback);

        // Looping fires on every pass of the worker loop, also when the queue is empty, so an
        // idle worker still exports what it recorded. Returning false from a Looping listener
        // pauses the worker, so this one must return nothing.
        app('events')->listen(Looping::class, function () use ($callback): void {
            $callback();
        });
    }

    public function onWorkerStopping(Closure $callback): void
    {
        app('events')->listen(WorkerStopping::class, function () use ($callback): void {
            $callback();
        });
    }
}
