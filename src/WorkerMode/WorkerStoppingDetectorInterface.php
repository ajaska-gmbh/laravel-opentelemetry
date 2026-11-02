<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode;

use Closure;

/**
 * A worker mode that announces when the worker is about to stop, so pending metrics can be
 * exported first. Matters where the process may end without running shutdown functions
 * (e.g. Laravel's queue worker kills itself with SIGKILL when a job times out).
 */
interface WorkerStoppingDetectorInterface
{
    /**
     * Register a callback to be executed when the worker is about to stop
     */
    public function onWorkerStopping(Closure $callback): void;
}
