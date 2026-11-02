<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode;

use Closure;

/**
 * A worker mode with frequent points (between jobs, between operations) at which metrics may be
 * collected when the interval is due. Unlike onIterationEnded, a tick never flushes traces or
 * logs, even with flush_after_each_iteration: it can fire far more often than once per unit of work.
 */
interface PeriodicWorkerModeDetectorInterface
{
    /**
     * Register a callback to be executed at each tick
     */
    public function onTick(Closure $callback): void;
}
