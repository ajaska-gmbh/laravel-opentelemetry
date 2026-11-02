<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors;

use Closure;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Http\Client\Events\ResponseReceived;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeDetectorInterface;

/**
 * Detects long-running console commands (daemons such as a custom `app:run` loop) named in
 * `opentelemetry.worker_mode.long_running_commands`. Without it they export metrics only when
 * they exit.
 *
 * Such commands fire no "iteration ended" event, so metrics are collected (at most every
 * metrics_collect_interval) after a database query or an outgoing HTTP response: points where
 * the command is between operations, never in the middle of one. A command that does neither
 * still exports at exit.
 */
class LongRunningCommandWorkerModeDetector implements WorkerModeDetectorInterface
{
    /**
     * @param  list<string>  $commands
     */
    public function __construct(protected array $commands = []) {}

    public function detect(): bool
    {
        return $this->commands !== [] && app()->runningConsoleCommand(...$this->commands);
    }

    public function onIterationEnded(Closure $callback): void
    {
        app('events')->listen(QueryExecuted::class, function () use ($callback): void {
            $callback();
        });
        app('events')->listen(ResponseReceived::class, function () use ($callback): void {
            $callback();
        });
    }
}
