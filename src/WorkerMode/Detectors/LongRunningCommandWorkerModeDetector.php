<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors;

use Closure;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Http\Client\Events\ResponseReceived;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\PeriodicWorkerModeDetectorInterface;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\WorkerModeDetectorInterface;

/**
 * Detects long-running console commands (daemons such as a custom `app:run` loop) named in
 * `opentelemetry.worker_mode.long_running_commands`. Without it they export metrics only when
 * they exit.
 *
 * Such commands fire no "iteration ended" event, so metrics are collected (at most every
 * metrics_collect_interval) after a database query or an outgoing HTTP response: points where
 * the command is between operations, never in the middle of one, and never inside an open
 * database transaction. A command that does neither still exports at exit.
 */
class LongRunningCommandWorkerModeDetector implements PeriodicWorkerModeDetectorInterface, WorkerModeDetectorInterface
{
    /**
     * @param  list<string>  $commands  bare command names, e.g. "dialer:run" (no options)
     */
    public function __construct(protected array $commands = []) {}

    public function detect(): bool
    {
        return $this->commands !== [] && app()->runningConsoleCommand(...$this->commands);
    }

    /**
     * A daemon has no unit of work to end, so nothing is flushed per iteration.
     */
    public function onIterationEnded(Closure $callback): void {}

    public function onTick(Closure $callback): void
    {
        app('events')->listen(QueryExecuted::class, function (QueryExecuted $event) use ($callback): void {
            // An export blocks on the network; inside a transaction it would hold its locks.
            if ($event->connection->transactionLevel() > 0) {
                return;
            }

            $callback();
        });
        app('events')->listen(ResponseReceived::class, function () use ($callback): void {
            $callback();
        });
    }
}
