<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode;

use Carbon\Carbon;
use Keepsuit\LaravelOpenTelemetry\Facades\Meter;
use Keepsuit\LaravelOpenTelemetry\Facades\OpenTelemetry;
use Throwable;

use function Safe\error_log;

class WorkerModeManager
{
    protected int $lastMetricsExportTimestamp;

    /**
     * Set while exporting, so an event fired by the export itself cannot start another one.
     */
    protected bool $exporting = false;

    public function __construct(
        protected bool $flushAfterEachIteration = false,
        protected int $metricsExportInterval = 60,
        /**
         * @var WorkerModeDetectorInterface[]
         */
        protected array $detectors = []
    ) {
        $this->lastMetricsExportTimestamp = Carbon::now()->getTimestamp();

        $this->initDetectors();
    }

    protected function initDetectors(): void
    {
        foreach ($this->detectors as $detector) {
            if ($detector->detect()) {
                $detector->onIterationEnded(fn () => $this->handleIterationEnded());

                if ($detector instanceof PeriodicWorkerModeDetectorInterface) {
                    $detector->onTick(fn () => $this->collectIfDue());
                }

                if ($detector instanceof WorkerStoppingDetectorInterface) {
                    $detector->onWorkerStopping(fn () => $this->handleWorkerStopping());
                }
            }
        }
    }

    protected function handleIterationEnded(): void
    {
        if ($this->flushAfterEachIteration) {
            $this->export(fn () => OpenTelemetry::flush());

            return;
        }

        $this->collectIfDue();
    }

    protected function collectIfDue(): void
    {
        $timestamp = Carbon::now()->getTimestamp();
        if ($timestamp - $this->lastMetricsExportTimestamp >= $this->metricsExportInterval) {
            $this->lastMetricsExportTimestamp = $timestamp;
            $this->export(fn () => Meter::collect());
        }
    }

    /**
     * Export everything pending (traces, metrics, logs) before the worker stops, regardless of
     * the interval: a worker killed after a job timeout runs no shutdown functions.
     */
    protected function handleWorkerStopping(): void
    {
        $this->lastMetricsExportTimestamp = Carbon::now()->getTimestamp();
        $this->export(fn () => OpenTelemetry::flush());
    }

    /**
     * Telemetry must never break the worker: these callbacks run inside framework event
     * listeners (a failing QueryExecuted listener would fail the query's caller).
     */
    protected function export(callable $export): void
    {
        if ($this->exporting) {
            return;
        }

        $this->exporting = true;

        try {
            $export();
        } catch (Throwable $e) {
            error_log('opentelemetry: worker mode metrics export failed: '.$e->getMessage());
        } finally {
            $this->exporting = false;
        }
    }
}
