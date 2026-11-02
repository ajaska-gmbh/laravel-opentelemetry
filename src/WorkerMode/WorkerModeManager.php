<?php

namespace Keepsuit\LaravelOpenTelemetry\WorkerMode;

use Carbon\Carbon;
use Keepsuit\LaravelOpenTelemetry\Facades\Meter;
use Keepsuit\LaravelOpenTelemetry\Facades\OpenTelemetry;
use Throwable;

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
                // A long-running command "iterates" on every query and HTTP response, far too often
                // to flush everything each time: it only ever collects metrics on the interval.
                $detector->onIterationEnded($detector instanceof Detectors\LongRunningCommandWorkerModeDetector
                    ? fn () => $this->collectIfDue()
                    : fn () => $this->handleIterationEnded());

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
     * Export whatever is pending before the worker stops, regardless of the interval.
     */
    protected function handleWorkerStopping(): void
    {
        $this->lastMetricsExportTimestamp = Carbon::now()->getTimestamp();
        $this->export(fn () => $this->flushAfterEachIteration ? OpenTelemetry::flush() : Meter::collect());
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
