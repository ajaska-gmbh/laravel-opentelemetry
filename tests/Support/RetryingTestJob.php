<?php

namespace Keepsuit\LaravelOpenTelemetry\Tests\Support;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;
use Keepsuit\LaravelOpenTelemetry\Facades\Tracer;
use Spatie\Valuestore\Valuestore;

class RetryingTestJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int $tries = 2;

    public function __construct(
        protected Valuestore $valuestore
    ) {}

    public function handle(): void
    {
        $attempt = $this->job->attempts();

        $this->valuestore->put('uuid', $this->job->uuid());
        $this->valuestore->put('traceparentInJobAttempts', [
            ...$this->valuestore->get('traceparentInJobAttempts', []),
            $this->job->payload()['traceparent'] ?? null,
        ]);
        $this->valuestore->put('traceIdInJobAttempts', [
            ...$this->valuestore->get('traceIdInJobAttempts', []),
            Tracer::traceId(),
        ]);
        $this->valuestore->put('logContextInJobAttempts', [
            ...$this->valuestore->get('logContextInJobAttempts', []),
            Log::sharedContext(),
        ]);

        if ($attempt === 1) {
            throw new \Exception('Job failed on first attempt');
        }
    }
}
