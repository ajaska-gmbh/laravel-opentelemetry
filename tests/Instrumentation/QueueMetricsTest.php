<?php

use Keepsuit\LaravelOpenTelemetry\Instrumentation\QueueInstrumentation;
use Keepsuit\LaravelOpenTelemetry\Tests\Support\TestJob;
use OpenTelemetry\SDK\Metrics\Data\Histogram;
use OpenTelemetry\SDK\Metrics\Data\Metric;
use OpenTelemetry\SDK\Metrics\Data\Sum;
use OpenTelemetry\SemConv\Attributes\ErrorAttributes;
use OpenTelemetry\SemConv\Incubating\Attributes\MessagingIncubatingAttributes;
use Spatie\Valuestore\Valuestore;

/**
 * The queue COUNTERS, as opposed to the spans covered by QueueInstrumentationTest.
 *
 * They exist because spans cannot answer "how many jobs failed" without reading and aggregating
 * traces, and because a failure rate should be one ratio over one metric.
 */
beforeEach(function () {
    registerInstrumentation(QueueInstrumentation::class);

    $this->valuestore = Valuestore::make(__DIR__.'/testJob.json')->flush();
});

afterEach(function () {
    $this->valuestore->flush();
});

function queueMetric(string $name): ?Metric
{
    return getRecordedMetrics()->firstWhere('name', $name);
}

it('counts a job pushed onto a queue', function () {
    dispatch(new TestJob($this->valuestore));

    $metric = queueMetric(QueueInstrumentation::METRIC_SENT_MESSAGES);

    expect($metric)->toBeInstanceOf(Metric::class)
        ->and($metric->data)->toBeInstanceOf(Sum::class)
        ->and($metric->data->dataPoints[0]->value)->toBe(1)
        ->and($metric->data->dataPoints[0]->attributes->get(MessagingIncubatingAttributes::MESSAGING_SYSTEM))->toBe('redis');
});

it('counts a processed job with no error type', function () {
    dispatch(new TestJob($this->valuestore));
    executeQueueWork();

    $metric = queueMetric(QueueInstrumentation::METRIC_CONSUMED_MESSAGES);
    $point = $metric->data->dataPoints[0];

    expect($point->value)->toBe(1)
        // Success must carry NO error.type at all — an empty-string label would make
        // `{error_type=""}` and `{error_type!=""}` both match, and the failure-rate query wrong.
        ->and($point->attributes->has(ErrorAttributes::ERROR_TYPE))->toBeFalse()
        ->and($point->attributes->get(MessagingIncubatingAttributes::MESSAGING_DESTINATION_NAME))->toBe('default');
});

it('records the consumer operation duration', function () {
    dispatch(new TestJob($this->valuestore));
    executeQueueWork();

    $metric = queueMetric(QueueInstrumentation::METRIC_OPERATION_DURATION);

    expect($metric)->toBeInstanceOf(Metric::class)
        ->and($metric->data)->toBeInstanceOf(Histogram::class)
        ->and($metric->data->dataPoints[0]->count)->toBe(1)
        ->and($metric->data->dataPoints[0]->sum)->toBeGreaterThanOrEqual(0.0);
});

it('labels a failed job with the exception class', function () {
    dispatch(new TestJob($this->valuestore, fail: true));
    executeQueueWork();

    $metric = queueMetric(QueueInstrumentation::METRIC_CONSUMED_MESSAGES);
    $point = collect($metric->data->dataPoints)
        ->first(fn ($point) => $point->attributes->has(ErrorAttributes::ERROR_TYPE));

    expect($point)->not->toBeNull()
        // The exception CLASS, never its message: a message can carry ids, emails or SQL, and an
        // attribute value becomes a metric label — unbounded cardinality and a data leak at once.
        ->and($point->attributes->get(ErrorAttributes::ERROR_TYPE))->toBeString()
        ->and($point->attributes->get(ErrorAttributes::ERROR_TYPE))->not->toContain(' ');
});

it('counts a failing job once, not twice', function () {
    // JobExceptionOccurred and JobFailed both fire for a job that exhausts its attempts. Counting
    // in both listeners would double every failure — and failures are exactly the number people
    // build alerts on.
    dispatch(new TestJob($this->valuestore, fail: true));
    executeQueueWork();

    $total = collect(queueMetric(QueueInstrumentation::METRIC_CONSUMED_MESSAGES)->data->dataPoints)
        ->sum(fn ($point) => $point->value);

    expect($total)->toBe(1);
});
