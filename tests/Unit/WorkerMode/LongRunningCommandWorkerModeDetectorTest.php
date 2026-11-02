<?php

use Illuminate\Database\Connection;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Http\Client\Events\ResponseReceived;
use Illuminate\Http\Client\Request;
use Illuminate\Http\Client\Response;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors\LongRunningCommandWorkerModeDetector;

function queryOn(int $transactionLevel): QueryExecuted
{
    $connection = Mockery::mock(Connection::class);
    $connection->shouldReceive('getName')->andReturn('mysql');
    $connection->shouldReceive('transactionLevel')->andReturn($transactionLevel);

    return new QueryExecuted('select 1', [], 1.0, $connection);
}

test('detects a configured long-running command', function () {
    $ORIGINAL_SERVER = $_SERVER;

    try {
        $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
        $_SERVER['argv'] = ['artisan', 'dialer:run', '--max-runtime=86400'];

        expect((new LongRunningCommandWorkerModeDetector(['app:run', 'dialer:run']))->detect())->toBeTrue()
            ->and((new LongRunningCommandWorkerModeDetector(['app:run']))->detect())->toBeFalse()
            // Entries are bare command names.
            ->and((new LongRunningCommandWorkerModeDetector(['dialer:run --max-runtime=86400']))->detect())->toBeFalse();
    } finally {
        $_SERVER = $ORIGINAL_SERVER;
    }
});

test('detects nothing without configured commands', function () {
    expect((new LongRunningCommandWorkerModeDetector([]))->detect())->toBeFalse();
});

test('ticks after database queries and HTTP responses', function () {
    $detector = new LongRunningCommandWorkerModeDetector(['dialer:run']);
    $called = 0;

    $detector->onTick(function () use (&$called) {
        $called++;
    });

    event(queryOn(0));
    event(new ResponseReceived(
        new Request(new GuzzleHttp\Psr7\Request('GET', 'https://example.test')),
        new Response(new GuzzleHttp\Psr7\Response(200)),
    ));

    expect($called)->toBe(2);
});

test('does not tick inside an open database transaction', function () {
    $detector = new LongRunningCommandWorkerModeDetector(['dialer:run']);
    $called = 0;

    $detector->onTick(function () use (&$called) {
        $called++;
    });

    event(queryOn(1));
    event(queryOn(0));

    expect($called)->toBe(1);
});

test('never registers anything per iteration', function () {
    $detector = new LongRunningCommandWorkerModeDetector(['dialer:run']);
    $called = false;

    $detector->onIterationEnded(function () use (&$called) {
        $called = true;
    });
    event(queryOn(0));

    expect($called)->toBeFalse();
});
