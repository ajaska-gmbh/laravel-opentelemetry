<?php

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Http\Client\Events\ResponseReceived;
use Illuminate\Http\Client\Request;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\DB;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors\LongRunningCommandWorkerModeDetector;

test('detects a configured long-running command', function () {
    $ORIGINAL_SERVER = $_SERVER;

    $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
    $_SERVER['argv'] = ['artisan', 'dialer:run', '--max-runtime=86400'];

    expect((new LongRunningCommandWorkerModeDetector(['app:run', 'dialer:run']))->detect())->toBeTrue()
        ->and((new LongRunningCommandWorkerModeDetector(['app:run']))->detect())->toBeFalse();

    $_SERVER = $ORIGINAL_SERVER;
});

test('detects nothing without configured commands', function () {
    expect((new LongRunningCommandWorkerModeDetector([]))->detect())->toBeFalse();
});

test('calls back after database queries and HTTP responses', function () {
    $detector = new LongRunningCommandWorkerModeDetector(['dialer:run']);
    $called = 0;

    $detector->onIterationEnded(function () use (&$called) {
        $called++;
    });

    event(new QueryExecuted('select 1', [], 1.0, DB::connection()));
    event(new ResponseReceived(
        new Request(new GuzzleHttp\Psr7\Request('GET', 'https://example.test')),
        new Response(new GuzzleHttp\Psr7\Response(200)),
    ));

    expect($called)->toBe(2);
});
