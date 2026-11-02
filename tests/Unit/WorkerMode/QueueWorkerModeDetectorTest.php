<?php

use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Events\JobAttempted;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Support\Facades\Event;
use Keepsuit\LaravelOpenTelemetry\WorkerMode\Detectors\QueueWorkerModeDetector;

test('detects when queue:work is running', function () {
    $ORIGINAL_SERVER = $_SERVER;

    $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
    $_SERVER['argv'] = ['artisan', 'queue:work'];

    $detector = new QueueWorkerModeDetector;
    expect($detector->detect())->toBeTrue();

    $_SERVER = $ORIGINAL_SERVER;
});

test('detects when horizon:work is running', function () {
    $ORIGINAL_SERVER = $_SERVER;

    $_SERVER['APP_RUNNING_IN_CONSOLE'] = '1';
    $_SERVER['argv'] = ['artisan', 'horizon:work'];

    $detector = new QueueWorkerModeDetector;
    expect($detector->detect())->toBeTrue();

    $_SERVER = $ORIGINAL_SERVER;
});

test('does not detect when neither queue:work nor horizon:work is running', function () {
    $detector = new QueueWorkerModeDetector;
    expect($detector->detect())->toBeFalse();
});

test('registers callback for JobAttempted event', function () {
    $detector = new QueueWorkerModeDetector;
    $called = false;

    $detector->onIterationEnded(function () use (&$called) {
        $called = true;
    });

    // Create a mock job that satisfies the JobAttempted event constructor
    $mockJob = Mockery::mock(Job::class);
    $mockJob->shouldReceive('hasFailed')->andReturn(false);

    event(new JobAttempted('default', $mockJob, false));

    expect($called)->toBeTrue();
});

test('ticks on the Looping event and never pauses the worker', function () {
    $detector = new QueueWorkerModeDetector;
    $called = 0;

    $detector->onTick(function () use (&$called) {
        $called++;

        return false;
    });

    // The worker pauses when until(Looping) returns false; ours must return nothing.
    expect(Event::until(new Looping('redis', 'default')))->toBeNull()
        ->and($called)->toBe(1);
});

test('registers callback for the WorkerStopping event', function () {
    $detector = new QueueWorkerModeDetector;
    $called = false;

    $detector->onWorkerStopping(function () use (&$called) {
        $called = true;
    });

    event(new WorkerStopping(1));

    expect($called)->toBeTrue();
});
