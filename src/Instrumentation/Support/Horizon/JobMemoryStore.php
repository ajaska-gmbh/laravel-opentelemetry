<?php

namespace Keepsuit\LaravelOpenTelemetry\Instrumentation\Support\Horizon;

use Illuminate\Contracts\Redis\Factory as RedisFactory;
use Illuminate\Support\Str;

class JobMemoryStore
{
    public const PREFIX = 'otel:horizon:jobmem:';

    public const RUNNING_KEY = 'otel:horizon:jobrunning';

    public const KEEP_BUCKETS = 15;

    public const WINDOW_BUCKETS = 10;

    protected ?string $connection;

    public function __construct(?string $connection = null)
    {
        $this->connection = $connection;
    }

    public function record(string $queue, string $job, int $peak, int $added, int $interval): void
    {
        $this->eval(
            <<<'LUA'
            redis.call('HINCRBY', KEYS[1], ARGV[1] .. '|count', 1)
            redis.call('HINCRBY', KEYS[1], ARGV[1] .. '|sum_peak', ARGV[2])
            redis.call('HINCRBY', KEYS[1], ARGV[1] .. '|sum_added', ARGV[3])
            local maxField = ARGV[1] .. '|max_peak'
            local current = redis.call('HGET', KEYS[1], maxField)
            if (not current) or (tonumber(ARGV[2]) > tonumber(current)) then
                redis.call('HSET', KEYS[1], maxField, ARGV[2])
            end
            redis.call('EXPIRE', KEYS[1], ARGV[4])
            LUA,
            [$this->bucketKey($interval, 0)],
            [$this->field($queue, $job), (string) $peak, (string) $added, (string) ($interval * self::KEEP_BUCKETS)]
        );
    }

    public function adjustRunning(string $queue, string $job, int $delta, int $interval): void
    {
        $this->eval(
            <<<'LUA'
            redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
            redis.call('EXPIRE', KEYS[1], ARGV[3])
            LUA,
            [self::RUNNING_KEY],
            [$this->field($queue, $job), (string) $delta, (string) max(300, $interval * self::KEEP_BUCKETS)]
        );
    }

    public function readWindow(int $interval, ?int $buckets = null): array
    {
        $buckets = max(1, $buckets ?? self::WINDOW_BUCKETS);
        $rows = [];

        for ($offset = -1; $offset >= -$buckets; $offset--) {
            foreach ($this->hgetall($this->bucketKey($interval, $offset)) as $field => $value) {
                $parts = explode('|', (string) $field);

                if (count($parts) !== 3) {
                    continue;
                }

                [$queue, $job, $metric] = $parts;
                $key = $queue.'|'.$job;

                if (! isset($rows[$key])) {
                    $rows[$key] = [
                        'queue' => $queue,
                        'job' => $job,
                        'count' => 0,
                        'sum_peak' => 0,
                        'max_peak' => 0,
                        'sum_added' => 0,
                    ];
                }

                $amount = (int) $value;

                if ($metric === 'max_peak') {
                    $rows[$key]['max_peak'] = max($rows[$key]['max_peak'], $amount);
                } elseif ($metric === 'count') {
                    $rows[$key]['count'] += $amount;
                } elseif ($metric === 'sum_peak') {
                    $rows[$key]['sum_peak'] += $amount;
                } elseif ($metric === 'sum_added') {
                    $rows[$key]['sum_added'] += $amount;
                }
            }
        }

        return array_values(array_filter($rows, fn (array $row): bool => $row['count'] > 0));
    }

    public function readRunning(): array
    {
        $rows = [];

        foreach ($this->hgetall(self::RUNNING_KEY) as $field => $value) {
            $parts = explode('|', (string) $field);

            if (count($parts) !== 2) {
                continue;
            }

            $rows[] = [
                'queue' => $parts[0],
                'job' => $parts[1],
                'running' => max(0, (int) $value),
            ];
        }

        return $rows;
    }

    public static function normalizeQueue(?string $queue): string
    {
        $queue = Str::after((string) ($queue ?: 'default'), 'queues:');

        return Str::before($queue, ':') ?: 'default';
    }

    protected function field(string $queue, string $job): string
    {
        return str_replace('|', '_', $queue).'|'.str_replace('|', '_', $job);
    }

    protected function bucketKey(int $interval, int $offset): string
    {
        return self::PREFIX.(intdiv(time(), max(1, $interval)) + $offset);
    }

    protected function hgetall(string $key): array
    {
        $result = $this->command('hgetall', [$key]);

        return is_array($result) ? $result : [];
    }

    protected function eval(string $script, array $keys, array $args): void
    {
        $this->command('eval', [$script, count($keys), ...$keys, ...$args]);
    }

    protected function command(string $method, array $parameters): mixed
    {
        try {
            return app(RedisFactory::class)
                ->connection($this->connection)
                ->{$method}(...$parameters);
        } catch (\Throwable) {
            return null;
        }
    }
}
