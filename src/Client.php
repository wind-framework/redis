<?php

namespace Wind\Redis;

use Amp\DeferredFuture;
use Revolt\EventLoop;
use SplQueue;
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Connection\TcpConnection;
use Workerman\Timer;

/**
 * Wind Redis Client
 */
class Client
{

    use Commands;

    private AsyncTcpConnection $connection;

    private int $status = 0;

    private const STATUS_CLOSED = 0;
    private const STATUS_CONNECTING = 1;
    private const STATUS_CONNECTED = 2;

    private ?DeferredFuture $connectDeferred;

    private array $config = [];

    private SplQueue $queue;
    private bool $queuePaused = false;

    /**
     * @var int
     */
    private $db = 0;

    /**
     * @var string|array
     */
    private $password = null;

    /**
     * @var string
     */
    private $lastError = '';

    /**
     * @var array
     */
    private $subscribes = [];

    private ?Command $pending = null;

    /**
     * Create redis connection instance
     *
     * @param string|array $connection Redis connection config index name or config array.
     */
    public function __construct($connection='default')
    {
        $this->config = is_array($connection) ? $connection : config('redis.'.$connection);

        if ($this->config === null) {
            throw new Exception("Unable to find config for redis connection '$connection'.");
        }

        if (!\class_exists('Protocols\Redis')) {
            \class_alias('Wind\Redis\Protocol', 'Protocols\Redis');
        }

        $address = 'redis://'.$this->config['host'].':'.($this->config['port'] ?? 6379);
        $this->connection = new AsyncTcpConnection($address, $this->config['context'] ?? []);

        $this->queue = new SplQueue;
        $this->queue->setIteratorMode(SplQueue::IT_MODE_DELETE);

        $this->connect();
    }

    /**
     * connect
     *
     * @return void
     */
    public function connect()
    {
        if ($this->isConnected()) {
            return;
        }

        $timeout = $this->config['connect_timeout'] ?? 5;
        $reconnect = $this->config['auto_reconnect'] ?? true;

        //连接超时设置
        $connectTimer = Timer::add($timeout, function() {
            $this->connection->destroy();
            $this->connectDeferred?->error(new Exception('Connect to redis timeout.'));
            $this->connectDeferred = null;
        }, [], false);

        $this->connection->onConnect = function () use (&$connectTimer, $reconnect) {
            if ($connectTimer) {
                Timer::del($connectTimer);
                $connectTimer = null;
            }

            try {
                if ($this->password || !empty($this->config['auth'])) {
                    // echo "Auth...\n";
                    $this->auth($this->password ?: $this->config['auth']);
                }

                if ($this->db || isset($this->config['db'])) {
                    // echo "Select db...\n";
                    $this->select($this->db ?: $this->config['db']);
                }

                $this->lastError = '';
                $this->status = self::STATUS_CONNECTED;

                $this->connectDeferred->complete();
                $this->connectDeferred = null;

                $this->queuePaused = false;

                $this->process();

            } catch (Exception $e) {
                $this->connectDeferred?->error($e);
                $this->connectDeferred = null;
            }
        };

        $this->connection->onError = function ($connection, $code, $message) use (&$connectTimer, $reconnect) {
            if ($connectTimer) {
                Timer::del($connectTimer);
                $connectTimer = null;
            }

            $msg = "[$code] $message";
            $this->lastError = $msg;
        };

        $this->connection->onClose = function () use (&$connectTimer, $reconnect) {
            if ($connectTimer) {
                Timer::del($connectTimer);
                $connectTimer = null;
            }

            $this->queuePaused = true;

            if ($reconnect) {
                //将当前队列的内容先重新放进队列
                if ($this->pending) {
                    $this->queue->enqueue($this->pending);
                    $this->pending = null;
                }

                //自动重连时等待并尝试重连
                $reconnectDelay = $this->config['reconnect_delay'] ?? 2;
                echo "Reconnect after {$reconnectDelay} seconds cause: {$this->lastError}.\n";
                $this->status = self::STATUS_CONNECTING;
                Timer::add($reconnectDelay, fn() => EventLoop::queue($this->connect(...)), [], false);
            } else {
                $error = new Exception($this->lastError ?: 'Disconnected.');
                $this->status = self::STATUS_CLOSED;

                //不自动重连时，所有队列都报连接关闭错误
                if ($this->pending) {
                    $this->pending->deferred->error($error);
                    $this->pending = null;
                }

                while (!$this->queue->isEmpty()) {
                    $pending = $this->queue->dequeue();
                    $pending->deferred->error($error);
                }

                $this->connectDeferred?->error($error);
                $this->connectDeferred = null;
            }
        };

        $this->connection->onMessage = function ($connection, $data) {
            [$type, $result] = $data;

            //!号代表协议解析错误
            if ($type !== '-' && $type !== '!') {
                $this->lastError = '';

                if ($type === '+' && $result === 'OK') {
                    $result = true;
                } elseif (is_array($result) && $this->subscribes && ($result[0] == 'message' || $result[0] == 'pmessage')) {
                    //来自订阅的消息
                    $channel = $result[1];
                    if (isset($this->subscribes[$channel])) {
                        $this->subscribes[$channel](array_slice($result, 1));
                    } else {
                        echo "Unknown subscribe of channel {$channel}.\n";
                    }
                    return;
                }
            } else {
                $this->lastError = $result;
                $result = false;
            }

            if ($this->pending) {
                $pending = $this->pending;
                $this->pending = null;

                if ($result !== false) {
                    $pending->deferred->complete($result);
                } else {
                    $pending->deferred->error(new Exception($this->lastError));
                }

                $pending->callback();
            }

            if ($type === '!') {
                $this->close();
                $this->connect();
            } else {
                $this->process();
            }
        };

        $this->connectDeferred ??= new DeferredFuture();
        $this->status = self::STATUS_CONNECTING;

        EventLoop::defer($this->connection->connect(...));

        $this->connectDeferred->getFuture()->await();
    }

    public function isConnected()
    {
        return $this->status == self::STATUS_CONNECTED;
    }

    /**
     * process
     */
    protected function process()
    {
        if ($this->pending !== null || $this->queue->isEmpty() || $this->queuePaused) {
            return;
        }

        /** @var Command $command */
        $command = $this->queue->dequeue();
        $this->connection->send($command->buffer());
        $this->pending = $command;
    }

    public function select($db)
    {
        $result = $this->call('SELECT', [$db], $this->status == self::STATUS_CONNECTING);
        $this->db = $db;
        return $result;
    }

    public function auth($password)
    {
        $result = $this->call('AUTH', [$password], $this->status == self::STATUS_CONNECTING);
        $this->password = $password;
        return $result;
    }

    /**
     * subscribe
     *
     * @param callable $callback
     * @param array $channels
     */
    public function subscribe($callback , ...$channels)
    {
        $result = $this->call('SUBSCRIBE', $channels);
        foreach ($channels as $ch) {
            $this->subscribes[$ch] = $callback;
        }
        return $result;
    }

    public function unsubscribe(...$channels)
    {
        $result = $this->call('UNSUBSCRIBE', $channels);
        foreach ($channels as $ch) {
            unset($this->subscribes[$ch]);
        }
        return $result;
    }

    /**
     * psubscribe
     *
     * @param $patterns
     * @param $cb
     */
    public function pSubscribe($callback, ...$patterns)
    {
        $result = $this->call('PSUBSCRIBE', $patterns);
        foreach ($patterns as $ch) {
            $this->subscribes[$ch] = $callback;
        }
        return $result;
    }

    public function pUnsubscribe($patterns)
    {
        $result = $this->call('PUNSUBSCRIBE', $patterns);
        foreach ($patterns as $ch) {
            unset($this->subscribes[$ch]);
        }
        return $result;
    }

    public function multi()
    {
        return $this->call('MULTI', [], false, function() {
            $this->queuePaused = true;
        });
    }

    public function exec()
    {
        return $this->call('EXEC', [], true, function() {
            $this->queuePaused = false;
        });
    }

    public function discard()
    {
        return $this->call('DISCARD', [], true, function() {
            $this->queuePaused = false;
        });
    }

    /**
     * Start transaction
     *
     * @param callable(Transaction) $inTransactionCallback
     * @return array Result of exec command
     */
    public function transaction(callable $inTransactionCallback)
    {
        $this->multi();
        $transaction = new Transaction($this);
        try {
            $inTransactionCallback($transaction);
            return $this->exec();
        } catch (\Throwable $e) {
            $this->discard();
            throw $e;
        } finally {
            $this->process();
        }
    }

    /**
     * 发送指令
     *
     * @param string $cmd
     * @param array $args
     * @param boolean $direct Directly send or into the queue
     * @param \Closure $callback Atomic callback, see Command
     */
    public function call($cmd, $args, $direct=false, $callback=null)
    {
        if ($this->status == self::STATUS_CLOSED) {
            throw new Exception($this->lastError ?: 'Lost connection.');
        }

        $command = new Command($cmd, $args, $callback);

        if (!$direct) {
            $this->queue->enqueue($command);
            $this->process();
        } else {
            $this->connection->send($command->buffer());
            $this->pending = $command;
        }

        return $command->deferred->getFuture()->await();
    }

    public function __call($method, $args)
    {
        return $this->call($method, $args);
    }

    /**
     * error
     *
     * @return string
     */
    public function getLastError()
    {
        return $this->lastError;
    }

    /**
     * close
     */
    public function close()
    {
        if ($this->connection->getStatus() == TcpConnection::STATUS_CLOSED) {
            return;
        }

        $this->subscribes = [];
        $this->connection->onConnect = $this->connection->onError = $this->connection->onClose = $this->connection->onMessage = null;
        $this->connection->close();
    }

    public function __destruct()
    {
        $this->close();
    }

    /**
     * scan
     *
     * @throws Exception
     */
    public function scan()
    {
        throw new Exception('Not implemented');
    }

    /**
     * hScan
     *
     * @throws Exception
     */
    public function hScan()
    {
        throw new Exception('Not implemented');
    }

    /**
     * hScan
     *
     * @throws Exception
     */
    public function sScan()
    {
        throw new Exception('Not implemented');
    }

    /**
     * hScan
     *
     * @throws Exception
     */
    public function zScan()
    {
        throw new Exception('Not implemented');
    }

}
