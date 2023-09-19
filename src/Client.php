<?php

namespace Wind\Redis;

use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Revolt\EventLoop;
use SplQueue;

use function Amp\Socket\connect;

/**
 * Wind Framework Redis Client
 */
class Client
{

    use Commands;

    private array $config = [];

    private Socket $socket;

    private int $status = 0;

    private const STATUS_CLOSED = 0;
    private const STATUS_CONNECTING = 1;
    private const STATUS_CONNECTED = 2;

    private SplQueue $queue;

    /**
     * @var int
     */
    private $db = 0;

    /**
     * @var string|array|null
     */
    private $password = null;

    /**
     * @var array
     */
    private $subscribes = [];

    private bool $processing = false;
    private bool $queuePaused = false;

    /**
     * Create redis connection instance
     *
     * @param string|array $connection Redis connection config index name or config array.
     */
    public function __construct($connection='default')
    {
        if (is_array($connection)) {
            $this->config = $connection;
        } else {
            $this->config = config('redis.'.$connection);
            if ($this->config === null) {
                throw new Exception("Unable to find config for redis connection '$connection'.");
            }
        }

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
        if ($this->status == self::STATUS_CONNECTED) {
            return;
        }

        $this->status = self::STATUS_CONNECTING;

        $address = $this->config['host'].':'.($this->config['port'] ?? 6379);
        $timeout = $this->config['connect_timeout'] ?? 5;

        $connectContext = (new ConnectContext)
            ->withConnectTimeout($timeout);

        try {
            $this->socket = connect($address, $connectContext);
        } catch (\Throwable $e) {

            $reconnect = $this->config['auto_reconnect'] ?? true;

            if ($reconnect) {
                $reconnectDelay = $this->config['reconnect_delay'] ?? 2;
                echo "Redis reconnect after {$reconnectDelay} seconds cause: {$e->getMessage()}.\n";
                EventLoop::delay($reconnectDelay, $this->connect(...));
                return;
            } else {
                throw $e;
            }
        }

        // $this->socket->onClose(function() {
        //     echo "Redis connection closed\n";
        // });

        if ($this->password || !empty($this->config['auth'])) {
            $this->auth($this->password ?: $this->config['auth']);
        }

        $this->status = self::STATUS_CONNECTED;

        //重新恢复队列
        if ($this->queuePaused) {
            $this->queuePaused = false;
            $this->process();
        }
    }

    public function auth($password)
    {
        $result = $this->call('AUTH', [$password], $this->status == self::STATUS_CONNECTING);
        $this->password = $password;
        return $result;
    }

    public function select($db)
    {
        $result = $this->call('SELECT', [$db], $this->status == self::STATUS_CONNECTING);
        $this->db = $db;
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

    protected function multi()
    {
        $this->call('MULTI');
        $this->queuePaused = true;
    }

    protected function exec()
    {
        $arr = $this->call('EXEC', direct: true);
        $this->queuePaused = false;
        return $arr;
    }

    protected function discard()
    {
        $this->call('DISCARD', direct: true);
        $this->queuePaused = false;
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
            echo $e;
            $this->discard();
            throw $e;
        } finally {
            $this->process();
        }
    }

    public function call($cmd, $args=[], $direct=false)
    {
        $command = new Command($cmd, $args);

        if (!$direct) {
            $this->queue->enqueue($command);
            $this->process();
        } else {
            $this->execCommand($command);
        }

        return $command->deferred->getFuture()->await();
    }

    public function __call($method, $args)
    {
        return $this->call($method, $args);
    }

    private function process()
    {
        if ($this->processing || $this->queue->isEmpty() || $this->queuePaused) {
            return;
        }

        $this->processing = true;

        EventLoop::queue(function() {
            while (!$this->queue->isEmpty()) {
                if ($this->queuePaused) {
                    break;
                }
                /** @var Command $command */
                $cmd = $this->queue->dequeue();
                $this->execCommand($cmd);
            }
            $this->processing = false;
        });
    }

    private function execCommand(Command $cmd)
    {
        // echo "Send: ".$cmd->__toString();

        $this->socket->write($cmd->__toString());
        $buffer = $this->socket->read();

        if ($buffer === null) {
            echo "Redis connection is closed\n";
            echo 'Readable: '.($this->socket->isReadable() ? 'true' : 'false')."\n";
            echo 'Writeable: '.($this->socket->isWritable() ? 'true' : 'false')."\n";
            echo 'IsClosed: '.($this->socket->isClosed() ? 'true' : 'false')."\n";

            $this->queuePaused = true;
            $this->queue->enqueue($cmd);

            $this->socket->close();
            $this->status = self::STATUS_CLOSED;

            $this->connect();

            return;
        }

        [$type, $data] = $this->decode($buffer);

        //!号代表协议解析错误
        if ($type !== '-' && $type !== '!') {
            if ($type === '+' && $data === 'OK') {
                $cmd->deferred->complete(true);
            } else {
                $cmd->deferred->complete($data);
            }
        } else {
            $cmd->deferred->error(new Exception($data));
        }

        // echo "Get: $type ".(is_array($data) ? print_r($data, true) : $data)."\n";
    }

    private function decode($buffer)
    {
        $type = $buffer[0];
        switch ($type) {
            case ':':
                return [$type, (int) substr($buffer, 1)];
            case '+':
                return [$type, \substr($buffer, 1, strlen($buffer) - 3)];
            case '-':
                return [$type, \substr($buffer, 1, strlen($buffer) - 3)];
            case '$':
                if (0 === strpos($buffer, '$-1')) {
                    return [$type, null];
                }
                $pos = \strpos($buffer, "\r\n");
                return [$type, \substr($buffer, $pos + 2, (int)substr($buffer, 1, $pos))];
            case '*':
                if (0 === strpos($buffer, '*-1')) {
                    return [$type, null];
                }
                $pos = \strpos($buffer, "\r\n");
                $value = [];
                $count = (int)substr($buffer, 1, $pos - 1);
                while ($count --) {
                    $next_pos = strpos($buffer, "\r\n", $pos + 2);
                    if (!$next_pos) {
                        return 0;
                    }
                    $sub_type = $buffer[$pos + 2];
                    switch ($sub_type) {
                        case ':':
                            $value[] = (int) substr($buffer, $pos + 3, $next_pos - $pos - 3);
                            $pos = $next_pos;
                            break;
                        case '+':
                            $value[] = substr($buffer, $pos + 3, $next_pos - $pos - 3);
                            $pos = $next_pos;
                            break;
                        case '-':
                            $value[] = substr($buffer, $pos + 3, $next_pos - $pos - 3);
                            $pos = $next_pos;
                            break;
                        case '$':
                            if($pos + 2 === strpos($buffer, '$-1', $pos)) {
                                $pos = $next_pos;
                                $value[] = null;
                                break;
                            }
                            $length = (int)substr($buffer, $pos + 3, $next_pos - $pos -3);
                            $value[] = substr($buffer, $next_pos + 2, $length);
                            $pos = $next_pos + $length + 2;
                            break;
                        default:
                            return ['!', "protocol error, got '$sub_type' as reply type byte. buffer:".bin2hex($buffer)." pos:$pos"];
                    }
                }
                return [$type, $value];
            default:
                return ['!', "protocol error, got '$type' as reply type byte."];

        }
    }

    /**
     * close
     */
    public function close()
    {
        if ($this->socket->isClosed()) {
            return;
        }

        $this->subscribes = [];
        $this->socket->close();
        $this->status = self::STATUS_CLOSED;
    }

    public function __destruct()
    {
        $this->close();
    }

}
