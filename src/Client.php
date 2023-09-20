<?php

namespace Wind\Redis;

use Amp\Socket\ConnectContext;
use Amp\Socket\Socket;
use Wind\Socket\SimpleTextClient;

use function Amp\Socket\connect;

/**
 * Wind Framework Redis Client
 */
class Client extends SimpleTextClient
{

    use Commands;

    private array $config = [];

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

    /**
     * Create redis connection instance
     *
     * @param string|array $connection Redis connection config index name or config array.
     */
    public function __construct($connection='default')
    {
        parent::__construct();

        if (is_array($connection)) {
            $this->config = $connection;
        } else {
            $this->config = config('redis.'.$connection);
            if ($this->config === null) {
                throw new Exception("Unable to find config for redis connection '$connection'.");
            }
        }

        $this->autoReconnect = $this->config['auto_reconnect'] ?? true;

        if (isset($this->config['reconnect_delay'])) {
            $this->reconnectDelay = $this->config['reconnect_delay'];
        }

        $this->connect();
    }

    protected function createSocket(): Socket
    {
        $address = $this->config['host'].':'.($this->config['port'] ?? 6379);
        $timeout = $this->config['connect_timeout'] ?? 5;

        $connectContext = (new ConnectContext)
            ->withConnectTimeout($timeout);

        return connect($address, $connectContext);
    }

    protected function authenticate()
    {
        if ($this->password || !empty($this->config['auth'])) {
            $this->auth($this->password ?: $this->config['auth']);
        }

        $this->select($this->db);
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
        return $this->execute(new Command($cmd, $args), $direct);
    }

    public function __call($method, $args)
    {
        return $this->call($method, $args);
    }

    /**
     * close
     */
    protected function cleanResources()
    {
        $this->subscribes = [];
    }

    public function __destruct()
    {
        $this->close();
    }

}
