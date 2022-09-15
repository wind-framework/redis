<?php

namespace Wind\Redis;

use Amp\DeferredFuture;
use Revolt\EventLoop;
use SplQueue;
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Timer;

/**
 * Wind Redis Client
 *
 * Strings methods
 * @method int append($key, $value)
 * @method int bitCount($key)
 * @method int decr($key)
 * @method int decrBy($key, $value)
 * @method string|bool get($key)
 * @method int getBit($key, $offset)
 * @method string getRange($key, $start, $end)
 * @method string getSet($key, $value)
 * @method int incr($key)
 * @method int incrBy($key, $value)
 * @method float incrByFloat($key, $value)
 * @method array mGet(array $keys)
 * @method array getMultiple(array $keys)
 * @method bool setBit($key, $offset, $value)
 * @method bool setEx($key, $ttl, $value)
 * @method bool pSetEx($key, $ttl, $value)
 * @method bool setNx($key, $value)
 * @method string setRange($key, $offset, $value)
 * @method int strLen($key)
 * Keys methods
 * @method int del(...$keys)
 * @method int unlink(...$keys)
 * @method false|string dump($key)
 * @method int exists(...$keys)
 * @method bool expire($key, $ttl)
 * @method bool pexpire($key, $ttl)
 * @method bool expireAt($key, $timestamp)
 * @method bool pexpireAt($key, $timestamp)
 * @method array keys($pattern)
 * @method bool|array scan($it)
 * @method void migrate($host, $port, $keys, $dbIndex, $timeout, $copy = false, $replace = false)
 * @method bool move($key, $dbIndex)
 * @method string|int|bool object($information, $key)
 * @method bool persist($key)
 * @method string randomKey()
 * @method bool rename($srcKey, $dstKey)
 * @method bool renameNx($srcKey, $dstKey)
 * @method string type($key)
 * @method int ttl($key)
 * @method int pttl($key)
 * @method void restore($key, $ttl, $value)
 * Hashes methods
 * @method false|int hSet($key, $hashKey, $value)
 * @method bool hSetNx($key, $hashKey, $value)
 * @method false|string hGet($key, $hashKey)
 * @method false|int hLen($key)
 * @method false|int hDel($key, ...$hashKeys)
 * @method array hKeys($key)
 * @method array hVals($key)
 * @method bool hExists($key, $hashKey)
 * @method int hIncrBy($key, $hashKey, $value)
 * @method float hIncrByFloat($key, $hashKey, $value)
 * @method array hScan($key, $iterator, $pattern = '', $count = 0)
 * @method int hStrLen($key, $hashKey)
 * Lists methods
 * @method array blPop($keys, $timeout)
 * @method array brPop($keys, $timeout)
 * @method false|string bRPopLPush($srcKey, $dstKey, $timeout)
 * @method false|string lIndex($key, $index)
 * @method int lInsert($key, $position, $pivot, $value)
 * @method false|string lPop($key)
 * @method false|int lPush($key, ...$entries)
 * @method false|int lPushx($key, $value)
 * @method array lRange($key, $start, $end)
 * @method false|int lRem($key, $value, $count)
 * @method bool lSet($key, $index, $value)
 * @method false|array lTrim($key, $start, $end)
 * @method false|string rPop($key)
 * @method false|string rPopLPush($srcKey, $dstKey)
 * @method false|int rPush($key, ...$entries)
 * @method false|int rPushX($key, $value)
 * @method false|int lLen($key)
 * Sets methods
 * @method int sAdd($key, $value)
 * @method int sCard($key)
 * @method array sDiff($keys)
 * @method false|int sDiffStore($dst, $keys)
 * @method false|array sInter($keys)
 * @method false|int sInterStore($dst, $keys)
 * @method bool sIsMember($key, $member)
 * @method array sMembers($key)
 * @method bool sMove($src, $dst, $member)
 * @method false|string|array sPop($key, $count = 0)
 * @method false|string|array sRandMember($key, $count = 0)
 * @method int sRem($key, ...$members)
 * @method array sUnion(...$keys)
 * @method false|int sUnionStore($dst, ...$keys)
 * @method false|array sScan($key, $iterator, $pattern = '', $count = 0)
 * Sorted sets methods
 * @method array bzPopMin($keys, $timeout)
 * @method array bzPopMax($keys, $timeout)
 * @method int zAdd($key, $score, $value)
 * @method int zCard($key)
 * @method int zCount($key, $start, $end)
 * @method double zIncrBy($key, $value, $member)
 * @method int zinterstore($keyOutput, $arrayZSetKeys, $arrayWeights = [], $aggregateFunction = '')
 * @method array zPopMin($key, $count)
 * @method array zPopMax($key, $count)
 * @method array zRange($key, $start, $end, $withScores = false)
 * @method array zRangeByScore($key, $start, $end, $options = [])
 * @method array zRevRangeByScore($key, $start, $end, $options = [])
 * @method array zRangeByLex($key, $min, $max, $offset = 0, $limit = 0)
 * @method int zRank($key, $member)
 * @method int zRevRank($key, $member)
 * @method int zRem($key, ...$members)
 * @method int zRemRangeByRank($key, $start, $end)
 * @method int zRemRangeByScore($key, $start, $end)
 * @method array zRevRange($key, $start, $end, $withScores = false)
 * @method double zScore($key, $member)
 * @method int zunionstore($keyOutput, $arrayZSetKeys, $arrayWeights = [], $aggregateFunction = '')
 * @method false|array zScan($key, $iterator, $pattern = '', $count = 0)
 * HyperLogLogs methods
 * @method int pfAdd($key, $values)
 * @method int pfCount($keys)
 * @method bool pfMerge($dstKey, $srcKeys)
 * Geocoding methods
 * @method int geoAdd($key, $longitude, $latitude, $member, ...$items)
 * @method array geoHash($key, ...$members)
 * @method array geoPos($key, ...$members)
 * @method double geoDist($key, $members, $unit = '')
 * @method int|array geoRadius($key, $longitude, $latitude, $radius, $unit, $options = [])
 * @method array geoRadiusByMember($key, $member, $radius, $units, $options = [])
 * Streams methods
 * @method int xAck($stream, $group, $arrMessages)
 * @method string xAdd($strKey, $strId, $arrMessage, $iMaxLen = 0, $booApproximate = false)
 * @method array xClaim($strKey, $strGroup, $strConsumer, $minIdleTime, $arrIds, $arrOptions = [])
 * @method int xDel($strKey, $arrIds)
 * @method mixed xGroup($command, $strKey, $strGroup, $strMsgId, $booMKStream = null)
 * @method mixed xInfo($command, $strStream, $strGroup = null)
 * @method int xLen($stream)
 * @method array xPending($strStream, $strGroup, $strStart = 0, $strEnd = 0, $iCount = 0, $strConsumer = null)
 * @method array xRange($strStream, $strStart, $strEnd, $iCount = 0)
 * @method array xRead($arrStreams, $iCount = 0, $iBlock = null)
 * @method array xReadGroup($strGroup, $strConsumer, $arrStreams, $iCount = 0, $iBlock = null)
 * @method array xRevRange($strStream, $strEnd, $strStart, $iCount = 0)
 * @method int xTrim($strStream, $iMaxLen, $booApproximate = null)
 * Pub/sub methods
 * @method mixed publish($channel, $message)
 * @method mixed pubSub($keyword, $argument = null)
 * Generic methods
 * @method mixed rawCommand(...$commandAndArgs)
 * Transactions methods
 * @method mixed multi()
 * @method mixed exec()
 * @method mixed discard()
 * @method mixed watch($keys)
 * @method mixed unwatch($keys)
 * Scripting methods
 * @method mixed eval($script, $args = [], $numKeys = 0)
 * @method mixed evalSha($sha, $args = [], $numKeys = 0)
 * @method mixed script($command, ...$scripts)
 * @method mixed client(...$args)
 * @method string getLastError()
 */
class Redis
{

    private AsyncTcpConnection $connection;

    private int $status = 0;

    private const STATUS_CLOSED = 0;
    private const STATUS_CONNECTING = 1;
    private const STATUS_CONNECTED = 2;

    private ?DeferredFuture $connectDeferred;

    private array $config = [];

    private SplQueue $queue;
    private bool $queuePaused = true;

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
                echo "Auth...\n";
                if ($this->password || !empty($this->config['auth'])) {
                    $this->auth($this->password ?: $this->config['auth']);
                }

                echo "Select db...\n";
                if ($this->db || isset($this->config['db'])) {
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
                    $this->pending->error($error);
                    $this->pending = null;
                }

                while (!$this->queue->isEmpty()) {
                    $pending = $this->queue->dequeue();
                    $pending->error($error);
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
                } elseif (is_array($result) && ($result[0] == 'message' || $result[0] == 'pmessage')) {
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
                if ($result !== false) {
                    $this->pending->complete($result);
                } else {
                    $this->pending->error(new Exception($this->lastError));
                }
                $this->pending = null;
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
        if (!$this->isConnected() || $this->pending || $this->queue->isEmpty()) {
            return;
        }

        /** @var Command $cmd */
        $cmd = $this->queue->dequeue();
        $this->connection->send($cmd->buffer());
        $this->pending = $cmd;
    }

    public function select($db)
    {
        $result = $this->__call('SELECT', [$db]);
        $this->db = $db;
        return $result;
    }

    public function auth($password)
    {
        $result = $this->__call('AUTH', [$password]);
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
        $result = $this->__call('SUBSCRIBE', $channels);
        foreach ($channels as $ch) {
            $this->subscribes[$ch] = $callback;
        }
        return $result;
    }

    public function unsubscribe(...$channels)
    {
        $result = $this->__call('UNSUBSCRIBE', $channels);
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
        $result = $this->__call('PSUBSCRIBE', $patterns);
        foreach ($patterns as $ch) {
            $this->subscribes[$ch] = $callback;
        }
        return $result;
    }

    public function pUnsubscribe($patterns)
    {
        $result = $this->__call('PUNSUBSCRIBE', $patterns);
        foreach ($patterns as $ch) {
            unset($this->subscribes[$ch]);
        }
        return $result;
    }

    /**
     * set
     *
     * @param string $key
     * @param string $value
     * @param int $ex
     * @param int $px
     * @param string $opt NX or XX
     * @return bool
     */
    public function set($key, $value, $ex=null, $px=null, $opt=null)
    {
        $args = [$key, $value];
        $ex !== null && array_push($args, 'EX', $ex);
        $px !== null && array_push($args, 'PX', $px);
        $opt && $args[] = $opt;
        return $this->__call('SET', $args);
    }

    /**
     * sort
     *
     * @param $key
     * @param $options
     * @param null $cb
     */
    function sort($key, $options)
    {
        $args = [$key];
        if (isset($options['sort'])) {
            $args[] = $options['sort'];
            unset($options['sort']);
        }

        foreach ($options as $op => $value) {
            $args[] = $op;
            if (!is_array($value)) {
                $args[] = $value;
                continue;
            }
            foreach ($value as $sub_value) {
                $args[] = $sub_value;
            }
        }

        return $this->__call('SORT', $args);
    }

    /**
     * mSet
     *
     * @param array $array
     * @return bool
     */
    public function mSet(array $array)
    {
        return $this->mapCb('MSET', $array);
    }

    /**
     * mSetNx
     *
     * @param array $array
     * @return int
     */
    public function mSetNx(array $array)
    {
        return $this->mapCb('MSETNX', $array);
    }

    /**
     * mapCb
     *
     * @param string $command
     * @param array $array
     * @return mixed
     */
    protected function mapCb($command, array $array)
    {
        $args = [$command];
        foreach ($array as $key => $value) {
            $args[] = $key;
            $args[] = $value;
        }
        return $this->__call($command, $args);
    }

    /**
     * hMSet
     *
     * @param string $key
     * @param array $array
     * @return bool
     */
    public function hMSet($key, array $array)
    {
        $args = [$key];

        foreach ($array as $k => $v) {
            array_push($args, $k, $v);
        }

        return $this->__call('HMSET', $args);
    }

    /**
     * hMGet
     *
     * @param $key
     * @param array $fields
     * @return array
     */
    public function hMGet($key, array $fields)
    {
        $result = $this->__call('HMGET', array_merge($key, $fields));
        if (!is_array($result)) {
            return $result;
        }
        return array_combine($fields, $result);
    }

    /**
     * hGetAll
     *
     * @param $key
     * @param null $cb
     */
    public function hGetAll($key)
    {
        $result = $this->__call('HGETALL', [$key]);

        if (!is_array($result)) {
            return $result;
        }

        $data = [];

        foreach (array_chunk($result, 2) as $row) {
            list($k, $v) = $row;
            $data[$k] = $v;
        }

        return $data;
    }

    /**
     * 事务
     */
    public function transaction(callable $inTransactionCallback)
    {
        $this->multi();
        $this->queuePaused = true;
        try {
            $inTransactionCallback($this);
            $this->exec();
        } catch (\Throwable $e) {
            $this->discard();
        }
        $this->queuePaused = false;
    }

    /**
     * 添加命令到队列
     *
     * @param $method
     * @param $args
     * @return mixed
     */
    public function __call($method, $args)
    {
        if ($this->status == self::STATUS_CLOSED) {
            throw new Exception($this->lastError ?: 'Lost connection.');
        }

        $cmd = new Command($method, $args);

        //当处于普通命令时，命令全部都队列机制，当系统连接、处理事务中时，则整个连接是原子性的，此时不走命令机制，且队列被暂停
        if (!$this->queuePaused) {
            $this->queue->enqueue($cmd);
            $this->process();
        } else {
            $this->connection->send([$method, ...$args]);
            $this->pending = $cmd;
        }

        return $cmd->await();
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
        if (!$this->connection) {
            return;
        }

        $this->subscribes = [];
        $this->connection->onConnect = $this->connection->onError = $this->connection->onClose =
        $this->connection->onMessge = null;
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
