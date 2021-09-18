<?php

namespace Wind\Redis;

use Amp\Deferred;
use Amp\Promise;
use Wind\Base\Countdown;
use Wind\Utils\ArrayUtil;
use Workerman\Redis\Client;
use Workerman\Redis\Exception;

use function Amp\asyncCallable;
use function Amp\await;

/**
 * Redis 协程客户端
 *
 * Strings methods
 * @method int append($key, $value)
 * @method int bitCount($key)
 * @method int decrBy($key, $value)
 * @method string|bool get($key)
 * @method int getBit($key, $offset)
 * @method string getRange($key, $start, $end)
 * @method string getSet($key, $value)
 * @method int incrBy($key, $value)
 * @method float incrByFloat($key, $value)
 * @method array mGet(array $keys)
 * @method void mSet(array $keys)
 * @method void mSetNx(array $keys)
 * @method array getMultiple(array $keys)
 * @method bool set($key, $value)
 * @method bool setBit($key, $offset, $value)
 * @method bool setEx($key, $ttl, $value)
 * @method bool pSetEx($key, $ttl, $value)
 * @method bool setNx($key, $value)
 * @method string setRange($key, $offset, $value)
 * @method int strLen($key)
 * @method int incr($key)
 * @method int decr($key)
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
 * @method array hMSet($key, array $array)
 * @method array hMGet($key, array $array)
 * @method array hGetAll($key)
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
 * @method void sort($key, $options)
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
 * @method void xGroup($command, $strKey, $strGroup, $strMsgId, $booMKStream = null)
 * @method void xInfo($command, $strStream, $strGroup = null)
 * @method int xLen($stream)
 * @method array xPending($strStream, $strGroup, $strStart = 0, $strEnd = 0, $iCount = 0, $strConsumer = null)
 * @method array xRange($strStream, $strStart, $strEnd, $iCount = 0)
 * @method array xRead($arrStreams, $iCount = 0, $iBlock = null)
 * @method array xReadGroup($strGroup, $strConsumer, $arrStreams, $iCount = 0, $iBlock = null)
 * @method array xRevRange($strStream, $strEnd, $strStart, $iCount = 0)
 * @method int xTrim($strStream, $iMaxLen, $booApproximate = null)
 * Pub/sub methods
 * @method void publish($channel, $message)
 * @method void pubSub($keyword, $argument = null)
 * Generic methods
 * @method void rawCommand(...$commandAndArgs)
 * Transactions methods
 * @method void watch($keys)
 * @method void unwatch($keys)
 * Scripting methods
 * @method mixed eval($script, $numKeys, ...$args=[])
 * @method mixed evalSha($sha1, $numKeys, ...$args=[])
 * @method void script($command, ...$scripts)
 * @method void client(...$args)
 * @method null|string getLastError()
 * @method bool clearLastError()
 * @method void _prefix($value)
 * @method void _serialize($value)
 * @method void _unserialize($value)
 * Introspection methods
 * @method bool isConnected()
 * @method void getHost()
 * @method void getPort()
 * @method false|int getDbNum()
 * @method false|double getTimeout()
 * @method void getReadTimeout()
 * @method void getPersistentID()
 * @method void getAuth()
 * @method void select($db)
 */
class Redis
{

    /**
     * Workerman Redis Client
     * @var Client
     */
    private $redis;

    /**
     * Redis host config
     *
     * @var array
     */
    private $config;

    /**
     * Connect status
     *
     * Promise: Connecting
     * true: Connected
     * false: Connect failed
     *
     * @var Promise|bool
     */
    private $connector = false;

    private $lastTrans;

    /**
     * 当前有多少正在排队、执行中的事务
     *
     * @var int
     */
    private $transCount = 0;

    /**
     * MULTI Transaction Countdown before call
     *
     * @var Countdown|null
     */
    private $pending;

    /**
     * Create redis connection instance
     *
     * @param string $name Redis connection config index name.
     */
    public function __construct($config='default')
    {
        $this->config = config('redis.'.$config);

        if ($this->config === null) {
            throw new Exception("Unable to find config for redis '$config'.");
        }

        $this->connect();
    }

    /**
     * Connect to redis
     *
     * Connect to redis if redis is not connect or connect already failed.
     *
     * @return bool
     */
    public function connect()
    {
        if ($this->connector === true) {
            return true;
        }

        if ($this->connector instanceof Promise) {
            return $this->connector;
        }

        $defer = new Deferred;

        $options = ArrayUtil::intersectKeys($this->config, ['connect_timeout', 'wait_timeout', 'context']);

        $this->redis = new Client("redis://{$this->config['host']}:{$this->config['port']}", $options, asyncCallable(function($status) use ($defer) {
            $error = null;

            if (!$status) {
                $error = new Exception($this->redis->error());
                goto Error;
            }

            if ($this->config['auth'] || $this->config['db']) {
                try {
                    //Password auth
                    $this->config['auth'] && $this->auth($this->config['auth']);
                    //Select DB
                    $this->config['db'] && $this->select($this->config['db']);
                } catch (\Throwable $e) {
                    $error = $e;
                    goto Error;
                }
            }

            //Success
            $defer->resolve();
            $this->connector = true;
            return;

            Error:
            $defer->fail($error);
            $this->connector = false;
        }));

        $this->connector = $defer->promise();

        return await($this->connector);
    }

    public function close()
    {
        $this->redis->close();
        return true;
    }

    /**
     * Begin Transaction use Multi
     *
     * Other operate with this redis connection will be block until transaction finished.
     *
     * @param callable $inTransactionCallback Callback to run in transaction code, support coroutine.
     *
     * ATTENTION: **DO NOT** get value by redis client in transaction, every operate in transaction will return `QUEUED`.
     *
     * Example:
     * ```
     * $redis->transaction(function($transaction) {
     *     $transaction->hSet('key', 'name', 'value');
     *     $this->lPush('key', 'value');
     * });
     * ```
     * @return array Return the results of every command in $inTransactionCallback returned.
     */
    public function transaction(callable $inTransactionCallback)
    {
        $defer = new Deferred;

        //将当前事务标记为下一个事务的前一个任务，让其等待此事务完成才能开始执行自己的事务
        $lastTrans = $this->lastTrans;
        $this->lastTrans = $defer->promise();

        ++$this->transCount;

        //等待前一个事务完成
        if ($lastTrans !== null) {
            await($lastTrans);
            $lastTrans = null;
        }

        $transaction = new Transaction($this->redis);
        $transaction->multi();

        try {
            $result = $inTransactionCallback($transaction);
            $transaction->exec();
            return $result;
        } catch (\Throwable $e) {
            $transaction->discard();
            throw $e;
        } finally {
            //每个事务完成后都减小独立命令（非事务命令）的前置等待事务数计数
            if ($this->pending !== null && $this->pending->countdown() == 0) {
                $this->pending = null;
            }
            --$this->transCount;
            $defer->resolve();
        }
    }

    public function __call($name, $args)
    {
        if ($this->connector instanceof Promise && $name != 'auth' && $name != 'select') {
            await($this->connector);
        }

        //等待队列中的事务完成后才开始发送消息
        //如果事务未完成，由于共用一个消息，此时发送会导致命令实际在 MULTI 生效范围内发送，
        //这种情况下会导致返回 "QUEUED" 问题。
        if ($this->transCount > 0) {
            //如果没有命令等待，则以当前排队的事务数为基数进行倒数，每个事务执行完后会减小此倒数
            //这样做法的好处是不必反复等待后面新排队进来的事务，而只需等待这一刻之前的事务完成即可
            //但有个问题，如果后面有新排队进来的事务，且前面的事务没有完成前，再往后排进来的独立命令都会加塞到此处执行，
            //导致后面的事务靠后执行，因为这种方式只支持一份独立命令的排队($this->>pending)
            //不过庆幸的是，前面的事务一旦执行完成，这个问题就会终止，新来的独立命令会继续往后排。
            if ($this->pending === null) {
                $this->pending = new Countdown($this->transCount);
            }
            await($this->pending->promise());
        }

        $defer = new Deferred;

        $args[] = static function($result, $redis) use ($defer) {
            if ($result !== false) {
                $defer->resolve($result);
            } else {
                $defer->fail(new Exception($redis->error()));
            }
        };

        call_user_func_array([$this->redis, $name], $args);

        return await($defer->promise());
    }

}
