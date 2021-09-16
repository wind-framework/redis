<?php

namespace Wind\Redis;

use Amp\Deferred;
use Amp\Promise;
use Amp\Success;
use Wind\Base\Countdown;
use Wind\Utils\ArrayUtil;
use Workerman\Redis\Client;
use Workerman\Redis\Exception;
use function Amp\asyncCoroutine;
use function Amp\call;

/**
 * Redis 协程客户端
 *
 * Strings methods
 * @method Promise append($key, $value) int
 * @method Promise bitCount($key) int
 * @method Promise decrBy($key, $value) int
 * @method Promise get($key) string|bool
 * @method Promise getBit($key, $offset) int
 * @method Promise getRange($key, $start, $end) string
 * @method Promise getSet($key, $value) string
 * @method Promise incrBy($key, $value) int
 * @method Promise incrByFloat($key, $value) float
 * @method Promise mGet(array $keys) array
 * @method Promise mSet(array $keys)
 * @method Promise mSetNx(array $keys)
 * @method Promise getMultiple(array $keys) array
 * @method Promise set($key, $value) bool
 * @method Promise setBit($key, $offset, $value) bool
 * @method Promise setEx($key, $ttl, $value) bool
 * @method Promise pSetEx($key, $ttl, $value) bool
 * @method Promise setNx($key, $value) bool
 * @method Promise setRange($key, $offset, $value) string
 * @method Promise strLen($key) int
 * @method Promise incr($key) int
 * @method Promise decr($key) int
 * Keys methods
 * @method Promise del(...$keys) int
 * @method Promise unlink(...$keys) int
 * @method Promise dump($key) false|string
 * @method Promise exists(...$keys) int
 * @method Promise expire($key, $ttl) bool
 * @method Promise pexpire($key, $ttl) bool
 * @method Promise expireAt($key, $timestamp) bool
 * @method Promise pexpireAt($key, $timestamp) bool
 * @method Promise keys($pattern) array
 * @method Promise scan($it) bool|array
 * @method Promise migrate($host, $port, $keys, $dbIndex, $timeout, $copy = false, $replace = false) void
 * @method Promise move($key, $dbIndex) bool
 * @method Promise object($information, $key) string|int|bool
 * @method Promise persist($key) bool
 * @method Promise randomKey() string
 * @method Promise rename($srcKey, $dstKey) bool
 * @method Promise renameNx($srcKey, $dstKey) bool
 * @method Promise type($key) string
 * @method Promise ttl($key) int
 * @method Promise pttl($key) int
 * @method Promise restore($key, $ttl, $value) void
 * Hashes methods
 * @method Promise hSet($key, $hashKey, $value) false|int
 * @method Promise hSetNx($key, $hashKey, $value) bool
 * @method Promise hGet($key, $hashKey) false|string
 * @method Promise hMSet($key, array $array) array
 * @method Promise hMGet($key, array $array) array
 * @method Promise hGetAll($key) array
 * @method Promise hLen($key) false|int
 * @method Promise hDel($key, ...$hashKeys) false|int
 * @method Promise hKeys($key) array
 * @method Promise hVals($key) array
 * @method Promise hExists($key, $hashKey) bool
 * @method Promise hIncrBy($key, $hashKey, $value) int
 * @method Promise hIncrByFloat($key, $hashKey, $value) float
 * @method Promise hScan($key, $iterator, $pattern = '', $count = 0) array
 * @method Promise hStrLen($key, $hashKey) int
 * Lists methods
 * @method Promise blPop($keys, $timeout) array
 * @method Promise brPop($keys, $timeout) array
 * @method Promise bRPopLPush($srcKey, $dstKey, $timeout) false|string
 * @method Promise lIndex($key, $index) false|string
 * @method Promise lInsert($key, $position, $pivot, $value) int
 * @method Promise lPop($key) false|string
 * @method Promise lPush($key, ...$entries) false|int
 * @method Promise lPushx($key, $value) false|int
 * @method Promise lRange($key, $start, $end) array
 * @method Promise lRem($key, $value, $count) false|int
 * @method Promise lSet($key, $index, $value) bool
 * @method Promise lTrim($key, $start, $end) false|array
 * @method Promise rPop($key) false|string
 * @method Promise rPopLPush($srcKey, $dstKey) false|string
 * @method Promise rPush($key, ...$entries) false|int
 * @method Promise rPushX($key, $value) false|int
 * @method Promise lLen($key) false|int
 * Sets methods
 * @method Promise sAdd($key, $value) int
 * @method Promise sCard($key) int
 * @method Promise sDiff($keys) array
 * @method Promise sDiffStore($dst, $keys) false|int
 * @method Promise sInter($keys) false|array
 * @method Promise sInterStore($dst, $keys) false|int
 * @method Promise sIsMember($key, $member) bool
 * @method Promise sMembers($key) array
 * @method Promise sMove($src, $dst, $member) bool
 * @method Promise sPop($key, $count = 0) false|string|array
 * @method Promise sRandMember($key, $count = 0) false|string|array
 * @method Promise sRem($key, ...$members) int
 * @method Promise sUnion(...$keys) array
 * @method Promise sUnionStore($dst, ...$keys) false|int
 * @method Promise sScan($key, $iterator, $pattern = '', $count = 0) false|array
 * Sorted sets methods
 * @method Promise bzPopMin($keys, $timeout) array
 * @method Promise bzPopMax($keys, $timeout) array
 * @method Promise zAdd($key, $score, $value) int
 * @method Promise zCard($key) int
 * @method Promise zCount($key, $start, $end) int
 * @method Promise zIncrBy($key, $value, $member) double
 * @method Promise zinterstore($keyOutput, $arrayZSetKeys, $arrayWeights = [], $aggregateFunction = '') int
 * @method Promise zPopMin($key, $count) array
 * @method Promise zPopMax($key, $count) array
 * @method Promise zRange($key, $start, $end, $withScores = false) array
 * @method Promise zRangeByScore($key, $start, $end, $options = []) array
 * @method Promise zRevRangeByScore($key, $start, $end, $options = []) array
 * @method Promise zRangeByLex($key, $min, $max, $offset = 0, $limit = 0) array
 * @method Promise zRank($key, $member) int
 * @method Promise zRevRank($key, $member) int
 * @method Promise zRem($key, ...$members) int
 * @method Promise zRemRangeByRank($key, $start, $end) int
 * @method Promise zRemRangeByScore($key, $start, $end) int
 * @method Promise zRevRange($key, $start, $end, $withScores = false) array
 * @method Promise zScore($key, $member) double
 * @method Promise zunionstore($keyOutput, $arrayZSetKeys, $arrayWeights = [], $aggregateFunction = '') int
 * @method Promise zScan($key, $iterator, $pattern = '', $count = 0) false|array
 * @method Promise sort($key, $options) Promise
 * HyperLogLogs methods
 * @method Promise pfAdd($key, $values) int
 * @method Promise pfCount($keys) int
 * @method Promise pfMerge($dstKey, $srcKeys) bool
 * Geocoding methods
 * @method Promise geoAdd($key, $longitude, $latitude, $member, ...$items) int
 * @method Promise geoHash($key, ...$members) array
 * @method Promise geoPos($key, ...$members) array
 * @method Promise geoDist($key, $members, $unit = '') double
 * @method Promise geoRadius($key, $longitude, $latitude, $radius, $unit, $options = []) int|array
 * @method Promise geoRadiusByMember($key, $member, $radius, $units, $options = []) array
 * Streams methods
 * @method Promise xAck($stream, $group, $arrMessages) int
 * @method Promise xAdd($strKey, $strId, $arrMessage, $iMaxLen = 0, $booApproximate = false) string
 * @method Promise xClaim($strKey, $strGroup, $strConsumer, $minIdleTime, $arrIds, $arrOptions = []) array
 * @method Promise xDel($strKey, $arrIds) int
 * @method Promise xGroup($command, $strKey, $strGroup, $strMsgId, $booMKStream = null)
 * @method Promise xInfo($command, $strStream, $strGroup = null)
 * @method Promise xLen($stream) int
 * @method Promise xPending($strStream, $strGroup, $strStart = 0, $strEnd = 0, $iCount = 0, $strConsumer = null) array
 * @method Promise xRange($strStream, $strStart, $strEnd, $iCount = 0) array
 * @method Promise xRead($arrStreams, $iCount = 0, $iBlock = null) array
 * @method Promise xReadGroup($strGroup, $strConsumer, $arrStreams, $iCount = 0, $iBlock = null) array
 * @method Promise xRevRange($strStream, $strEnd, $strStart, $iCount = 0) array
 * @method Promise xTrim($strStream, $iMaxLen, $booApproximate = null) int
 * Pub/sub methods
 * @method Promise publish($channel, $message)
 * @method Promise pubSub($keyword, $argument = null)
 * Generic methods
 * @method Promise rawCommand(...$commandAndArgs)
 * Transactions methods
 * @method Promise watch($keys)
 * @method Promise unwatch($keys)
 * Scripting methods
 * @method Promise eval($script, $numKeys, ...$args=[])
 * @method Promise evalSha($sha1, $numKeys, ...$args=[])
 * @method Promise script($command, ...$scripts)
 * @method Promise client(...$args)
 * @method Promise getLastError() null|string
 * @method Promise clearLastError() bool
 * @method Promise _prefix($value)
 * @method Promise _serialize($value)
 * @method Promise _unserialize($value)
 * Introspection methods
 * @method Promise isConnected() bool
 * @method Promise getHost()
 * @method Promise getPort()
 * @method Promise getDbNum() false|int
 * @method Promise getTimeout() false|double
 * @method Promise getReadTimeout()
 * @method Promise getPersistentID()
 * @method Promise getAuth()
 * @method Promise select($db)
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

    /**
     * Last transaction promise
     *
     * @var Promise
     */
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

    public function __construct($config='default')
    {
        $this->config = config('redis.'.$config);

        if ($this->config === null) {
            throw new Exception("Unable to find config for redis '$config'.");
        }

        Promise\rethrow($this->connect());
    }

    public function connect()
    {
        if ($this->connector === true) {
            return new Success();
        }

        if ($this->connector instanceof Promise) {
            return $this->connector;
        }

        $defer = new Deferred;

        $options = ArrayUtil::intersectKeys($this->config, ['connect_timeout', 'wait_timeout', 'context']);

        $this->redis = new Client("redis://{$this->config['host']}:{$this->config['port']}", $options, asyncCoroutine(function($status) use ($defer) {
            $error = null;

            if (!$status) {
                $error = new Exception($this->redis->error());
                goto Error;
            }

            if ($this->config['auth'] || $this->config['db']) {
                try {
                    //Password auth
                    $this->config['auth'] && yield $this->auth($this->config['auth']);
                    //Select DB
                    $this->config['db'] && yield $this->select($this->config['db']);
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

        return $this->connector = $defer->promise();
    }

    public function close()
    {
        $this->redis->close();
        return new Success();
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
     *     yield $transaction->hSet('key', 'name', 'value');
     *     yield $this->lPush('key', 'value');
     * });
     * ```
     * @return Promise Return the result of $inTransactionCallback returned.
     */
    public function transaction(callable $inTransactionCallback)
    {
        return call(function() use ($inTransactionCallback) {
            $defer = new Deferred;

            //将当前事务标记为下一个事务的前一个任务，让其等待此事务完成才能开始执行自己的事务
            $lastTrans = $this->lastTrans;
            $this->lastTrans = $defer->promise();

            ++$this->transCount;

            //等待前一个事务完成
            if ($lastTrans !== null) {
                yield $lastTrans;
                $lastTrans = null;
            }

            $transaction = new Transaction($this->redis);
            yield $transaction->multi();

            try {
                $result = yield call($inTransactionCallback, $transaction);
                yield $transaction->exec();
                return $result;
            } catch (\Throwable $e) {
                yield $transaction->discard();
                throw $e;
            } finally {
                //每个事务完成后都减小独立命令（非事务命令）的前置等待事务数计数
                if ($this->pending !== null && $this->pending->countdown() == 0) {
                    $this->pending = null;
                }
                --$this->transCount;
                $defer->resolve();
            }
        });
    }

    public function __call($name, $args)
    {
        return call(function() use ($name, $args) {
            if ($this->connector instanceof Promise && $name != 'auth' && $name != 'select') {
                yield $this->connector;
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
                yield $this->pending->promise();
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

            return $defer->promise();
        });
    }

}
