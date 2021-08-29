<?php

namespace Wind\Redis;

use Amp\Deferred;
use Amp\Promise;
use Wind\Base\Config;
use Workerman\Redis\Client;
use Workerman\Redis\Exception;

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
 * @method Promise sort($key, $options)
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
 * @method Promise xGroup($command, $strKey, $strGroup, $strMsgId, $booMKStream = null)
 * @method Promise xInfo($command, $strStream, $strGroup = null)
 * @method int xLen($stream)
 * @method array xPending($strStream, $strGroup, $strStart = 0, $strEnd = 0, $iCount = 0, $strConsumer = null)
 * @method array xRange($strStream, $strStart, $strEnd, $iCount = 0)
 * @method array xRead($arrStreams, $iCount = 0, $iBlock = null)
 * @method array xReadGroup($strGroup, $strConsumer, $arrStreams, $iCount = 0, $iBlock = null)
 * @method array xRevRange($strStream, $strEnd, $strStart, $iCount = 0)
 * @method int xTrim($strStream, $iMaxLen, $booApproximate = null)
 * Pub/sub methods
 * @method Promise publish($channel, $message)
 * @method Promise pubSub($keyword, $argument = null)
 * Generic methods
 * @method Promise rawCommand(...$commandAndArgs)
 * Transactions methods
 * @method Promise watch($keys)
 * @method Promise unwatch($keys)
 * Scripting methods
 * @method Promise eval($script, $args = [], $numKeys = 0)
 * @method Promise evalSha($sha, $args = [], $numKeys = 0)
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
    private $connectPromise;

    /**
     * MULTI Transaction Promise
     *
     * @var Promise|null
     */
    private $pending;

    public function __construct(Config $config)
    {
        $conf = $config->get('redis.default');

        $defer = new Deferred;
        $this->redis = new Client("redis://{$conf['host']}:{$conf['port']}", [], function($status, $redis) use ($defer, $conf) {
            if ($status) {
                if ($conf['auth'] || $conf['db']) {
                    try {
                        if ($conf['auth']) {
                            $r = $this->auth($conf['auth']);
                            if (!$r) {
                                throw new Exception("Auth failed to redis {$conf['host']}:{$conf['port']}.");
                            }
                        }
                        if ($conf['db'] && $conf['db'] != 0) {
                            $this->select($conf['db']);
                        }
                        $defer->resolve();
                    } catch (\Throwable $e) {
                        $defer->fail($e);
                    }
                } else {
                    $defer->resolve();
                }
            } else {
                $defer->fail(new Exception("Connected to redis server error."));
            }
        });
        $this->connectPromise = $defer->promise();
    }

    public function connect()
    {
        return await($this->connectPromise);
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
     * @return Promise Return the result of $inTransactionCallback returned.
     */
    public function transaction(callable $inTransactionCallback)
    {
        $this->pending && await($this->pending);

        $defer = new Deferred;
        $this->pending = $defer->promise();

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
            $defer->resolve();
            $this->pending = $transaction = null;
        }
    }

    public function __call($name, $args)
    {
        $this->pending && await($this->pending);

        $defer = new Deferred;

        $args[] = static function($result) use ($defer) {
            $defer->resolve($result);
        };

        call_user_func_array([$this->redis, $name], $args);

        return await($defer->promise());
    }

}
