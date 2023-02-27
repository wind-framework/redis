<?php

namespace Wind\Redis;

/**
 * Shared commands with transaction
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
 * @method mixed eval($script, $numKeys = 0, ...$args)
 * @method mixed evalSha($sha, $numKeys = 0, ...$args)
 * @method mixed script($command, ...$scripts)
 * @method mixed client(...$args)
 */
trait Commands
{

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

}
