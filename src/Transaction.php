<?php

namespace Wind\Redis;

/**
 * Transaction operation object
 */
class Transaction
{

    use Commands;

    public function __construct(private Redis $redis)
    {
    }

    /**
     * 因为事务执行在单个连接中是原子的，排它的，所以事务的 __call 不排队。
     * 此时普通 Redis 命令会进入暂停的队列中等待，而事务的命令在此处执行。
     * 同链接的其它事务需要借助普通命令 multi 开启，所以其它事务也只能等此事务执行完后队列恢复运行才会被启动。
     *
     * @param $method
     * @param $args
     * @return mixed
     */
    public function __call($method, $args)
    {
        return $this->redis->call($method, $args, true);
    }

}
