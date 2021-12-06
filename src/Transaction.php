<?php

namespace Wind\Redis;

use Amp\DeferredFuture;
use Amp\Promise;
use Workerman\Redis\Client;

use function Amp\await;

/**
 * Redis Transaction
 *
 * @mixin Redis
 * @method Promise multi()
 * @method Promise exec()
 * @method Promise discard()
 */
class Transaction
{

    private $redis;

    public function __construct(Client $redis)
    {
        $this->redis = $redis;
    }

    public function __call($name, $args)
    {
        $defer = new DeferredFuture;

        $args[] = static function($result) use ($defer) {
            $defer->complete($result);
        };

        call_user_func_array([$this->redis, $name], $args);

        return $defer->getFuture()->await();
    }

}
