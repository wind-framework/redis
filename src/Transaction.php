<?php

namespace Wind\Redis;

use Amp\Deferred;
use Amp\Promise;
use Workerman\Redis\Client;

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
        $defer = new Deferred;

        $args[] = function($result) use ($defer) {
            $defer->resolve($result);
        };

        call_user_func_array([$this->redis, $name], $args);

        return $defer->promise();
    }

}
