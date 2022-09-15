<?php

namespace Wind\Redis;

use Amp\DeferredFuture;

class Command {

    private DeferredFuture $deferred;

    public function __construct(public string $cmd, public array $args)
    {
        $this->deferred = new DeferredFuture();
    }

    /**
     * Buffer to send
     * return array
     */
    public function buffer()
    {
        return [$this->cmd, ...$this->args];
    }

    public function complete($result)
    {
        $this->deferred->complete($result);
    }

    public function error($error)
    {
        $this->deferred->error($error);
    }

    /**
     * @return mixed
     */
    public function await()
    {
        return $this->deferred->getFuture()->await();
    }

}

