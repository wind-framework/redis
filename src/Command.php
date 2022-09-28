<?php

namespace Wind\Redis;

use Amp\DeferredFuture;
use Closure;

class Command {

    public DeferredFuture $deferred;

    /**
     * @param string $cmd
     * @param array $args
     * @param Closure $callback And atomic callback after current command and before next command.
     */
    public function __construct(public string $cmd, public array $args, private ?Closure $callback=null)
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

    public function callback()
    {
        if ($this->callback !== null) {
            $closure = $this->callback;
            $closure();
        }
    }

}
