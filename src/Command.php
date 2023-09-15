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
     * @return array
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

    public function __toString(): string
    {
        if (count($this->args) === 0) {
            return $this->cmd."\r\n";
        }

        $data = [$this->cmd, ...$this->args];

        $cmd = '';
        $count = \count($data);
        foreach ($data as $item) {
            if (\is_array($item)) {
                $count += \count($item) - 1;
                foreach ($item as $str) {
                    $cmd .= '$' . \strlen($str) . "\r\n$str\r\n";
                }
            } else {
                $cmd .= '$' . \strlen($item) . "\r\n$item\r\n";
            }
        }

        return "*$count\r\n$cmd";
    }

}
