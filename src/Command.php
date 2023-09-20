<?php

namespace Wind\Redis;

use Amp\DeferredFuture;
use Amp\Future;
use Closure;
use Wind\Socket\SimpleTextCommand;

class Command implements SimpleTextCommand {

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

    public function getFuture(): Future
    {
        return $this->deferred->getFuture();
    }

    public function encode(): string
    {
        if (count($this->args) === 0) {
            return strtoupper($this->cmd)."\r\n";
        }

        $data = [strtoupper($this->cmd), ...$this->args];

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

    private function decode($buffer)
    {
        $type = $buffer[0];
        switch ($type) {
            case ':':
                return [$type, (int) substr($buffer, 1)];
            case '+':
                return [$type, \substr($buffer, 1, strlen($buffer) - 3)];
            case '-':
                return [$type, \substr($buffer, 1, strlen($buffer) - 3)];
            case '$':
                if (0 === strpos($buffer, '$-1')) {
                    return [$type, null];
                }
                $pos = \strpos($buffer, "\r\n");
                return [$type, \substr($buffer, $pos + 2, (int)substr($buffer, 1, $pos))];
            case '*':
                if (0 === strpos($buffer, '*-1')) {
                    return [$type, null];
                }
                $pos = \strpos($buffer, "\r\n");
                $value = [];
                $count = (int)substr($buffer, 1, $pos - 1);
                while ($count --) {
                    $next_pos = strpos($buffer, "\r\n", $pos + 2);
                    if (!$next_pos) {
                        return 0;
                    }
                    $sub_type = $buffer[$pos + 2];
                    switch ($sub_type) {
                        case ':':
                            $value[] = (int) substr($buffer, $pos + 3, $next_pos - $pos - 3);
                            $pos = $next_pos;
                            break;
                        case '+':
                            $value[] = substr($buffer, $pos + 3, $next_pos - $pos - 3);
                            $pos = $next_pos;
                            break;
                        case '-':
                            $value[] = substr($buffer, $pos + 3, $next_pos - $pos - 3);
                            $pos = $next_pos;
                            break;
                        case '$':
                            if($pos + 2 === strpos($buffer, '$-1', $pos)) {
                                $pos = $next_pos;
                                $value[] = null;
                                break;
                            }
                            $length = (int)substr($buffer, $pos + 3, $next_pos - $pos -3);
                            $value[] = substr($buffer, $next_pos + 2, $length);
                            $pos = $next_pos + $length + 2;
                            break;
                        default:
                            return ['!', "protocol error, got '$sub_type' as reply type byte. buffer:".bin2hex($buffer)." pos:$pos"];
                    }
                }
                return [$type, $value];
            default:
                return ['!', "protocol error, got '$type' as reply type byte."];

        }
    }

    public function resolve(string|\Throwable $buffer)
    {
        if ($buffer instanceof \Throwable) {
            $this->deferred->error($buffer);
            return;
        }

        [$type, $data] = $this->decode($buffer);

        //!号代表协议解析错误
        if ($type !== '-' && $type !== '!') {
            if ($type === '+' && $data === 'OK') {
                $this->deferred->complete(true);
            } else {
                $this->deferred->complete($data);
            }
        } else {
            $this->deferred->error(new Exception($data));
        }
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

}
