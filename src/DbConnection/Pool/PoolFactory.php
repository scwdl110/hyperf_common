<?php

declare(strict_types=1);

namespace Captainbi\Hyperf\DbConnection\Pool;

use Hyperf\Contract\SessionInterface;
use Hyperf\Di\Container;
use Psr\Container\ContainerInterface;
use function Captainbi\Hyperf\appendDbCodeno;

class PoolFactory
{
    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var DbPool[]
     */
    protected $pools = [];

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    public function getPool(string $name): DbPool
    {

        $key = appendDbCodeno($name, 1);
        if (isset($this->pools[$key])) {
            return $this->pools[$key];
        }

        if ($this->container instanceof Container) {
            $pool = $this->container->make(DbPool::class, ['name' => $name]);
        } else {
            $pool = new DbPool($this->container, $name);
        }

        return $this->pools[$key] = $pool;
    }
}