<?php


namespace Captainbi\Hyperf\DbConnection\Pool;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ConnectionInterface;
use Hyperf\Contract\SessionInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Captainbi\Hyperf\DbConnection\Connection;
use Hyperf\DbConnection\Frequency;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Pool\Pool;
use Hyperf\Utils\Arr;
use Psr\Container\ContainerInterface;
use function Captainbi\Hyperf\appendDbCodeno;

class DbPool extends Pool
{
    /**
     * @Inject()
     * @var SessionInterface
     */
    private $session;

    /**
     * @var StdoutLoggerInterface
     */
    private $logger;

    protected $name;

    protected $config;

    public function __construct(ContainerInterface $container, string $name)
    {
        $this->name = $name;
        $config = $container->get(ConfigInterface::class);
        $key = sprintf('databases.%s', $this->name);
        if (! $config->has($key)) {
            throw new \InvalidArgumentException(sprintf('config[%s] is not exist!', $key));
        }
        // Rewrite the `name` of the configuration item to ensure that the model query builder gets the right connection.
        $config->set("{$key}.name", $name);

        $this->config = $config->get($key);

        $options = Arr::get($this->config, 'pool', []);

        $this->frequency = make(Frequency::class, [$this]);
        parent::__construct($container, $options);
    }

    public function getName(): string
    {
        return $this->name;
    }

    protected function createConnection(): ConnectionInterface
    {
        $this->config['database'] = appendDbCodeno($this->config['database']);
        return new Connection($this->container, $this, $this->config);
    }

}