<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Log;

use Hyperf\Utils\ApplicationContext;
use Psr\Container\ContainerInterface;

class StdoutLoggerFactory
{
    public function __invoke(ContainerInterface $container)
    {
        $loggerFactory = ApplicationContext::getContainer()->get("Hyperf\Logger\LoggerFactory");
        return $loggerFactory->get('log', 'sys');
    }
}