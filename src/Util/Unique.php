<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Hyperf\Snowflake\IdGeneratorInterface;
use Hyperf\Utils\ApplicationContext;

class Unique {
    /**
     * 返回雪花id
     * @return string
     */
    public static function snowflake()
    {
        $container = ApplicationContext::getContainer();
        $generator = $container->get(IdGeneratorInterface::class);

        $snowflakeId = (string)$generator->generate();
        return $snowflakeId;
    }


}