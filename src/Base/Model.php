<?php


namespace Captainbi\Hyperf\Base;

use Hyperf\DbConnection\Model\Model as BaseModel;
use Hyperf\ModelCache\Cacheable;
use Hyperf\ModelCache\CacheableInterface;

class Model extends BaseModel implements CacheableInterface
{
    use Cacheable;
}