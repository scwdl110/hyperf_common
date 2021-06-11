<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace App\Model;

use Captainbi\Hyperf\Base\Model;

class BaseModel extends Model
{
    //时间戳字段
    public $timestamps = true;
    const CREATED_AT = 'create_time';
    const UPDATED_AT = 'modified_time';
    protected $dateFormat = 'U';

}
