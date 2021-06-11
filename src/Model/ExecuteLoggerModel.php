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

namespace Captainbi\Hyperf\Model;

use Captainbi\Hyperf\Base\Model;

class ExecuteLoggerModel extends Model
{
    public $timestamps = true;
    const CREATED_AT = 'create_time';
    const UPDATED_AT = 'modified_time';
    protected $dateFormat = 'U';

    protected $connection = 'default';
    protected $table = 'execute_logger';

    protected $fillable = ['admin_id','user_id','execute_sql'];
}
