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

class AccessLoggerModel extends Model
{
    public $timestamps = true;
    const CREATED_AT = 'create_time';
    const UPDATED_AT = 'modified_time';
    protected $dateFormat = 'U';

    protected $connection = 'default';
    protected $table = 'access_logger';

    protected $fillable = ['admin_id','user_id','access_url','query_string','http_header','http_method','http_params'];
}
