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

class UserExtInfoModel extends BaseModel
{
    protected $table = 'user_ext_info';
    protected $fillable = ['uuid', 'admin_id','client_id','ext_info'];
}
