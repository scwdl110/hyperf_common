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

class ChannelModel extends Model
{
    protected $connection = 'erp_base';
    protected $table = 'channel';
}
