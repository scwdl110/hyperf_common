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
use Captainbi\Hyperf\Util\Redis;
use Captainbi\Hyperf\Util\Unique;

class ChannelModel extends Model
{
    protected $connection = 'erp_base';
    protected $table = 'channel';



    const CHANNEL_OPERATION_IDS_REDIS_KEY='jdx_channel_operation_ids_';

    private $redis;


    public function getChannelOperationPatternList($user_id)
    {
        $redis =new Redis();
        $this->redis = $redis->getClient('bi');
        $redis_key = self::CHANNEL_OPERATION_IDS_REDIS_KEY . $user_id;
        $channel_operation_data = $this->redis->get($redis_key);
        if (false===$channel_operation_data) {
            $where = array(
                ['user_id', '=', $user_id],
                ['goods_operation_pattern', '=', 2],
                ['operation_user_admin_id', '>', 0],
                ['status',  '<=', 1]
            );
            $channel_operation_data = Unique::getArray(ChannelModel::where($where)->get(["id","operation_user_admin_id"]));
            $this->redis->set($redis_key, serialize($channel_operation_data),4*60*60);
        }else{
            $channel_operation_data = unserialize($channel_operation_data);
        }
        $channel_operation_data = !empty($channel_operation_data) ? $channel_operation_data : [];
        return $channel_operation_data;
    }

}
