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
namespace App\Model\user;

use App\Lib\Redis;
use Captainbi\Hyperf\Base\Model;
use Captainbi\Hyperf\Util\Unique;

class UserAdminRolePrivModel extends Model
{
    protected $connection = 'erp_base';
    protected $table = 'user_admin_role_priv';

    const GOODS_PRIV_VALUE_ALL          = 1;
    const GOODS_PRIV_VALUE_RELATED_USER = 2;
    const GOODS_PRIV_VALUE_NONE         = 3;

    const ROLE_GOODS_PRIV_REDIS_KEY='role_goods_priv_';

    private $redis;

    public function __construct(array $attributes = [])
    {
        $this->redis = new Redis();
        parent::__construct($attributes);
    }


    public function getUserRolePrivByArray($role_ids ,$priv_key = ''){

        $return_priv = [];
        $priv_value = self::GOODS_PRIV_VALUE_ALL;

        $role_id_arr = is_array($role_ids) ? $role_ids : explode(",",$role_ids);

        //获取角色商品权限缓存数据--编辑角色商品权限时刷新缓存
        $not_in_redis = array();
        foreach ($role_id_arr as $role_id){
            $redis_key = self::ROLE_GOODS_PRIV_REDIS_KEY . $role_id;
            $goods_priv_list = $this->redis->get($redis_key);
            if ($goods_priv_list === false) {//不存在redis需要查询
                $not_in_redis[] = $role_id;
            }else{
                //存在直接返回
                $return_priv[$role_id] = array_column($goods_priv_list, null, 'priv_key');
            }

        }

        //不存在的批量查询
        if (!empty($not_in_redis)){
            $db_role_priv = Unique::getArray(self::whereIn("role_id",$not_in_redis)->select("goods_priv,role_id"));
            if (!empty($db_role_priv)){
                foreach ($db_role_priv as $value){
                    $redis_key       = self::ROLE_GOODS_PRIV_REDIS_KEY . $value['role_id'];
                    $goods_priv_list = !empty($value['goods_priv']) ? @json_decode($value['goods_priv'], true) : [];
                    $this->redis->set($redis_key, $goods_priv_list);
                    $return_priv[$value['role_id']] = array_column($goods_priv_list, null, 'priv_key');
                }
            }
        }

        //遍历最大权限
        if (!empty($return_priv)){
            $priv_value = self::GOODS_PRIV_VALUE_NONE;
            foreach ($return_priv as $item){
                if (isset($item[$priv_key]['priv_value'])){
                    if ($item[$priv_key]['priv_value'] < $priv_value){
                        $priv_value = $item[$priv_key]['priv_value'];
                    }
                }else{
                    $priv_value = self::GOODS_PRIV_VALUE_ALL;
                }

            }

        }

        $priv_data = ['priv_key' => $priv_key, 'priv_value' => $priv_value];
        return $priv_data;

    }
}
