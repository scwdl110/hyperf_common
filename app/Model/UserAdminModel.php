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

use App\Lib\Redis;
use Captainbi\Hyperf\Base\Model;
use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Unique;
use Hyperf\DbConnection\Db;

class UserAdminModel extends Model
{
    public $timestamps = true;
    protected $connection = 'erp_base';
    protected $table = 'user_admin';

    private $redis;

    const USER_ADMIN_INFO_REDIS_KEY = 'user_admin_info_cache_';

    public function __construct(array $attributes = [])
    {
        $this->redis = new Redis();
        parent::__construct($attributes);
    }


    /**
     *  //用户信息缓存: 用户角色、是否部门负责人，目前用于获取用户商品权限
     * //仅编辑用户信息时缓存，删除角色时不刷新缓存，因为删除角色后，用户登录后没有菜单权限，所以不用处理
     * @param $user_id
     * @param $admin_id
     * @param $user_admin_info
     */
    public function saveUserAdminCache($user_id, $admin_id, $user_admin_info)
    {
        if (empty($user_id) || empty($admin_id) || empty($user_admin_info))
            return;

        $redis_key = self::USER_ADMIN_INFO_REDIS_KEY . "{$user_id}_{$admin_id}";
        $this->redis->set($redis_key, $user_admin_info,4*3600);
    }

    /**
     * 获取用户角色ID，默认取缓存
     * @param $user_id
     * @param $admin_ids
     * @return int|mixed
     */
    public function getUserAdminInfo($user_id, $admin_ids)
    {
        if (empty($user_id) || empty($admin_ids))
            return 0;
        $admin_ids=!is_array($admin_ids) ? explode(",",(string)$admin_ids):$admin_ids;
        $un_cached_ids = array();
        $user_admin_redis_data = array();
        foreach ($admin_ids as $admin_id){
            $redis_key = self::USER_ADMIN_INFO_REDIS_KEY . "{$user_id}_{$admin_id}";
            $user_admin_info = $this->redis->get($redis_key);
            if ($user_admin_info === false){
                $un_cached_ids[] = $admin_id;
            }else{
                $user_admin_redis_data[] = $user_admin_info;
            }
        }
        if (!empty($un_cached_ids)){
            $where = array(
                ['user_id', '=', $user_id],
            );
            $columns = "id,user_id,role_id,is_responsible,user_department_id,is_master";
            $user_admin_info_list = Unique::getArray(self::where($where)->whereIn("id",$un_cached_ids)->select(Db::raw($columns))->get());
            foreach ($user_admin_info_list as $item) {
                $this->saveUserAdminCache($user_id, $item['id'], $item);
                $user_admin_redis_data[]=$item;
            }
        }
        return array_values($user_admin_redis_data);
    }

}
