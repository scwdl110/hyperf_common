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
use App\Model\UserAdminModel;
use Captainbi\Hyperf\Base\Model;
use Captainbi\Hyperf\Util\Unique;

class UserDepartmentModel extends Model
{
    protected $connection = 'erp_base';
    protected $table = 'user_department';


    const DEPARTMENT_USER_ADMIN_IDS_REDIS_KEY='department_user_admin_ids_';

    private $redis;

    public function __construct(array $attributes = [])
    {
        $this->redis = new Redis();
        parent::__construct($attributes);
    }


    private function getUsersByDepartmentId($department_id,$user_id)
    {
        if(empty($department_id))
        {
            return [];
        }
        $all_department_ids = $this->getAllChildAndSelfDeparmentId($user_id, $department_id);
        $where = array(
            ['user_id', '=', $user_id],
            ['status',  '<>', 2]
        );
        $data = Unique::getArray(UserAdminModel::where($where)->whereIn("user_department_id",$all_department_ids)->select("id"));
        $user_related_ids = empty($data)?[]:array_column($data,'id');
        $redis_key = self::DEPARTMENT_USER_ADMIN_IDS_REDIS_KEY . $department_id;
        $this->redis->set($redis_key, $user_related_ids,4*3600);
        return $user_related_ids;
    }



    public function getAllChildAndSelfDeparmentId($user_id = 0  ,$department_id = 0 , $is_master = 0){
        if($is_master == 1 && $department_id == 0 ) {
            $lists = Unique::getArray(self::where("user_id",'=',$user_id)->whereIn("status",[1,2])->select("id,level")) ;
            $result = array() ;
            if(!empty($lists)){
                $result =array_column($lists , 'id') ;
            }
            return $result ;
        } else if($department_id == 0){
            return array() ;
        }else{
            $result[] = $department_id ;
            $lists = Unique::getArray(self::where("user_id",'=',$user_id)->where("parent_id",'=',$department_id)->whereIn("status",[1,2])->select("id,level")) ;
            if(empty($lists)){
                return $result ;
            }else{
                $rt = array_column($lists , 'id') ;
                $result = array_merge($result , $rt) ;
                if($lists[0]['level'] == 3){
                    return $result ;
                }else{
                    $lists2 = Unique::getArray(self::where("user_id",'=',$user_id)->whereIn("parent_id",'=',$rt)->whereIn("status",[1,2])->select("id,level")) ;
                    if(empty($lists2)){
                        return $result ;
                    }else{
                        $rt2 = array_column($lists2 , 'id') ;
                        $result = array_merge($result , $rt2) ;
                        return $result ;
                    }
                }
            }
        }

    }
}
