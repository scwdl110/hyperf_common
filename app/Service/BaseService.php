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
namespace App\Service;

use App\Model\ChannelModel;
use App\Model\User\UserAdminRolePrivModel;
use App\Model\User\UserDepartmentModel;
use Captainbi\Hyperf\Base\Service;
use App\Model\UserAdminModel;
use App\Model\UserModel;
use Captainbi\Hyperf\Exception\BusinessException;
use Hyperf\Redis\Redis;
use Hyperf\Utils\Context;
use Psr\Http\Message\ServerRequestInterface;

class BaseService extends Service {


    /**
     * 获取当前用户信息
     * @return mixed
     */

    public function getUserInfo($admin_id = null, $user_id = null)
    {
        if ($admin_id == null && $user_id == null) {
            $context = Context::get(ServerRequestInterface::class);
            $userInfo = $context->getAttribute('userInfo');
            if (empty($userInfo)) {
                throw new BusinessException();
            }
        } else {
            $userInfo['admin_id'] = $admin_id;
            $userInfo['user_id'] = $user_id;
            $user_admin_data = UserAdminModel::query()->select("is_master")->where([['id', '=', $admin_id]])->first();
            $user_data = UserModel::query()->select("codeno", "dbhost")->where([['id', '=', $user_id]])->first();
            if (empty($user_admin_data) || empty($user_data)) {
                throw new BusinessException();
            }
            $userInfo['is_master'] = $user_admin_data->is_master;
            $userInfo['codeno'] = $user_data->codeno;
            $userInfo['dbhost'] = $user_data->dbhost;
        }

        return $userInfo;
    }

    /**
     * 获取商品权限信息
     * @param $priv_key string 权限key
     * @param $userInfo array 用户信息
     * @return array
     */
    public function getUserGoodsPriv($priv_key,$userInfo){
        $goods_privilege = ['priv_key' => $priv_key, 'priv_value' => UserAdminRolePrivModel::GOODS_PRIV_VALUE_NONE, 'priv_user_admin_ids' => [], "related_user_admin_ids_str"=> "","operation_channel_ids_arr" => []];

        //没传key直接返回全部权限
        if (empty($priv_key)) {
            $goods_privilege['priv_value'] = UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL;
            return $goods_privilege;
        }

        //主账号直接返回所有可见权限
        $is_master = $userInfo['is_master'];
        if ($is_master == 1) {
            $goods_privilege['priv_value'] = UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL;
            return $goods_privilege;
        }

        //获取子账号权限
        $user_id = $userInfo['user_id'];
        $admin_id = $userInfo['admin_id'];

        $user_admin_info   = UserAdminModel::getModel()->getUserAdminInfo($user_id,[$admin_id]);
        $user_admin_info   = $user_admin_info[0];
        //2.用户数据不存在或角色为空时，menu_id为空时返回无权限,正常情况所有用户应该都有一个角色
        if (empty($user_admin_info) || empty($user_admin_info['role_id'])) {
            $goods_privilege['priv_value'] = UserAdminRolePrivModel::GOODS_PRIV_VALUE_NONE;
            return $goods_privilege;
        }
        $role_privilege = UserAdminRolePrivModel::getModel()->getUserRolePrivByArray([$user_admin_info['role_id']], $priv_key);
        if ($role_privilege['priv_value'] == UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL) //角色未配置权限或当前模块未配置权限时返回所有可见权限
        {
            $goods_privilege['priv_value'] = UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL;
            return $goods_privilege;
        }
        $goods_privilege = $role_privilege;
        if ($role_privilege['priv_value'] == UserAdminRolePrivModel::GOODS_PRIV_VALUE_RELATED_USER) //权限为关联人可见时，获取部门负责人下面的员工id作为关联人id返回
        {
            //获取关联人信息
            $user_related_ids=[$admin_id];
            if($user_admin_info['is_responsible']==1) {
                $user_related_ids = UserDepartmentModel::getModel()->getUsersByDepartmentId($user_admin_info['user_department_id'],$user_id);
                $user_related_ids = array_unique(array_merge($user_related_ids, [$admin_id]));

                //部门负责人下面有人是全部可见或者是主账号时，部门负责人直接返回全部可见
//                $tmp_related_ids=array_diff($user_related_ids,[$admin_id]);
//                if(!empty($tmp_related_ids))
//                {
//                    $user_related_list=UserAdminModel::getModel()->getUserAdminInfo($user_id,$tmp_related_ids);
//                    if(!empty($user_related_list))
//                    {
//                        $master_related= array_filter($user_related_list,function ($item){
//                            return $item['is_master']==1;
//                        });
//                        if(count($master_related)>0)
//                        {
//                            $goods_privilege['priv_value'] = UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL;
//                            return $goods_privilege;
//                        }
//                        $user_related_role_ids  =array_column($user_related_list,'role_id');
//                        $user_related_role_priv_list = UserAdminRolePrivModel::getModel()->getUserRolePrivByArray($user_related_role_ids, $priv_key);
//                        if ($user_related_role_priv_list['priv_value'] == UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL) //角色未配置权限或当前模块未配置权限时返回所有可见权限
//                        {
//                            $goods_privilege['priv_value'] = UserAdminRolePrivModel::GOODS_PRIV_VALUE_ALL;
//                            return $goods_privilege;
//                        }
//                    }
//                }
            }
            $goods_privilege['related_user_admin_ids_str'] = !empty($user_related_ids) ? implode(",", $user_related_ids) : "{$admin_id}";

            //获取按店铺运营的数据，筛选出店铺运营人员为当前用户关联人的店铺IDS

            $channel_operation_arr=ChannelModel::getModel()->getChannelOperationPatternList($user_id);
            if(!empty($channel_operation_arr))
            {
                $channel_operation_arr=array_filter($channel_operation_arr,function ($item) use($user_related_ids){
                    return in_array($item['operation_user_admin_id'],$user_related_ids);
                });
                $channel_operation_arr= array_column($channel_operation_arr,'id');
            }
            $goods_privilege['operation_channel_ids_arr']= !empty($channel_operation_arr) ? $channel_operation_arr:[];

        }
        return $goods_privilege;
    }

}
