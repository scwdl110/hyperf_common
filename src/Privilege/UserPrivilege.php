<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Privilege;

use Captainbi\Hyperf\Model\UserAdminModel;
use Captainbi\Hyperf\Model\UserAdminNewMenuModel;
use Hyperf\Contract\ConfigInterface;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\Utils\ApplicationContext;

class UserPrivilege
{
    //是否拥有权限
    public function isPrivilege()
    {
        $container = ApplicationContext::getContainer();
        // 通过 DI 容器直接注入
        $request = $container->get(RequestInterface::class);
        $requestData = $request->all();
        $url = $request->url();

        //切割url
        $urlPathArr = explode('/', $url);
        $last = count($urlPathArr)-1;

        //有漏的再增加
        if(isset($requestData['m']) && isset($requestData['c']) && isset($requestData['a'])){
            //?m=&c=&a=

        }elseif(preg_match_all('/(.*)-(.*)-(.*)/', $urlPathArr[$last], $patArray)){
            //m-c-a

        }else{
            preg_match_all('/(.*)\/(.*)\/(.*)/', $urlPathArr[$last], $patArray);
            //m/c/a



            //正常pathinfo不适配
            //例子:id作为id的占位符
            $urlArr = parse_url($url);

        }
        return true;
    }

    /**
     * 获取用户菜单资源
     * @return array
     */

    public function getMenuResource(){
        //上测试环境后去掉用户
        //补上redis缓存
        $container = ApplicationContext::getContainer();
        // 通过 DI 容器获取或直接注入 RedisFactory 类
        $config = $container->get(ConfigInterface::class);
        $sessionPrefix = $config->get("session.options.key_prefix");
        $userId = $session[$sessionPrefix.'user_id']??304;
        $adminId = $session[$sessionPrefix.'admin_id']??128030965;
        if(!$userId || !$adminId){
            return [
                'code' => 0,
                'msg'  => trans('user.user_not_login'),
            ];
        }

        //菜单资源id
        $where = [
            'a.id' => $adminId,
            'a.user_id' => $userId,
            'a.status' => 1,
            'c.status' => 1
        ];
        $mUserAdminModel = UserAdminModel::query()->from('user_admin as a')
            ->join('user_admin_role_relation as b', 'a.id', '=', 'b.admin_id')
            ->join('user_admin_role as c', 'b.role_id', '=', 'c.id')
            ->join('user_admin_role_priv as d', 'c.id', '=', 'd.role_id')
            ->where($where)
            ->select('d.new_menu_id')
            ->get();

        if(!$mUserAdminModel){
            return [
                'code' => 0,
                'msg'  => trans('user.menu_not_privilege'),
            ];
        }

        //合并去重
        $menuArrId = [];
        foreach ($mUserAdminModel as $k=>$v){
            if(!$v['new_menu_id']){
                continue;
            }
            $menuArrId = array_merge($menuArrId, explode(',', $v['new_menu_id']));
        }

        $menuArrId = array_unique($menuArrId);

        //菜单资源
        $resourceArr = [];
        $where = [
            ['is_deleted', '=', 0],
        ];
        $mUserAdminNewMenuModel = UserAdminNewMenuModel::query()->where($where)->whereIn('id', $menuArrId)->select('resource')->get();
        if(!$mUserAdminNewMenuModel){
            return [
                'code' => 0,
                'msg'  => trans('user.menu_not_privilege'),
            ];
        }
        //合并去重
        foreach ($mUserAdminNewMenuModel as $k=>$v){
            if(!$v['resource']){
                continue;
            }
            $resourceArr = array_merge($resourceArr, preg_split('/[\r\n]/s', $v['resource']));
        }

        $resourceArr = array_unique($resourceArr);

        //去除空值和重新排列
        $newResourceArr = [];
        foreach ($resourceArr  as $k=>$v){
            if(!$v){
                continue;
            }
            $newResourceArr[] = $v;
        }



        return [
            'code' => 1,
            'resource'  => $newResourceArr
        ];
    }


}