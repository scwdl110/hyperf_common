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
    /**
     * 是否拥有权限
     * @return bool
     */
    public function isPrivilege()
    {
        $menuResource = $this->getMenuResource();
        if(!$menuResource['code']){
            return false;
        }

        $container = ApplicationContext::getContainer();
        // 通过 DI 容器直接注入
        $request = $container->get(RequestInterface::class);
        $requestData = $request->all();
        $url = $request->url();
        $this->urlHasPrivilieg($url);

    }


    /**
     * url判断权限
     * @param $url
     * @return bool
     */
    public function urlHasPrivilieg($url){
        //切割url
        $urlPathArr = explode('/', $url);
        $last = count($urlPathArr)-1;

        //有漏的再增加(判断权限)
        if(isset($requestData['m']) && isset($requestData['c']) && isset($requestData['a'])){
            //?m=&c=&a=
            foreach ($menuResource as $k=>$v){
                $urlArr = parse_url($v);
                if(!isset($urlArr['query'])){
                    continue;
                }
                $query = '&'.$urlArr['query'].'&';
                preg_match_all('/&m=(.*)&/', $query, $modelArray);
                preg_match_all('/&c=(.*)&/', $query, $controlArray);
                preg_match_all('/&a=(.*)&/', $query, $methodArray);
                if($modelArray[1] == $requestData['m'] && $controlArray[1] == $requestData['c'] && $methodArray[1] == $requestData['a']){
                    return true;
                }
            }

        }elseif(preg_match_all('/(.*)-(.*)-(.*)\.php/', $urlPathArr[$last], $patArray)){
            //m-c-a.php
            foreach ($menuResource as $k=>$v){
                $urlArr = parse_url($v);
                if(!isset($urlArr['path'])){
                    continue;
                }
                preg_match_all('/(.*)-(.*)-(.*)\.php/', $urlArr['path'], $resultArray);
                if($resultArray[1] == $patArray[1] && $resultArray[2] == $patArray[2] && $resultArray[3] == $patArray[3]){
                    return true;
                }
            }

        }else{
            $sourceUrlArr = parse_url($url);
            if(!isset($sourceUrlArr['path'])){
                return false;
            }
            preg_match_all('/(.*)\/(.*)\/(.*)$/', $sourceUrlArr['path'], $patArray);
            foreach ($menuResource as $k=>$v){
                $urlArr = parse_url($v);
                if(!isset($urlArr['path'])){
                    continue;
                }
                //m/c/a
                preg_match_all('/(.*)\/(.*)\/(.*)$/', $urlArr['path'], $resultArray);
                if($resultArray[1] == $patArray[1] && $resultArray[2] == $patArray[2] && $resultArray[3] == $patArray[3]){
                    return true;
                }

                //剩下正常pathinfo不适配
                //例子:id作为id的占位符
                $sourcePath = preg_replace('/\d/', ':id', $sourceUrlArr['path']);
                $newPath = preg_replace('/:(.*?)(?!\s)/', ':id', $urlArr['path']);
                if($sourcePath==$newPath){
                    return true;
                }
            }


        }
        return false;
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