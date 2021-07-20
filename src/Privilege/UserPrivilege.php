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
//        $url = $request->url();
//        $url = '/cgfd/m1/c1/a1/51';
//        $url = '/cgfd/m1/c1/a1';
        $url = '/gfdc/gfd/m1-c1-a1.php';
//        $url = '/fsd/index.php?m=m1&c=c1&a=a1';
        $container = ApplicationContext::getContainer();
        // 通过 DI 容器直接注入
        $request = $container->get(RequestInterface::class);
        $requestData = $request->all();
        $res = $this->urlHasPrivilege($url, $requestData);
        return $res;

    }


    /**
     * url判断权限
     * @param $url
     * @return bool
     */
    public function urlHasPrivilege($url, $requestData){
        //获取权限
        $menuResource = $this->getMenuResource();
        if(!$menuResource['code']){
            return false;
        }
        $menuResource = $menuResource['resource'];

        $sourceUrlArr = parse_url((string)$url);
        if(!isset($sourceUrlArr['path'])){
            return false;
        }

        //有漏的再增加(判断权限)
        if(isset($requestData['m']) && isset($requestData['c']) && isset($requestData['a'])){
            //?m=&c=&a=
            foreach ($menuResource as $k=>$v){
                $urlArr = parse_url((string)$v);
                if(!isset($urlArr['query'])){
                    continue;
                }
                $query = '&'.$urlArr['query'].'&';
                $mResult = preg_match('/&m=(.*?)&/', $query, $modelArray);
                $cResult = preg_match('/&c=(.*?)&/', $query, $controlArray);
                $aResult = preg_match('/&a=(.*?)&/', $query, $methodArray);
                if(!$mResult || !$cResult || !$aResult){
                    continue;
                }
                if($modelArray[1] == $requestData['m'] && $controlArray[1] == $requestData['c'] && $methodArray[1] == $requestData['a']){
                    return true;
                }
            }


        }else{
            //是否有占位符
            $placeholder = 0;
            if(preg_match('/(\/\d+\/)|(\/\d+$)/', $sourceUrlArr['path'], $patArray)){
                //例子:id作为id的占位符(先处理占位符)
                $patterns = [
                    '/\/\d+\//',
                    '/\/\d+$/'
                ];
                $replacements = [
                    "/:id/",
                    "/:id"
                ];
                $sourceUrlArr['path'] = preg_replace($patterns, $replacements, $sourceUrlArr['path']);
                $placeholder = 1;
            }

            //剩下正常pathinfo
            $sourcePathArr = explode('/',trim($sourceUrlArr['path'], '/'));
            foreach ($menuResource as $k=>$v){
                $urlArr = parse_url($v);
                if(!isset($urlArr['path'])){
                    continue;
                }

                if($placeholder){
                    //占位符处理
                    $patterns = [
                        '/\/:(.*?)\//',
                        '/\/:(.*)$/'
                    ];
                    $replacements = [
                        "/:id/",
                        "/:id"
                    ];
                    $urlArr['path'] = preg_replace($patterns, $replacements, $urlArr['path']);
                }


                //m/c/a   /m-c-a.php
                $pathArr = explode('/',trim($urlArr['path'], '/'));
                if(end($pathArr) == end($sourcePathArr)){
                    //退出标志
                    $flag = 1;
                    //相等计数
                    $eqNum = 1;
                    //总计数
                    $totalNum = count($pathArr);
                    while($flag){
                        //往前对比
                        $path = prev($pathArr);
                        $sourcePath = prev($sourcePathArr);
                        if($path === FALSE){
                            $flag=0;
                        }elseif($path==$sourcePath){
                            $eqNum++;
                        }
                    }

                    if($eqNum == $totalNum){
                        return true;
                    }
                    var_dump($pathArr,$sourcePathArr);exit;
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