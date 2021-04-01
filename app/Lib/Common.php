<?php

namespace App\Lib;

use App\Model\UserModel;
use Hyperf\HttpServer\Contract\RequestInterface;
use App\Service\UserService;
use Hyperf\Di\Annotation\Inject;
use App\Lib\Redis;

class Common
{

    /**
     * @Inject()
     * @var UserService
     */
    protected $userService;

    /**
     * @Inject()
     * @var RequestInterface
     */
    protected $requset;

    /**
     * @Inject()
     * @var Redis
     */
    protected $redis;

    function getDbCode()
    {
        $user_info = self::getUserInfo();
        if (empty($user_info)) {
            return false;
        } else {
            $rt = array('dbhost' => $user_info['dbhost'], 'codeno' => $user_info['codeno']);
            return $rt;
        }
    }


    function getUserInfo()
    {
        $user_id = (int)$this->requset->input('user_id');
        if ($user_id > 0) {
            $user_info = $this->redis->get('COMMON_API_USERINFO_' . $user_id);
            if (empty($user_info)) {
                $user_info = $this->userService->getUserInfo($user_id);
                if (!empty($user_info)) {
                    $this->redis->set('COMMON_API_USERINFO_' . $user_id, $user_info);
                }
            }
        } else {
            $user_info = array();
        }
        return $user_info;
    }
}