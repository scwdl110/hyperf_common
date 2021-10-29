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

use Captainbi\Hyperf\Base\Service;
use App\Model\UserAdminModel;
use App\Model\UserModel;
use Captainbi\Hyperf\Exception\BusinessException;
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

}
