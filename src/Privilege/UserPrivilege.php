<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Privilege;

use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\Utils\ApplicationContext;

class UserPrivilege
{
    //是否拥有权限
    public function is_privilege()
    {
        $container = ApplicationContext::getContainer();
        // 通过 DI 容器直接注入
        $request = $container->get(RequestInterface::class);
        $url = $request->fullUrl();
        var_dump($url);exit;
        return true;
    }
}