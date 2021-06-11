<?php

declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use App\Model\UserModel;
use Captainbi\Hyperf\Util\Redis;
use Hyperf\Utils\Context;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;

/**
 * 参数提交的分库分表，需引入App\Model\UserModel;
 * 后面做强制化
 * Class UserParamMiddleware
 * @package App\Middleware
 */
class UserParamMiddleware implements MiddlewareInterface
{
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        //获取用户dbhost 和 codeno
        $user_id = (int)$request->input('user_id', 0);
        if($user_id > 0 ){
            $redis = new Redis();
            $redis = $redis->getClient();
            $user_info = $redis->get('COMMON_API_USERINFO_'.$user_id) ;
            if(empty($user_info)){
                $user_info = UserModel::query()->where(array('id'=>$user_id , 'status'=>1))->select("admin_id", "user_id", "is_master", "dbhost", "codeno")->first();
                if(!empty($user_info)){
                    $redis->set('COMMON_API_USERINFO_'.$user_id ,$user_info) ;
                }
            }

        }else{
            return Context::get(ResponseInterface::class)->withStatus(401, 'Unauthorized');
        }


        $request = $request->withAttribute('userInfo', [
            'admin_id' => $user_info['admin_id'],
            'user_id' => $user_info['user_id'],
            'is_master' => $user_info['is_master'],
            'dbhost' => $user_info['dbhost'],
            'codeno' => $user_info['codeno'],
        ]);
        Context::set(ServerRequestInterface::class, $request);

        return $handler->handle($request);
    }
}
