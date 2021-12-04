<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Hyperf\DbConnection\Db;
use Hyperf\Utils\Context;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;

class OpenMiddleware implements MiddlewareInterface
{
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        //获取token
        $authorization = $request->getHeader('authorization');
        if(!isset($authorization[0])){
            return Context::get(ResponseInterface::class)->withStatus(401, 'authorization Unauthorized');
        }

        $accessToken = trim(str_replace('bearer', '', $authorization[0]));
        if(!$accessToken){
            return Context::get(ResponseInterface::class)->withStatus(401, 'Unauthorized');
        }

//        缺get_ws_id redis

        //获取client
        $where = [
            ['a.access_token', '=', $accessToken],
        ];
        $tokenArr = Db::connection('pg_kong')->table('oauth2_tokens as a')
            ->join('oauth2_credentials as b', 'a.credential_id', '=', 'b.id')
            ->where($where)
            ->select('b.client_id')->first();
        if(!$tokenArr){
            return Context::get(ResponseInterface::class)->withStatus(401, 'access_token Unauthorized');
        }

        $where = [
            ['client_id', '=', $tokenArr['client_id']],
        ];
        $client = Db::table('open_client')->where($where)->select('client_type')->first();

        if(!$client){
            return Context::get(ResponseInterface::class)->withStatus(401, 'client_id Unauthorized');
        }

        //获取user
        switch ($client['client_type']){
            case 0:
                //自用
                $userId = $this->self($tokenArr['client_id']);
                if(!$userId){
                    return Context::get(ResponseInterface::class)->withStatus(401, 'client_user Unauthorized');
                }
                break;
//            case 1:
//                //第三方
//                $this->middle();
//                break;
            default:
                return Context::get(ResponseInterface::class)->withStatus(401, 'client_type Unauthorized');
                break;
        }


        $where = [
            ['id', '=', $userId],
        ];
        $user = Db::connection('erp_base')->table('user')->where($where)->select('dbhost', 'codeno')->first();
        if(!$user){
            return Context::get(ResponseInterface::class)->withStatus(401, 'user Unauthorized');
        }

        $request = $request->withAttribute('userInfo', [
            'user_id' => $userId,
            'client_id' => $tokenArr['client_id'],
            'dbhost' => $user['dbhost'],
            'codeno' => $user['codeno'],
        ]);
        Context::set(ServerRequestInterface::class, $request);

        return $handler->handle($request);
    }


    private function self($client_id){
        $where = [
            ['client_id', '=', $client_id],
        ];
        $clientUser = Db::table('open_client_user')->where($where)->select('user_id')->first();
        if(!$clientUser){
            return Context::get(ResponseInterface::class)->withStatus(401, 'client_user Unauthorized');
        }
        return $clientUser['user_id'];
    }
}
