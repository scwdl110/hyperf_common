<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Captainbi\Hyperf\Util\Functions;
use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\HttpServer\Router\Dispatched;
use Hyperf\HttpServer\Router\DispatcherFactory;
use Hyperf\Utils\ApplicationContext;
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

        $accessToken = trim(str_ireplace('bearer', '', $authorization[0]));
        if(!$accessToken){
            return Context::get(ResponseInterface::class)->withStatus(401, 'Unauthorized');
        }

        $configInterface = ApplicationContext::getContainer()->get(ConfigInterface::class);
        $serverName = $configInterface->get("server.servers.0.name");
        $maxVersion = $configInterface->get("open.version", 1);
        $aesKey = $configInterface->get("open.channel_id_aes_key", '');
        $noMerchantUrlPath = $configInterface->get("open.no_merchant_url_path", []);

        $path = $request->getUri()->getPath();
        preg_match_all ("/\/v(\d+)\/(.*)/", $path, $pat_array);
        if(!isset($pat_array[1][0]) || !$pat_array[1][0] || !isset($pat_array[2][0]) || !$pat_array[2][0]){
            //没版本直接进入
            return $handler->handle($request);
        }
        if(in_array('/'.$pat_array[2][0], $noMerchantUrlPath)){
            $channelId = 0;
        }else{
            $openChannelId = $request->getHeader('OpenChannelId');
            if(!$aesKey || !isset($openChannelId[0])){
                return Context::get(ResponseInterface::class)->withStatus(401, 'open_channel_id not found');
            }
            $channelId = Functions::decryOpen($openChannelId[0], $aesKey);
            if(!$channelId){
                return Context::get(ResponseInterface::class)->withStatus(401, 'open_channel_id not found');
            }
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

        $clientId = data_get($tokenArr, 'client_id', '');

        if(!$tokenArr || !$clientId){
            return Context::get(ResponseInterface::class)->withStatus(401, 'access_token Unauthorized');
        }

        $where = [
            ['client_id', '=', $clientId],
        ];
        $client = Db::table('open_client')->where($where)->select('client_type')->first();

        if(!$client){
            return Context::get(ResponseInterface::class)->withStatus(401, 'client_id Unauthorized');
        }

        //获取user
        switch (data_get($client, 'client_type', '')){
            case 0:
                //自用
                $userId = $this->self($clientId);
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

        //admin
        $where = [
            ['is_master', '=', 1],
            ['user_id', '=', $userId],
        ];
        $admin = Db::connection('erp_base')->table('user_admin')->where($where)->select('id')->first();
        if(!$admin){
            return Context::get(ResponseInterface::class)->withStatus(401, 'admin Unauthorized');
        }
        $adminId = data_get($admin, 'id', '');

        //channel需授权
        if($channelId){
            $where = [
                ['user_id', '=', $userId],
                ['channel_id', '=', $channelId],
                ['client_id', '=', $clientId],
            ];
            $openClientUserChannel = Db::table('open_client_user_channel')->where($where)->select('site_id')->first();
            if(!$openClientUserChannel){
                return Context::get(ResponseInterface::class)->withStatus(401, 'channel Unauthorized');
            }
            $siteId = $openClientUserChannel['site_id'];
        }else{
            $siteId = 0;
        }


        //分库分表
        $where = [
            ['id', '=', $userId],
        ];
        $user = Db::connection('erp_base')->table('user')->where($where)->select('dbhost', 'codeno')->first();
        if(!$user){
            return Context::get(ResponseInterface::class)->withStatus(401, 'user Unauthorized');
        }
        $dbhost = data_get($user, 'dbhost', '');
        $codeno = data_get($user, 'codeno', '');

        $dispatched = $request->getAttribute(Dispatched::class);

        //是否要最大版本号和版本号redis
        if($dispatched->status==0){
            //版本往前
            //maxVersion兼容减一
            $pathVersion = intval($pat_array[1][0]);
            $version = min($pathVersion, $maxVersion+1);
            //版本号
            if($version<=1){
                return Context::get(ResponseInterface::class)->withStatus(404, 'version not found');
            }

            $flag = 0;
            if(!isset(ApplicationContext::getContainer()->get(DispatcherFactory::class)->getRouter($serverName)->getData()[0][$request->getMethod()])){
                return Context::get(ResponseInterface::class)->withStatus(404, 'route and method not found');
            }
            $allHandler = ApplicationContext::getContainer()->get(DispatcherFactory::class)->getRouter($serverName)->getData()[0][$request->getMethod()];
            for ($i=$version-1;$i>0;$i--){
                $newPath = str_replace('v'.$pathVersion.'/', 'v'.$i.'/', $path);
                if(!isset($allHandler[$newPath])){
                    continue;
                }
                //修改标志
                $flag = 1;
            }

            if($flag==0){
                return Context::get(ResponseInterface::class)->withStatus(404, 'all version not found');
            }

            $dispatched->status=1;
            $dispatched->handler = $allHandler[$newPath];
        }

        $request = $request->withAttribute('userInfo', [
            'user_id' => $userId,
            'admin_id' => $adminId,
            'client_id' => $clientId,
            'channel_id' => $channelId,
            'dbhost' => $dbhost,
            'codeno' => $codeno,
            'site_id' => $siteId,
        ]);
        Context::set(ServerRequestInterface::class, $request);

        return $handler->handle($request);
    }

    /**
     * @param $client_id
     * @return array|mixed|ResponseInterface
     */
    private function self($client_id){
        $where = [
            ['client_id', '=', $client_id],
        ];
        $clientUser = Db::table('open_client_user')->where($where)->select('user_id')->first();
        if(!$clientUser){
            return Context::get(ResponseInterface::class)->withStatus(401, 'client_user Unauthorized');
        }
        return data_get($clientUser, 'user_id', '');
    }
}
