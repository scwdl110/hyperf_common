<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Captainbi\Hyperf\Util\Functions;
use Captainbi\Hyperf\Util\Redis;
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
        $path = $request->getUri()->getPath();
        preg_match_all ("/\/v(\d+)\/(.*)/", $path, $pat_array);
        if(!isset($pat_array[1][0]) || !$pat_array[1][0] || !isset($pat_array[2][0]) || !$pat_array[2][0]){
            //没版本直接进入
            return $handler->handle($request);
        }

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


        //center_open_client_id
        $redis = new Redis();
        $key = 'center_open_client_id_'.$accessToken;
        $redis = $redis->getClient();
        $clientId = $redis->get($key);
        if($clientId===false){
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

            $redis->set($key, $clientId, 86400);
        }

        //授权和取消授权需更新同个redis_key
        //center_open_client_type
        $key = 'center_open_client_type_'.$clientId;
        $clientType = $redis->get($key);
        if($clientType===false){
            $where = [
                ['client_id', '=', $clientId],
            ];
            $client = Db::table('open_client')->where($where)->select('client_type')->first();

            if(!$client){
                return Context::get(ResponseInterface::class)->withStatus(401, 'client_id Unauthorized');
            }
            $clientType = data_get($client, 'client_type', '');

            $redis->set($key, $clientType, 86400);
        }


        //获取user
        switch ($clientType){
            case 0:
                //自用
                $userId = $this->self($redis, $clientId);
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
        //center_open_admin_id
        $key = 'center_open_admin_id_'.$userId;
        $adminId = $redis->get($key);
        if($adminId===false){
            $where = [
                ['is_master', '=', 1],
                ['user_id', '=', $userId],
            ];
            $admin = Db::connection('erp_base')->table('user_admin')->where($where)->select('id')->first();
            if(!$admin){
                return Context::get(ResponseInterface::class)->withStatus(401, 'admin Unauthorized');
            }
            $adminId = data_get($admin, 'id', 0);

            $redis->set($key, $adminId, 86400);
        }


        //channel需授权
        if($channelId){
            $key = 'center_open_client_user_channel'.$clientId."_".$userId."_".$channelId;
            $openClientUserChannelCount = $redis->get($key);
            if($openClientUserChannelCount===false){
                $where = [
                    ['user_id', '=', $userId],
                    ['channel_id', '=', $channelId],
                    ['client_id', '=', $clientId],
                ];
                $openClientUserChannelCount = Db::table('open_client_user_channel')->where($where)->count();
                if($openClientUserChannelCount<=0){
                    return Context::get(ResponseInterface::class)->withStatus(401, 'open_channel Unauthorized');
                }

                $redis->set($key, 1, 86400);
            }

            $channel = Functions::getChannel(intval($channelId));
            if(!$channel){
                return Context::get(ResponseInterface::class)->withStatus(401, 'channel Unauthorized');
            }

            $siteId = data_get($channel, 'site_id', 0);
            $MerchantID = data_get($channel, 'Merchant_ID', '');
            $areaId = data_get($channel, 'area_id', 0);
            $title = data_get($channel, 'title', 0);

        }else{
            $siteId = 0;
            $MerchantID = '';
            $areaId = 0;
            $title = '';
        }


        //分库分表
        //center_open_user
        $key = 'center_open_user_'.$userId;
        $user = $redis->get($key);
        if($user===false){
            $where = [
                ['id', '=', $userId],
            ];
            $user = Db::connection('erp_base')->table('user')->where($where)->select('dbhost', 'codeno')->first();
            if(!$user){
                return Context::get(ResponseInterface::class)->withStatus(401, 'user Unauthorized');
            }

            $dbhost = data_get($user, 'dbhost', '');
            $codeno = data_get($user, 'codeno', '');

            $user = [
                'dbhost' => $dbhost,
                'codeno' => $codeno,
            ];

            $redis->set($key, json_encode($user), 86400);
        }else{
            $user = json_decode($user,true);
            $dbhost = $user['dbhost'];
            $codeno = $user['codeno'];
        }

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
            'Merchant_ID' => $MerchantID,
            'area_id' => $areaId,
            'title' => $title
        ]);
        Context::set(ServerRequestInterface::class, $request);

        return $handler->handle($request);
    }

    /**
     * @param $redis
     * @param $client_id
     * @return array|mixed|ResponseInterface
     */
    private function self($redis, $client_id){
        //授权和取消授权需更新同个redis_key
        $key = 'center_open_self_user_id_'.$client_id;
        $userId = $redis->get($key);
        if($userId===false){
            $where = [
                ['client_id', '=', $client_id],
                ['is_delete', '=', 0],
                ['is_disable', '=', 0],
            ];
            $clientUser = Db::table('open_client_user')->where($where)->select('user_id')->first();
            if(!$clientUser){
                return Context::get(ResponseInterface::class)->withStatus(401, 'client_user Unauthorized');
            }
            $userId = data_get($clientUser, 'user_id', 0);
            $redis->set($key, $userId, 86400);
        }

        return $userId;
    }
}
