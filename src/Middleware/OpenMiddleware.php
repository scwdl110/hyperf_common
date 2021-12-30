<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Captainbi\Hyperf\Util\Functions;
use Captainbi\Hyperf\Util\Redis;
use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\HttpServer\Contract\RequestInterface;
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


        //锁住同个api调用
        $redis = new Redis();
        $redis = $redis->getClient();
        $redisKey = 'center_open_lock_'.$channelId."_".$path;
        $res = $this->lock($redis, $redisKey);
        if(!$res){
            return Context::get(ResponseInterface::class)->withStatus(401, 'please wait previous request');
        }
        defer(function()use($redis, $redisKey){
            $this->unlock($redis, $redisKey);
        });


        //center_open_client_id
        $key = 'center_open_client_id_'.$accessToken;
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

        //验证次数
        $res = $this->checkCount($redis, $userId);
        if(!$res['code']){
            return Context::get(ResponseInterface::class)->withStatus(401, $res['msg']);
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
            $channelIds = Functions::getOpenClientUserChannel($channelId, $userId);
            if(!$channelIds || !in_array($channelId, $channelIds)){
                return Context::get(ResponseInterface::class)->withStatus(401, 'open_channel Unauthorized');
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

        $response = $handler->handle($request);

        return $response;
    }

    /**
     * @param $redis
     * @param $client_id
     * @return bool
     */
    private function self($redis, $client_id){
        $arr = Functions::getOpenSelfClientUser($client_id);
        if(!isset($arr['user_id']) || !isset($arr['is_disable']) || $arr['is_disable']!=0){
            return false;
        }

        return $arr['user_id'];
    }


    /**
     * 锁住同个api调用
     * @param $redis
     * @param $lockKey
     * @return bool|ResponseInterface
     */
    private function lock($redis, $lockKey)
    {
        return $redis->set($lockKey,1,['nx', 'ex' => 60]);
    }

    //解锁住同个api调用
    private function unlock($redis, $lockKey)
    {
        $redis->del($lockKey);
        return true;
    }

    //验证次数
    private function checkCount($redis, $userId)
    {
        $time = time();
        $key = 'center_open_api_count_'.$userId;
        $apiCount = $redis->get($key);
        if($apiCount===false){
            $where = [
                ['user_id', '=', $userId],
                ['tools_id', '=', 11],
                ['status', '=', 1],
            ];
            $toolsUserRel = Db::connection("erp_base")->table('tools_user_rel')->where($where)->select('api_count', 'end_time')->first();
            if(!$toolsUserRel){
                return [
                    'code' => 0,
                    'msg' => 'please buy api tools',
                ];
            }

            if(data_get($toolsUserRel, 'end_time', 0) < $time){
                return [
                    'code' => 0,
                    'msg' => 'api tools expired',
                ];
            }

            $apiCount = data_get($toolsUserRel, 'api_count', 0);
            //有一个小时缓存
            $redis->set($key, $apiCount, 3600);
        }

        $minutes = date("Ymdhi");
        $key = "center_open_check_count_".$userId."_".$minutes;
        $checkCount = $redis->incr($key);
        if($checkCount>$apiCount){
            return [
                'code' => 0,
                'msg' => 'The calling frequency is too high. Please wait or upgrade the package',
            ];
        }

        $redis->expire($key,60);
        return [
            'code' => 1,
            'msg' => 'success',
        ];
    }

    //写日志
    private function log($code,$msg,$path,$userId,$clientId,$channelId){
        //定制化只支持开放平台
        $context = Context::get(ServerRequestInterface::class);
        $insertData = [
            'user_id' => $userId,
            'client_id' => $clientId,
            'channel_id' => $channelId,
            'path' => $path,
            'code' => $code,
            'msg' => $msg,
        ];
        Db::connection("erp_report")->table("open_api_log")->insert($insertData);
    }
}
