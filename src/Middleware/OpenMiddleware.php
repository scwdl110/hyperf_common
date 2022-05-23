<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Captainbi\Hyperf\Util\Functions;
use Captainbi\Hyperf\Util\Redis;
use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\HttpMessage\Stream\SwooleStream;
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
    public $freeLimit = 20;

    private function getBody($code, $message)
    {
        return new SwooleStream(json_encode([
            'code' => $code,
            'message' => $message,
        ], JSON_UNESCAPED_UNICODE));
    }

    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $path = $request->getUri()->getPath();
        preg_match_all("/\/v(\d+)\/(.*)/", $path, $pat_array);
        if (!isset($pat_array[1][0]) || !$pat_array[1][0] || !isset($pat_array[2][0]) || !$pat_array[2][0]) {
            //没版本直接进入
            return $handler->handle($request);
        }

        //获取token
        $authorization = $request->getHeader('authorization');
        if (!isset($authorization[0])) {
            return Context::get(ResponseInterface::class)->withStatus(401, 'authorization Unauthorized')->withBody($this->getBody(100901, "用户协议未授权"));
        }

        $accessToken = trim(str_ireplace('bearer', '', $authorization[0]));
        if (!$accessToken) {
            return Context::get(ResponseInterface::class)->withStatus(401, 'Unauthorized')->withBody($this->getBody(100902, "未授权"));
        }

        $configInterface = ApplicationContext::getContainer()->get(ConfigInterface::class);
        $serverName = $configInterface->get("server.servers.0.name");
        $maxVersion = $configInterface->get("open.version", 1);
        $aesKey = $configInterface->get("open.channel_id_aes_key", '');
        $noMerchantUrlPath = $configInterface->get("open.no_merchant_url_path", []);

        //center_open_client_id
        $key = 'center_open_client_id_' . $accessToken;
        $redis = new Redis();
        $redis = $redis->getClient();
        $clientId = $redis->get($key);
        if ($clientId === false) {
            //获取client
            $wsId = Functions::getOpenWsId();
            if (!$wsId) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'ws_id Unauthorized')->withBody($this->getBody(100905, "ws_id 未授权"));;
            }
            $where = [
                ['a.ws_id', '=', $wsId],
                ['a.access_token', '=', $accessToken],
            ];
            $tokenArr = Db::connection('pg_kong')->table('oauth2_tokens as a')
                ->join('oauth2_credentials as b', 'a.credential_id', '=', 'b.id')
                ->where($where)
                ->select('b.client_id')->first();

            $clientId = data_get($tokenArr, 'client_id', '');
            if (!$tokenArr || !$clientId) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'access_token Unauthorized')->withBody($this->getBody(100906, "access_token未授权"));;
            }

            $redis->set($key, $clientId, 86400);
        }


        //center_open_client_type
        $key = 'center_open_client_type_' . $clientId;
        $clientType = $redis->get($key);
        if ($clientType === false) {
            $where = [
                ['client_id', '=', $clientId],
            ];
            $client = Db::table('open_client')->where($where)->select('client_type')->first();

            if (!$client) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'client_id Unauthorized')->withBody($this->getBody(100907, "client_id未授权"));;
            }
            $clientType = data_get($client, 'client_type', '');

            $redis->set($key, $clientType, 86400);
        }


        //获取user
        switch ($clientType) {
            case 0:
                //自用
                $userId = $this->self($redis, $clientId);
                if (!$userId) {
                    return Context::get(ResponseInterface::class)->withStatus(401, 'client_user Unauthorized')->withBody($this->getBody(100908, "client_user未授权"));;
                }
                break;
//            case 1:
//                //第三方
//                $this->middle();
//                break;
            default:
                return Context::get(ResponseInterface::class)->withStatus(401, 'client_type Unauthorized')->withBody($this->getBody(100909, "client_type未授权"));;
                break;
        }


        if (in_array('/' . $pat_array[2][0], $noMerchantUrlPath)) {
            $channelAndCpcId = 0;
        } else {
            //channel_id
            $openChannelId = $request->getHeader('OpenChannelId');
            if (!$aesKey || !isset($openChannelId[0])) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'open_channel_id not found')->withBody($this->getBody(100903, "open_channel_id 未找到"));;
            }

            //center_open_client_channel_and_cpc_id
            $key = 'center_open_client_channel_and_cpc_id_'.$clientId."_".$userId."_".$openChannelId[0];
            $channelAndCpcId = $redis->get($key);
            if ($channelAndCpcId === false) {
                $where = [
                    ['client_id', '=', $clientId],
                    ['user_id', '=', $userId],
                    ['encry_channel_id', '=', $openChannelId[0]],
                ];
                $channelAndCpc = Db::table('open_client_user_channel')->where($where)->select('channel_and_cpc_id')->first();
                $channelAndCpcId = data_get($channelAndCpc, 'channel_and_cpc_id', '0');

                $redis->set($key, $channelAndCpcId, 86400);
            }

            if (!$channelAndCpcId) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'open_channel_id not found')->withBody($this->getBody(100903, "open_channel_id 未找到"));;
            }

        }


        //锁住同个api调用
        $redisKey = 'center_open_lock_' . $channelAndCpcId . "_" . $path;
        $res = $this->lock($redis, $redisKey);
        if (!$res) {
            return Context::get(ResponseInterface::class)->withStatus(401, 'please wait previous request')->withBody($this->getBody(100904, "请等待上一个请求"));;
        }
        defer(function () use ($redis, $redisKey) {
            $this->unlock($redis, $redisKey);
        });


        //验证次数
        $res = $this->checkCount($redis, $userId);
        if (!$res['code']) {
            return Context::get(ResponseInterface::class)->withStatus(401, $res['msg'])->withBody($this->getBody(100910, "请求频率过快，请稍等"));;
        }

        //admin
        //center_open_admin_id
        $key = 'center_open_admin_id_' . $userId;
        $cache = $redis->get($key);
        if ($cache === false) {
            $where = [
                ['is_master', '=', 1],
                ['user_id', '=', $userId],
            ];
            $admin = Db::connection('erp_base')->table('user_admin')->where($where)->select(array('id', 'is_master'))->first();
            $user = Db::connection('erp_base')->table('user')->where([['id', '=', $userId]])->select(array('redis_id'))->first();
            if (!$admin) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'admin Unauthorized')->withBody($this->getBody(100911, "admin未授权"));;
            }
            $adminId = data_get($admin, 'id', 0);
            $isMaster = data_get($admin, 'is_master', 0);
            $redisId = data_get($user, 'redis_id', 0);
            $userInfo['admin_id'] = $adminId;
            $userInfo['is_master'] = $isMaster;
            $userInfo['redis_id'] = $redisId;
            $redis->set($key, json_encode($userInfo), 86400);
        } else {
            $userInfo = json_decode($cache, true);
            $adminId = $userInfo['admin_id'];
            $isMaster = $userInfo['is_master'];
            $redisId = $userInfo['redis_id'];
        }

        //channel需授权
        if ($channelAndCpcId) {
            $channel = Functions::getChannelCpc(intval($channelAndCpcId));
            if (!$channel) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'channel Unauthorized')->withBody($this->getBody(100913, "店铺未授权"));;
            }

            $siteId = data_get($channel, 'site_id', 0);
            $MerchantID = data_get($channel, 'Merchant_ID', '');
            $areaId = data_get($channel, 'area_id', 0);
            $title = data_get($channel, 'site_name', 0);
            $channelId = data_get($channel, 'channel_id', 0);
            $cpcProfilesId = data_get($channel, 'cpc_profiles_id', 0);
        } else {
            $siteId = 0;
            $MerchantID = '';
            $areaId = 0;
            $title = '';
            $channelId = 0;
            $cpcProfilesId = 0;
        }


        //分库分表
        //center_open_user
        $key = 'center_open_user_' . $userId;
        $user = $redis->get($key);
        if ($user === false) {
            $where = [
                ['id', '=', $userId],
            ];
            $user = Db::connection('erp_base')->table('user')->where($where)->select('dbhost', 'codeno')->first();
            if (!$user) {
                return Context::get(ResponseInterface::class)->withStatus(401, 'user Unauthorized')->withBody($this->getBody(100914, "用户未授权"));;
            }

            $dbhost = data_get($user, 'dbhost', '');
            $codeno = data_get($user, 'codeno', '');

            $user = [
                'dbhost' => $dbhost,
                'codeno' => $codeno,
            ];

            $redis->set($key, json_encode($user), 86400);
        } else {
            $user = json_decode($user, true);
            $dbhost = $user['dbhost'];
            $codeno = $user['codeno'];
        }

        $dispatched = $request->getAttribute(Dispatched::class);

        //是否要最大版本号和版本号redis
        if ($dispatched->status == 0) {
            //版本往前
            //maxVersion兼容减一
            $pathVersion = intval($pat_array[1][0]);
            $version = min($pathVersion, $maxVersion + 1);
            //版本号
            if ($version <= 1) {
                return Context::get(ResponseInterface::class)->withStatus(404, 'version not found')->withBody($this->getBody(100915, "版本号未找到"));;
            }

            $flag = 0;
            if (!isset(ApplicationContext::getContainer()->get(DispatcherFactory::class)->getRouter($serverName)->getData()[0][$request->getMethod()])) {
                return Context::get(ResponseInterface::class)->withStatus(404, 'route and method not found')->withBody($this->getBody(100916, "路由及方法未找到"));;
            }
            $allHandler = ApplicationContext::getContainer()->get(DispatcherFactory::class)->getRouter($serverName)->getData()[0][$request->getMethod()];
            for ($i = $version - 1; $i > 0; $i--) {
                $newPath = str_replace('v' . $pathVersion . '/', 'v' . $i . '/', $path);
                if (!isset($allHandler[$newPath])) {
                    continue;
                }
                //修改标志
                $flag = 1;
            }

            if ($flag == 0) {
                return Context::get(ResponseInterface::class)->withStatus(404, 'all version not found')->withBody($this->getBody(100917, "所有版本未找到"));
            }

            $dispatched->status = 1;
            $dispatched->handler = $allHandler[$newPath];
        }

        $request = $request->withAttribute('userInfo', [
            'user_id' => $userId,
            'admin_id' => $adminId,
            'is_master' => $isMaster,
            'client_id' => $clientId,
            'channel_id' => $channelId,
            'cpc_profiles_id' => $cpcProfilesId,
            'dbhost' => $dbhost,
            'codeno' => $codeno,
            'site_id' => $siteId,
            'Merchant_ID' => $MerchantID,
            'area_id' => $areaId,
            'title' => $title,
            'redis_id' => $redisId
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
    private function self($redis, $client_id)
    {
        $arr = Functions::getOpenSelfClientUser($client_id);
        if (!isset($arr['user_id']) || !isset($arr['is_disable']) || $arr['is_disable'] != 0) {
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
        return $redis->set($lockKey, 1, ['nx', 'ex' => 60]);
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
        $key = 'center_open_api_count_' . $userId;
        $apiCount = $redis->get($key);
        if ($apiCount === false) {
            $where = [
                ['user_id', '=', $userId],
                ['tools_id', '=', 11],
                ['status', '=', 1],
            ];
            $toolsUserRel = Db::connection("erp_base")->table('tools_user_rel')->where($where)->select('api_count', 'end_time')->first();
            if (!$toolsUserRel) {
//                return [
//                    'code' => 0,
//                    'msg' => 'please buy api tools',
//                ];
                $toolsUserRel = [
                    'api_count' => $this->freeLimit,
                    'end_time' => -1
                ];
            }

            $endTime = data_get($toolsUserRel, 'end_time', 0);
            //过期或者没套餐的
            if ($endTime == -1 || $endTime < $time) {
//                return [
//                    'code' => 0,
//                    'msg' => 'api tools expired',
//                ];
                $apiCount = $this->freeLimit;
            } else {
                $apiCount = data_get($toolsUserRel, 'api_count', 0);
            }


            //有一个小时缓存
            $redis->set($key, $apiCount, 3600);
        }

        $minutes = date("Ymdhi");
        $key = "center_open_check_count_" . $userId . "_" . $minutes;
        $checkCount = $redis->incr($key);
        if ($checkCount > $apiCount) {
            return [
                'code' => 0,
                'msg' => 'The calling frequency is too high. Please wait or upgrade the package',
            ];
        }

        $redis->expire($key, 60);
        return [
            'code' => 1,
            'msg' => 'success',
        ];
    }

    //写日志
    private function log($code, $msg, $path, $userId, $clientId, $channelId)
    {
        //定制化只支持开放平台
        $context = Context::get(ServerRequestInterface::class);
        $time = time();
        $insertData = [
            'user_id' => $userId,
            'client_id' => $clientId,
            'channel_id' => $channelId,
            'path' => $path,
            'code' => $code,
            'msg' => $msg,
            'create_time' => $time,
            'modified_time' => $time,
        ];
        Db::connection("erp_report")->table("open_api_log")->insert($insertData);
    }
}
