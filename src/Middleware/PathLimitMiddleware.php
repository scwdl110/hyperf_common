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

/**
 * 须在验证后面加这个中间件
 * Class PathLimitMiddleware
 * @package Captainbi\Hyperf\Middleware
 */
class PathLimitMiddleware implements MiddlewareInterface
{
    public $defaultLimit = 10;

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

        $configInterface = ApplicationContext::getContainer()->get(ConfigInterface::class);
        $otherIsLimit = $configInterface->get("pathlimit.other_is_limit");
        $limitPath = $configInterface->get("pathlimit.limit_path");
        $project = $configInterface->get("config.app_name");
        $redis = new Redis();
        $redis = $redis->getClient();

        $userInfo = $request->getAttribute('userInfo');

        //缺少merchant_id
        if(!isset($userInfo['merchant_id']) && !isset($userInfo['Merchant_ID'])){
            $msg = "缺少merchant_id";
            return Context::get(ResponseInterface::class)->withStatus(401, $msg)->withBody($this->getBody(-1, $msg));
        }elseif(!isset($userInfo['merchant_id'])){
            $merchantId = $userInfo['Merchant_ID'];
        }else{
            $merchantId = $userInfo['merchant_id'];
        }

        //计数是否有匹配到path
        $num = 0;
        foreach ($limitPath as $k=>$apiCount){
            if(strpos($path, $k)){
                //验证次数
                $res = $this->checkCount($redis, $project, $path, $merchantId, $apiCount);
                if (!$res['code']) {
                    return Context::get(ResponseInterface::class)->withStatus(401, $res['msg'])->withBody($this->getBody(100910, $res['msg']));
                }

                //通过
                $num++;
                break;
            }
        }


        //判断other_is_limit
        if(!$num && $otherIsLimit){
            //验证次数
            $res = $this->checkCount($redis, $project, $path, $merchantId, $this->defaultLimit);
            if (!$res['code']) {
                return Context::get(ResponseInterface::class)->withStatus(401, $res['msg'])->withBody($this->getBody(100910, $res['msg']));
            }
        }

        $response = $handler->handle($request);

        $pathLimitReturnError = Context::get('pathLimitReturnError', 0);
        if($pathLimitReturnError){
            //有报错
            $msg = "超过亚马逊访问次数,请稍后尝试";
            return Context::get(ResponseInterface::class)->withStatus(401, $msg)->withBody($this->getBody(100910, $msg));
        }

        return $response;
    }


    /**
     * 验证次数
     * @param $redis
     * @param $project
     * @param $path
     * @param $merchantId
     * @param $apiCount
     * @return array
     */
    private function checkCount($redis, $project, $path, $merchantId, $apiCount)
    {
        $time = time();
        if (!isset($apiCount['rate']) || !isset($apiCount['burst'])) {
            return [
                'code' => 0,
                'msg' => '缺少参数',
            ];
        }

        //现在的参数
        $key = "center_path_limit_current_param_" .$project."_".$path."_".$merchantId;
        $currentParam = $redis->get($key);
        if($currentParam===false){
            $currentCount = $apiCount['burst'];
            $currentParam = [
                'time' => $time,
                'burst' => $apiCount['burst'],
            ];
            $redis->set($key, json_encode($currentParam), 3600);
        }else{
            $currentParam = json_decode($currentParam, true);
            $currentCount = ($time-$currentParam['time'])*$apiCount['rate']+$currentParam['burst'];
            if($currentCount > $apiCount['burst']){
                $currentCount = $apiCount['burst'];
            }
        }



        //现在访问的次数
        $key = "center_path_limit_check_count_" .$project."_".$path."_".$merchantId;
        $checkCount = $redis->incr($key);
        if ($checkCount > $currentCount) {
            return [
                'code' => 0,
                'msg' => '超过访问次数,请稍后尝试',
            ];
        }
        $redis->expire($key, 3600);

        return [
            'code' => 1,
            'msg' => 'success',
        ];
    }

}
