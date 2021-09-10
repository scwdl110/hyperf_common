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

namespace Captainbi\Hyperf\Aspect\Hyperf\ServiceGovernance\Listener;

use Hyperf\Di\Annotation\Aspect;
use Hyperf\Di\Aop\AbstractAspect;
use Hyperf\Di\Aop\ProceedingJoinPoint;
use Hyperf\ServiceGovernance\Listener\RegisterServiceListener as HyperfRegisterServiceListener;

///**
// * @Aspect
// */
class RegisterServiceListener extends AbstractAspect
{
    // 要切入的类，可以多个，亦可通过 :: 标识到具体的某个方法，通过 * 可以模糊匹配
    public $classes = [
        HyperfRegisterServiceListener::class . '::getInternalIp',
    ];

    // 要切入的注解，具体切入的还是使用了这些注解的类，仅可切入类注解和类方法注解
    public $annotations = [
    ];

    public function process(ProceedingJoinPoint $proceedingJoinPoint)
    {
        //获取本服务的host
        $host = config('servers.rpc_service_provider.local.host', null);

        return $host !== null ? $host : $proceedingJoinPoint->process();//本服务的host没有配置，就使用Hyperf框架本身提供的方法(Hyperf\ServiceGovernance\Listener\RegisterServiceListener::getInternalIp)获取
    }

}
