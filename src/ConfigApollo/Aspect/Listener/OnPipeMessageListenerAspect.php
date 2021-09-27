<?php

declare(strict_types=1);

namespace Captainbi\Hyperf\ConfigApollo\Aspect\Listener;

use Hyperf\Di\Annotation\Aspect;
use Hyperf\Di\Aop\AbstractAspect;
use Hyperf\Di\Aop\ProceedingJoinPoint;

/**
 * @Aspect
 */
class OnPipeMessageListenerAspect extends AbstractAspect
{
    // 要切入的类或 Trait，可以多个，亦可通过 :: 标识到具体的某个方法，通过 * 可以模糊匹配
    public $classes = [
        'Hyperf\ConfigApollo\Listener\OnPipeMessageListener::formatValue',
    ];

    // 要切入的注解，具体切入的还是使用了这些注解的类，仅可切入类注解和类方法注解
    public $annotations = [
    ];

    public function process(ProceedingJoinPoint $proceedingJoinPoint)
    {
        $value = current($proceedingJoinPoint->getArguments());
        if (!is_scalar($value)) {
            return $value;
        }

        return $proceedingJoinPoint->process();
    }
}
