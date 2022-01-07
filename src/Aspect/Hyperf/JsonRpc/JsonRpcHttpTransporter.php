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


namespace Captainbi\Hyperf\Aspect\Hyperf\JsonRpc;


use GuzzleHttp\RequestOptions;
use Hyperf\Di\Aop\AbstractAspect;
use Hyperf\Di\Aop\ProceedingJoinPoint;
use Hyperf\Di\Annotation\Aspect;
use Hyperf\JsonRpc\JsonRpcHttpTransporter as HyperfJsonRpcHttpTransporter;
use Hyperf\LoadBalancer\LoadBalancerInterface;
use Hyperf\LoadBalancer\Node;
use Hyperf\Utils\Context;
use Psr\Http\Message\ServerRequestInterface;

/**
 * @Aspect
 */
class JsonRpcHttpTransporter extends AbstractAspect
{
    // 要切入的类，可以多个，亦可通过 :: 标识到具体的某个方法，通过 * 可以模糊匹配
    public $classes = [
        HyperfJsonRpcHttpTransporter::class . '::send',
    ];

    // 要切入的注解，具体切入的还是使用了这些注解的类，仅可切入类注解和类方法注解
    public $annotations = [];

    public function process(ProceedingJoinPoint $proceedingJoinPoint)
    {
        return call([$this, "aop_" . $proceedingJoinPoint->methodName], [$proceedingJoinPoint]);
    }

    /**
     * Create a connector instance based on the configuration.
     *
     * @return ConnectorInterface
     * @throws \InvalidArgumentException
     */
    public function aop_send(ProceedingJoinPoint $proceedingJoinPoint)//,$data
    {
        $userInfo = Context::get(ServerRequestInterface::class)->getAttribute("userInfo");

        $admin_id = data_get($userInfo, 'admin_id', 0);
        $user_id = data_get($userInfo, 'user_id', 0);
        $is_master = data_get($userInfo, 'is_master', 0);
        $dbhost = data_get($userInfo, 'dbhost', 001);
        $codeno = data_get($userInfo, 'codeno', 001);

        $data = data_get($proceedingJoinPoint->arguments, 'keys.data', []);
//        //$proceedingJoinPoint->getAnnotationMetadata(),
//        //$proceedingJoinPoint->processOriginalMethod(),
//        $model = $proceedingJoinPoint->getInstance();
//        //$model->getTable(),$model->getConnectionName(),
//        //$model->getTable(),$model->getConnectionName(),
//        //$proceedingJoinPoint->getReflectMethod(),
//        //$proceedingJoinPoint->processOriginalMethod(),$proceedingJoinPoint->result,
//        //$proceedingJoinPoint->processOriginalMethod(),
//        var_dump($proceedingJoinPoint->className, $proceedingJoinPoint->methodName, $proceedingJoinPoint->getArguments());//

        $instance = $proceedingJoinPoint->getInstance();
        $node = $this->aop_getNode($proceedingJoinPoint);

        $uri = $node->host . ':' . $node->port;
        $schema = value(function () use ($node) {
            $schema = 'http';
            if (property_exists($node, 'schema')) {
                $schema = $node->schema;
            }
            if (!in_array($schema, ['http', 'https'])) {
                $schema = 'http';
            }
            $schema .= '://';
            return $schema;
        });

        $url = $schema . $uri;

        $response = $instance->getClient()->post($url, [
            RequestOptions::HEADERS => [
                'Content-Type' => 'application/json',
                'x-authenticated-userid' => "{$admin_id}:{$user_id}:{$is_master}:{$dbhost}:{$codeno}"
            ],
            RequestOptions::HTTP_ERRORS => false,
            RequestOptions::BODY => $data,
        ]);

        if ($response->getStatusCode() === 200) {
            return (string)$response->getBody();
        }
        $instance->getLoadBalancer()->removeNode($node);

        return '';
    }

    /**
     * If the load balancer is exists, then the node will select by the load balancer,
     * otherwise will get a random node.
     */
    public function aop_getNode(ProceedingJoinPoint $proceedingJoinPoint): Node
    {
        $instance = $proceedingJoinPoint->getInstance();
        if ($instance->getLoadBalancer() instanceof LoadBalancerInterface) {
            return $instance->getLoadBalancer()->select();
        }

        $nodes = $instance->getNodes();

        return $nodes[array_rand($nodes)];
    }
}
