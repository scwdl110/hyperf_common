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
namespace Captainbi\Hyperf\Exception\Handle;

use Captainbi\Hyperf\Util\Result;
use Hyperf\ExceptionHandler\ExceptionHandler;
use Hyperf\HttpMessage\Stream\SwooleStream;
use Hyperf\Logger\LoggerFactory;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;
use Captainbi\Hyperf\Util\Monitor\MonitorServiceManager;

class BaseExceptionHandler extends ExceptionHandler
{
    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var $code
     */
    protected $code = 0;

    /**
     * @var $message
     */
    protected $message = '';

    /**
     * @var $data
     */
    protected $data = [];


    public function __construct(LoggerFactory $loggerFactory)
    {
        $this->logger = $loggerFactory->get('log', 'default');
    }

    public function handle(Throwable $throwable, ResponseInterface $response)
    {
        $this->logger->error(sprintf('%s[%s] in %s', $throwable->getMessage(), $throwable->getLine(), $throwable->getFile()));
        $this->logger->error($throwable->getTraceAsString());

        $this->message = $this->message ? $this->message : $throwable->getMessage();
        $this->code = $this->code ? $this->code : $throwable->getCode();

        // 格式化输出
        $data = Result::fail($this->data, $this->message, $this->code);
        $this->stopPropagation();

        $enable_app_exception_monitor = config('monitor.enable_app_exception_monitor', false);
        if ($enable_app_exception_monitor) {//如果开启异常监控，就通过消息队列将异常，发送到相应的钉钉监控群
            try {

                $exceptionData = static::getMessage($throwable);

                //添加系统异常监控
                $exceptionName = '系统异常=>服务器ip-->' . getInternalIp();
                $message = data_get($exceptionData, 'message', '');
                $code = data_get($exceptionData, 'exception_code') ? data_get($exceptionData, 'exception_code') : (data_get($exceptionData, 'http_code') ? data_get($exceptionData, 'http_code') : -101);
                $parameters = [$exceptionName, $message, $code, data_get($exceptionData, 'file'), data_get($exceptionData, 'line'), $exceptionData];
                MonitorServiceManager::handle('Ali', 'Ding', 'report', $parameters);

            } catch (\Exception $ex) {
            }
        }

        return $response->withHeader('Server', 'Hyperf')->withStatus(500)->withBody(new SwooleStream($data));
    }

    public function isValid(Throwable $throwable): bool
    {
        return true;
    }

    public static function getMessage(Throwable $exception, $debug = true) {

        return [
            'exception_code' => $exception->getCode(),
            "http_code" => $exception->getCode(),
            'message' => $exception->getMessage(),
            'type' => get_class($exception),
            'file' => $exception->getFile(),
            'line' => $exception->getLine(),
            'stack-trace' => $exception->getTrace(),
        ];
    }
}