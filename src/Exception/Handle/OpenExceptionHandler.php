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

use Captainbi\Hyperf\Exception\OpenException;
use Captainbi\Hyperf\Util\Result;
use Hyperf\ExceptionHandler\ExceptionHandler;
use Hyperf\HttpMessage\Stream\SwooleStream;
use Hyperf\Logger\LoggerFactory;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Throwable;
use Captainbi\Hyperf\Util\Monitor\MonitorServiceManager;

class OpenExceptionHandler extends ExceptionHandler
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

        $message = $this->message ? $this->message : $throwable->getMessage();
        $code = $this->code ? $this->code : $throwable->getCode();

        // 格式化输出
        $data = Result::fail($this->data, $message, $code);
        $this->stopPropagation();

        return $response->withHeader('Server', 'Hyperf')->withStatus(200)->withBody(new SwooleStream($data));
    }

    public function isValid(Throwable $throwable): bool
    {
        return $throwable instanceof OpenException;
    }
}