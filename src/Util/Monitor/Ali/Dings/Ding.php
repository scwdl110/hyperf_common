<?php

namespace Captainbi\Hyperf\Util\Monitor\Ali\Dings;

use Captainbi\Hyperf\Util\Queue;
use Captainbi\Hyperf\Job\DingDingJob;
use Hyperf\Utils\Arr;
use Hyperf\Utils\ApplicationContext;
use Hyperf\HttpServer\Contract\RequestInterface;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class Ding {

    /**
     * 配置
     * @param string $exceptionName 错误的标题
     * @param string $message 错误的信息 
     * @param string $code 错误的code
     * @param string $file 错误的文件
     * @param string $line 错误的位置
     * @param string $trace 错误的跟踪
     */
    public static function report($exceptionName, $message, $code, $file = '', $line = '', $trace = '', bool $simple = false) {

        $request = ApplicationContext::getContainer()->get(RequestInterface::class);
        $requestData = $request->all();

        $trace = Arr::collapse([
            [
                'requestData' => $requestData,
            ],
            (is_array($trace) ? $trace : [$trace])
        ]);

        Queue::push(new DingDingJob(
            $request->fullUrl() . '|' . $request->getHeaderLine('HTTP_REFERER'),
            $exceptionName,
            $message,
            $code,
            $file,
            $line,
            $trace,
            $simple
        ));
    }

}
