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
namespace Captainbi\Hyperf\Constants;

use Hyperf\Constants\AbstractConstants;
use Hyperf\Constants\Annotation\Constants;

/**
 * @Constants
 */
class Constant extends AbstractConstants
{
    const RESPONSE_CODE_KEY = 'code';
    const RESPONSE_SUCCESS_CODE = 200;//响应成功状态码
    const RESPONSE_FAILURE_CODE = 0;//响应失败默认状态码
    const RESPONSE_MSG_KEY = 'msg';
    const RESPONSE_DATA_KEY = 'data';

    const SERVICE_KEY = 'service';
    const METHOD_KEY = 'method';
    const PARAMETERS_KEY = 'parameters';

    const QUEUE_CONNECTION = 'queue_connection';//消息队列  redis  连接
    const QUEUE_DELAY = 'delay';//消息队列延时key
    const QUEUE_CHANNEL = 'channel';//消息channel
    const CONTEXT_REQUEST_DATA = 'contextRequestData';
    const REQUEST_DATA_KEY = 'requestData';

}
