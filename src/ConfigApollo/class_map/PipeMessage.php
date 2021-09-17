<?php
declare(strict_types=1);

namespace Hyperf\ConfigApollo;

use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\StdoutLoggerInterface;

/**
 * 重写 Hyperf\ConfigApollo\PipeMessage
 * 兼容 json,xml,yaml 格式
 *
 * @author Chopin Ngo <wushaobin@captainbi.com>
 */
class PipeMessage
{
    /** @var string */
    public const RESTART_SERVICE_KEY = 'this_config_need_to_restart_service';

    /** @var array */
    public $configurations;

    /** @var string */
    public $releaseKey;

    /** @var string */
    public $namespace;

    /** @var bool */
    protected $restartService = false;

    public function __construct($data)
    {
        if (isset($data['configurations'], $data['releaseKey'], $data['namespace'])) {
            $this->configurations = $data['configurations'];
            $this->releaseKey = $data['releaseKey'];
            $this->namespace = $data['namespace'];

            $value = [];
            $configName = '';
            $content = trim($this->configurations['content'] ?? '');
            $namespace4Sub = substr($this->namespace ?? '', -4);
            $namespace5Sub = substr($this->namespace ?? '', -5);
            $logger = ApplicationContext::getContainer()->get(StdoutLoggerInterface::class);

            if ('.json' === $namespace5Sub) {
                if ($content && null !== ($value = @json_decode($content, true))) {
                    $configName = substr($this->namespace, 0, -5);
                }
            } elseif ('.yml' === $namespace4Sub || '.yaml' === $namespace5Sub) {
                if (!function_exists('yaml_parse')) {
                    $logger->error("Config [{$this->namespace}] need yaml extension to parse");
                    return;
                }

                if ($content && false !== ($value = @yaml_parse($content))) {
                    $configName = substr($this->namespace, 0, '.yml' === $namespace4Sub ? -4 : -5);
                }
            } elseif ('.xml' === $namespace4Sub) {
                if (!function_exists('simplexml_load_string')) {
                    $logger->error("Config [{$this->namespace}] need xml extension to parse");
                    return;
                }

                if ($content && false !== ($xml = @simplexml_load_string($content, null, \LIBXML_NOCDATA))) {
                    $configName = substr($this->namespace, 0, -4);
                    $value = json_decode(json_encode($xml), true);
                }
            } elseif ('.txt' === $namespace4Sub) {
                $value = $content;
                $configName = substr($this->namespace, 0, -4);
            } else {
                foreach ($this->configurations ?? [] as $key => $value) {
                    $key = explode('.', $key);
                    if (end($key) === self::RESTART_SERVICE_KEY && filter_var($value, \FILTER_VALIDATE_BOOLEAN)) {
                        $this->restartService = true;
                        return;
                    }
                }
            }

            if ('' !== $configName) {
                if (!is_scalar($value)) {
                    $this->restartService = $value[self::RESTART_SERVICE_KEY] ?? false;
                    unset($value[self::RESTART_SERVICE_KEY]);
                }
                $this->configurations = [$configName => $value];
            }
        }
    }

    /**
     * 是否包含需要重启服务的配置
     *
     * @return bool
     */
    public function needToRestartService(): bool
    {
        return $this->restartService;
    }

    /**
     * 验证消息合法性
     *
     * @return bool
     */
    public function isValid(): bool
    {
        return $this->configurations && $this->releaseKey && $this->namespace;
    }
}
