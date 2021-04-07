<?php

namespace Captainbi\Hyperf;

use Hyperf\Utils\ApplicationContext;

/**
 * 返回当前域名(带 http:// 或 https:// 协议头)
 *
 * @return string 获取失败返回空字符串
 */
function current_domain(): string
{
    $request = ApplicationContext::getContainer()->get(RequestInterface::class);
    $headers = $request->getHeaders();
    // 注，x-frontend-* 不是标准的 nginx http header，是本微服务框架在入口的负载均衡处添加的
    if (isset($headers['x-frontend-host'], $headers['x-frontend-proto'])) {
        $proto = strtolower($headers['x-frontend-proto'][0]);
        $domain = "{$proto}://{$headers['x-frontend-host'][0]}";

        if (isset($headers['x-frontend-port'])) {
            $port = (int)$headers['x-frontend-port'][0];
            if (!(80 === $port && 'http' === $proto) && !(443 === $port && 'https' === $proto)) {
                $domain = "{$domain}:{$port}";
            }
        }

        return strtolower($domain);
    } else {
        if (1 === preg_match('#^https?://[^/]+#i', (string)$request->getUri(), $match)) {
            return strtolower($match[0]);
        }
    }

    return '';
}

function current_url(): string
{
    $request = ApplicationContext::getContainer()->get(RequestInterface::class);
    // todo 用户实际访问的 url 可能在 负载均衡 或者 api 网关中被重写(rewrite)
    // $request->fullUrl() 方法只能获取到被重写后的 url
    // 如果在 hyperf 中需要输出 url 给用户或需要重定向，则使用 fullUrl 会出问题
    $url = $request->fullUrl();

    $domain = current_domain();
    if ($domain) {
        if ($domain === strtolower(substr($url, 0, strlen($domain)))) {
            return $url;
        }

        return preg_replace('#^https?://[^/]+#i', $domain, $url);
    }

    return '';
}
