<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Middleware;

use Hyperf\Utils\Context;
use Psr\Http\Message\ResponseInterface;
use Psr\Http\Message\ServerRequestInterface;
use Psr\Http\Server\MiddlewareInterface;
use Psr\Http\Server\RequestHandlerInterface;

/**
 * 获取 OAuth 2.0 鉴权后的用户数据，包含
 * admin_id (erp_base.b_user_admin.id)
 * user_id (erp_base.b_user.id 既 erp_base.b_user_admin.user_id)
 * is_master (erp_base.b_user_admin.is_master)
 * dbhost (erp_base.b_user.dbhost)
 * codeno (erp_base.b_user.codeno)
 *
 * 将保存在 ServerRequestInterface attribute 中，可通过
 * $request->getAttribute('userInfo') 获取，获取到的是数组数据，数组结构如下
 * [
 *     'admin_id' => int,
 *     'user_id' => int,
 *     'is_master' => bool,
 *     'dbhost' => string,
 *     'codeno' => string,
 * ]
 *
 * 对于本地开发，未接入 OAuth 2.0 服务的情况下，可在 .env 中添加
 * MOCK_OAUTH2_USERINFO
 * 配置信息，格式为
 * sprintf(
 *     '%d:%d:%d:%s:%s',
 *     $adminId,
 *     $userId,
 *     $isMaster,
 *     $dbhost,
 *     $codeno
 * );
 * 如：MOCK_OAUTH2_USERINFO=304:229:1:001:001
 */
class OAuth2RpcMiddleware implements MiddlewareInterface
{
    public function process(ServerRequestInterface $request, RequestHandlerInterface $handler): ResponseInterface
    {
        $authenticatedUserId = $request->getHeader('x-authenticated-userid');
        $authenticatedUserId = $authenticatedUserId[0] ?? '';
        if(!$authenticatedUserId){
            return Context::get(ResponseInterface::class)->withStatus(200);
        }

        if (1 !== preg_match('/^\d+:\d+:[01]:\d{3}:\d{3}$/', $authenticatedUserId)) {
            return Context::get(ResponseInterface::class)->withStatus(401, 'Unauthorized');
        }

        $data = explode(':', $authenticatedUserId);
        $request = $request->withAttribute('userInfo', [
            'admin_id' => (int)$data[0],
            'user_id' => (int)$data[1],
            'is_master' => (bool)$data[2],
            'dbhost' => $data[3],
            'codeno' => $data[4],
        ]);
        Context::set(ServerRequestInterface::class, $request);

        return $handler->handle($request);
    }
}
