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
namespace Captainbi\Hyperf\Session;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\SessionInterface;
use Hyperf\Utils\Context;
use Psr\Container\ContainerInterface;
use Psr\Http\Message\ServerRequestInterface;
use SessionHandlerInterface;

class SessionManager extends \Hyperf\Session\SessionManager
{

    public function __construct(ContainerInterface $container, ConfigInterface $config)
    {
        parent::__construct($container, $config);
    }

    public function start(ServerRequestInterface $request): SessionInterface
    {
        $sessionId = $this->parseSessionId($request);
        // @TODO Use make() function to create Session object.
        $keyPrefix = $this->config->get('session.options.key_prefix', '');
        $session = new Session($this->getSessionName(), $this->buildSessionHandler(), $sessionId, $keyPrefix);
        if (! $session->start()) {
            throw new \RuntimeException('Start session failed.');
        }
        $this->setSession($session);
        return $session;
    }

}
