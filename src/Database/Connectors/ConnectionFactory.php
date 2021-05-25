<?php

declare(strict_types=1);

namespace Captainbi\Hyperf\Database\Connectors;

use Hyperf\Contract\SessionInterface;
use Hyperf\Di\Annotation\Inject;

class ConnectionFactory extends \Hyperf\Database\Connectors\ConnectionFactory
{
    /**
     * @Inject()
     * @var SessionInterface
     */
    private $session;

    public function make(array $config, $name = null)
    {
        $code = $this->session->get('codeno'); //dbhost
        $code = $code ? '_'.$code : '';
        $config['database'] = $config['database'].$code;
        echo '===============ConnectionFactory00===============';
        var_dump($config);
        echo '===============ConnectionFactory00===============';
        $config = $this->parseConfig($config, $name);
        return parent::make($config, $name); // TODO: Change the autogenerated stub
    }

}