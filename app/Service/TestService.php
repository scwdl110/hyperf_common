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
namespace App\Service;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Contract\RequestInterface;
class TestService extends BaseService {

    /**
     * @Inject()
     * @var RequestInterface
     */
    protected $requset;

    public function test(){
        $dbhost = $this->requset->getAttribute('dbhost') ;
        $codeno = $this->requset->getAttribute('codeno') ;
        return array('dbhost'=>$dbhost , 'codeno'=>$codeno) ;
    }

}
