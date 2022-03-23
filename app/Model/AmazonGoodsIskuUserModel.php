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
namespace App\Model;

use Captainbi\Hyperf\Base\Model;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Contract\RequestInterface;
class AmazonGoodsIskuUserModel extends Model
{
    protected $dbhost;
    protected  $codeno ;
    protected $connection;
    protected $table;
    /**
     * @Inject()
     * @var RequestInterface
     */
    protected $request;
    public function __construct(){
        $this->dbhost = $this->request->getAttribute('dbhost'); ;
        $this->codeno = $this->request->getAttribute('codeno'); ;
        $this->connection = 'erp_captain_'.$this->dbhost ;
        $this->table = 'amazon_goods_isku_user_'.$this->codeno ;
        parent::__construct();
    }
}
