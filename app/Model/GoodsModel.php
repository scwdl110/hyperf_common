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

class GoodsModel extends Model
{
    protected $dbhost;
    protected  $codeno ;
    protected $connection;
    protected $table;
    public function __construct($dbhost = '001' , $codeno = '001')
    {
        parent::__construct();
        $this->dbhost = $dbhost ;
        $this->codeno = $codeno ;
        $this->connection = 'erp_goods_'.$dbhost ;
        $this->table = 'amazon_goods_'.$codeno ;
    }
}
