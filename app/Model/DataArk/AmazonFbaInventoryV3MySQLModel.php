<?php

namespace App\Model\DataArk;
use App\Model\AbstractMySQLModel;
class AmazonFbaInventoryV3MySQLModel extends AbstractMySQLModel
{
    protected $table = 'amazon_fba_inventory_v3_';
    protected $connection = 'erp_goods_';
}
