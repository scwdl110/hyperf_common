<?php

namespace App\Model\DataArk;
use App\Model\AbstractMySQLModel;
class AmazonFbaInventoryRankMySQLModel extends AbstractMySQLModel
{
    protected $table = 'amazon_fba_inventory_rank_';
    protected $connection = 'erp_goods_';
}
