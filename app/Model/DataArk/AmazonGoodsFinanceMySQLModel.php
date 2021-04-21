<?php

namespace App\Model\DataArk;

use App\Model\AbstractMySQLModel;

class AmazonGoodsFinanceMySQLModel extends AbstractMySQLModel
{
    protected $table = 'amazon_goods_finance_';

    protected $connection = 'erp_finance_';
}
