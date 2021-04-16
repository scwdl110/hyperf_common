<?php

namespace App\Model\DataArk\ES;

use App\Model\AbstractESModel;

class AmazonGoodsFinanceModel extends AbstractESModel
{
    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "f_amazon_goods_finance_{$this->dbhost}";
    }
}
