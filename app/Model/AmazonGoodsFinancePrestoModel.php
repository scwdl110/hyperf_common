<?php

namespace App\Model;

class AmazonGoodsFinancePrestoModel extends AbstractPrestoModel
{
    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "ods.ods_dataark_f_amazon_goods_finance_{$this->dbhost}";
    }
}
