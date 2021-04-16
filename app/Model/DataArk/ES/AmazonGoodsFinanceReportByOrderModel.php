<?php

namespace App\Model\DataArk\ES;

use App\Model\AbstractESModel;
use App\Model\DataArk\AmazonGoodsFinanceReportByOrderModelTrait;

class AmazonGoodsFinanceReportByOrderModel extends AbstractESModel
{
    use AmazonGoodsFinanceReportByOrderModelTrait;

    const SEARCH_TYPE_PRESTO = 0;

    const SEARCH_TYPE_ES = 1;

    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "f_amazon_goods_finance_report_by_order_{$this->dbhost}";
    }
}
