<?php

namespace App\Model\DataArk\Presto;

use App\Model\AbstractPrestoModel;
use App\Model\DataArk\AmazonGoodsFinanceReportByOrderModelTrait;

class AmazonGoodsFinanceReportByOrderModel extends AbstractPrestoModel
{
    use AmazonGoodsFinanceReportByOrderModelTrait;

    const SEARCH_TYPE_PRESTO = 0;

    const SEARCH_TYPE_ES = 1;

    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "ods.ods_dataark_f_amazon_goods_finance_report_by_order_{$this->dbhost}";
    }
}
