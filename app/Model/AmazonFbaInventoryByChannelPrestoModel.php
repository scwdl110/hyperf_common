<?php

namespace App\Model;

class AmazonFbaInventoryByChannelPrestoModel extends AbstractPrestoModel
{
    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "ods.ods_dataark_f_amazon_fba_inventory_by_channel_{$this->dbhost}";
    }
}
