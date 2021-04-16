<?php

namespace App\Model\DataArk\ES;

use App\Model\AbstractESModel;

class AmazonFbaInventoryByChannelModel extends AbstractESModel
{
    public function __construct(string $dbhost = '', string $codeno = '')
    {
        parent::__construct($dbhost, $codeno);

        $this->tableName = "f_amazon_fba_inventory_by_channel_{$this->dbhost}";
    }
}
