<?php

namespace App\Model\DataArk;

use App\Model\AbstractMySQLModel;

class AmazonFbaInventoryByChannelMySQLModel extends AbstractMySQLModel
{
    protected $table = 'amazon_fba_inventory_by_channel_';

    protected $connection = 'erp_finance_';
}
