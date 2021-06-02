<?php

namespace App\Model;

use App\Model\AbstractMySQLModel;

class ChannelTargetsMySQLModel extends AbstractMySQLModel
{
    protected $table = 'channel_targets';

    protected $connection = 'erp_finance';

}
