<?php

namespace App\Model;

use App\Model\AbstractMySQLModel;

class SiteRateMySQLModel extends AbstractMySQLModel
{
    protected $table = 'site_rate';

    protected $connection = 'erp_base';

}
