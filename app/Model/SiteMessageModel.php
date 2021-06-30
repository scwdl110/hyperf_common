<?php

namespace App\Model;

use Captainbi\Hyperf\Base\Model;

class SiteMessageModel extends Model
{
    protected $table = 'site_message';

    protected $connection = 'erp_report';

    protected $guarded = ['id'];

    const CREATED_AT = "create_time";
    const UPDATED_AT = "modified_time";

    protected $dateFormat = 'U';
}