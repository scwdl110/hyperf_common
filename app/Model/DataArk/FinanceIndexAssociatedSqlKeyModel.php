<?php

namespace App\Model\DataArk;

use Captainbi\Hyperf\Base\Model;

class FinanceIndexAssociatedSqlKeyModel extends Model
{
    protected $table = 'finance_index_associated_sql_key';

    protected $connection = 'erp_report';

    const CREATED_AT = "create_time";
    const UPDATED_AT = "modified_time";

    protected $dateFormat = 'U';
}
