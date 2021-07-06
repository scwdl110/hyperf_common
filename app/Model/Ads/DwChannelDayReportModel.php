<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace App\Model\Ads;

use Captainbi\Hyperf\Base\Model;

class DwChannelDayReportModel extends Model
{
    protected $connection = 'bigdata_ads';
    protected $table = 'dws_dataark_f_dw_channel_day_report_';

    protected $request;

    public function __construct()
    {
        $dbhost = $this->request->getAttribute('dbhost');
        $codeno = $this->request->getAttribute('codeno');
        $this->connection = 'bigdata_ads';
        $this->table = 'dws_dataark_f_dw_channel_day_report_' . $dbhost;
        parent::__construct();
    }

}
