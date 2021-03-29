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

namespace App\Model;

use Captainbi\Hyperf\Base\Model;
use Hyperf\Utils\Context;
use Hyperf\Di\Annotation\Inject;
use Hyperf\HttpServer\Contract\RequestInterface;
use Psr\Http\Message\ServerRequestInterface;


class FinanceReportModel extends Model
{
    /**
     * @Inject()
     * @var RequestInterface
     */

    protected $dbhost;
    protected $codeno;
    protected $connection;
    protected $table;

    /**
     * @var RequestInterface
     */
    protected $request;

    public function __construct()
    {
        $this->request = Context::get(ServerRequestInterface::class);

        $this->dbhost = $this->request->getAttribute('dbhost');;
        $this->codeno = $this->request->getAttribute('codeno');;

        $this->connection = 'erp_finance_' . $this->dbhost;
        $this->table = 'f_amazon_goods_finance_report_' . $this->codeno;
        parent::__construct();
    }
}
