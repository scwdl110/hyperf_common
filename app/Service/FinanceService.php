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

namespace App\Service;

use App\Lib\Common;

use App\Model\DataArk\FinanceIndexAssociatedSqlKeyModel;
use App\Model\DataArk\FinanceIndexModel;
use Captainbi\Hyperf\Util\Result;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\Logger\LoggerFactory;

use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;
use Hyperf\Contract\ConfigInterface;
use Hyperf\RpcServer\Annotation\RpcService;
use Hyperf\Utils\ApplicationContext;


/**
 * @RpcService(name="FinanceService", protocol="jsonrpc-http", server="jsonrpc-http", publishTo="consul")
 */
class FinanceService extends BaseService
{

    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;


    private function getArray($items)
    {
        return empty($items) ? array() : $items->toArray();
    }


    public function handleRequest($type = 1, $req = array())
    {
        $userInfo = $this->getUserInfo();

//        $req = $this->request->all();
        $result = ['lists' => [], 'count' => 0];
        if (empty($req)) {
            return $result;
        }

        if (config('misc.dataark_log_req', false)) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'dataark');
            $logger->info('request body', [$req, $userInfo]);
        }

//        $req['is_new_index'] = 1;
        $req['is_new_index'] = $req['is_new_index'] ?? 0;
//        $is_new_index = $req['is_new_index'] == 1 ? true:false;
        $searchKey = trim(strval($req['searchKey'] ?? ''));
        $searchVal = trim(strval($req['searchVal'] ?? ''));
        $searchType = intval($req['searchType'] ?? 0);
        $params = $req['params'] ?? [];
        if (isset($params['is_count']) && $params['is_count'] == 1) {//总计的页数只能为1
            $page = 1;
        } else {
            $params['is_count'] = 0;
            $page = intval($req['page'] ?? 1);
        }
        if (isset($params['user_id']) && $params['user_id'] == 266) {
            $logger1 = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('test', 'test');
            $logger1->info('request body', [$req, $userInfo]);
        }
        $params['is_new_index'] = $req['is_new_index'];
        $params['is_median'] = $params['is_median'] ?? 0;
        $params['total_status'] = $params['total_status'] ?? 0;
        $limit = intval($req['rows'] ?? 100);
        $sort = trim(strval($req['sort'] ?? ''));
        $order = trim(strval($req['order'] ?? ''));
        $params['force_sort'] = $sort;
        $channelIds = $req['channelIds'] ?? [];
        $countTip = intval($req['countTip'] ?? 0);
        $currencyInfo = $req['currencyInfo'] ?? [];
        $exchangeCode = $req['exchangeCode'] ?? '1';
        $timeLine = $req['timeLine'] ?? [];
        $deparmentData = $req['deparmentData'] ?? [];
        $rateInfo = $req['rateInfo'] ?? [];
        $offset = ($page - 1) * $limit;
        $params['searchKey'] = $searchKey;
        $params['searchVal'] = $searchVal;
        $params['matchType'] = trim(strval($req['matchType'] ?? ''));

        //对比数据信息
        /*说明：
        $compare_data => array(
            array(
                "target" => "sale_sales_volume,sale_sales_quota" , //对比指标
                "rename" => "compare_sale_sales_volume,compare_sale_sales_quota" , //对比字段重命名 。 默认为 compare1_sale_sales_volume， compare1_sale_sales_quota
                "compare_start_time" => 1630598400 , //对比开始时间
                "compare_end_time" => 1631203199 , //对比结束时间
                "where" => "origin_table.sale_sales_quota < (0.9*compare_table1.compare_sale_sales_quota/7*1.0000)" , //对比数据条件
                "join_type" => "LEFT JOIN " , // origin_table表和 compare_table1 表的连接方式 ，默认使用 left join
                "order" => "compare_table1.compare_sale_sales_quota DESC" , //排序方式
                "custom_target"=> array(
                    "origin_table.sale_sales_quota * 1.0000 / nullif(compare_table1.compare_sale_sales_volume)  AS diy_rate"   //自定义公式
                )
            ),
            array(
                ...
            )
        )*/

        $compare_data = $params['compare_data'] ?? [];
        if ($params['count_periods'] != '0') {  //统计维度不为无周期 ， 无法使用对比数据
            $compare_data = [];
        }
        $where = '';

        //不含时间的条件 ， 因对比数据为原筛选条件，改掉筛选时间范围
        $notime_where = '';

        if (empty($channelIds)) {
            return $result;
        }
        //替换查询用户
//        $userInfo['user_id'] = 21;
//        $params['user_id'] = 21;
//        $channelIds = explode(",","33185,30587,129003,11093,33022,11462,33010,6841,33116,5262,20052,33222,12409,32976,12411,18069,25615,129473,11236,12415,399839,399840,399838,11116,11004,32973,7352,128409,27287,33217,127398,11374,33007,11488,11027,33314,20076,139749,127106,11413,11389,29383,32993,32984,33034,11234,359212,359213,359211,451144,451145,451143,392750,392751,392749,56310,101200,129006,129009,129012,129021,129861,129018,187167,187168,187166,187183,187184,187182,187219,187220,187218,91483,399836,399837,399835,11238,292134,292135,292133,292136,292137,292138,292139,292141,292140,292113,292114,292115,292116,292117,369155,369154,141699,326273,326274,326272,356327,356328,356326,165231,165232,165233,165234,275859,165235,165261,165262,165263,165264,165265,184352,165266,184353,165267,165271,165272,165273,165274,165275,164772,164773,164774,275866,275867,164775,292289,292290,292291,292292,292294,292293,212437,212438,212439,212440,274254,212441,251408,251409,251410,251411,274209,251412,229527,229528,229529,229530,229531,229522,229523,229524,229525,274226,229526,229489,229490,229491,229492,229493,251413,251414,251415,251416,251417,251421,251422,251423,251424,251425,251393,251394,251395,251396,251397,303797,303798,303799,303800,303802,303801,290774,290775,290776,290777,290778,292208,292209,292210,292211,292212,314744,314350,314351,314352,314353,314355,314354,292258,292259,292260,292261,292263,292262,314368,314369,314370,314371,314372,326830,326831,326832,326833,326835,326834,360699,360700,360701,360702,360704,360703,314345,314346,314347,314348,314349,149541,149542,149543,149544,149545,253734,253735,252073,252074,149666,149667,149668,149669,275827,149670,160016,160017,160018,160019,275846,160020,149719,149720,149721,149722,149723,33004,292280,292281,292279,292271,292272,292270,303792,303793,303791,326691,326692,326690,326891,326892,326890,360691,360692,360690,326734,326736,326733,360717,360718,360716,382313,382314,382312,388594,388595,388593,407984,407985,407983,408007,408008,408006,149672,149673,149671,245391,245392,245390,129016,129017,129015,326758,326759,326757,164764,164765,164763,251347,251348,251346,251399,251400,251398,149556,149557,149555,251402,251401");
        //结束

        if (count($channelIds) > 1) {
            $params['operation_channel_ids'] = implode(',', $channelIds);
            $where = "report.user_id={$userInfo['user_id']} AND report.channel_id IN (" . implode(',', $channelIds) . ')';
            $params['user_sessions_where'] = $where;
            if ($type == 1) {
                $where .= " and amazon_goods.goods_user_id={$userInfo['user_id']} AND amazon_goods.goods_channel_id IN (" . implode(',', $channelIds) . ')';
            }
        } else {
            $params['operation_channel_ids'] = $channelIds[0];
            $where = "report.user_id={$userInfo['user_id']} AND report.channel_id={$channelIds[0]}";
            $params['user_sessions_where'] = $where;
            if ($type == 1) {
                $where .= " and amazon_goods.goods_user_id={$userInfo['user_id']} AND amazon_goods.goods_channel_id={$channelIds[0]}";
            }
        }
        $params['origin_where'] = $where;
        $params['origin_report_where'] = $params['user_sessions_where'];

        if (empty($searchKey) && !empty($searchVal)) {
            $likes = [
                'isku' => 'report.isku',
                'tags' => 'gtags.tag_name',
                'sku' => 'report.goods_sku',
                'asin' => 'report.goods_asin',
                'group' => 'report.goods_group_name',
                'parent_asin' => 'report.goods_parent_asin',
                'operators' => 'report.operation_user_admin_name',
                'class1' => 'report.goods_product_category_name_1',
            ];

            $eqs = [
                'site_id' => 'report.site_id',
                'site_group' => 'report.area_id',
                'channel_id' => 'report.channel_id',
            ];

            if (isset($likes[$params['count_dimension']])) {
                $where .= " AND {$likes[$params['count_dimension']]} LIKE '%{$searchVal}%' ";
            } elseif (isset($eqs[$params['count_dimension']])) {
                $where .= " AND {$eqs[$params['count_dimension']]}=" . intval($searchVal);
            }
        }

        if (isset($params['where_parent']) && !empty($params['where_parent'])) {//维度下钻需要的相关信息

            if ($type == 1) {
                if (!empty($params['where_parent']['parent_asin'])) {
                    $where .= " AND amazon_goods.goods_parent_asin = '" . addslashes($params['where_parent']['parent_asin']) . "'";
                }

                if (!empty($params['where_parent']['asin'])) {
                    $where .= " AND amazon_goods.goods_asin = '" . addslashes($params['where_parent']['asin']) . "'";
                }

                if (!empty($params['where_parent']['goods_parent_asin'])) {
                    $goods_parent_asin = json_decode(base64_decode($params['where_parent']['goods_parent_asin']), true);
                    if ($params['is_distinct_channel'] == 1) {
                        $where_strs = array();
                        foreach ($goods_parent_asin as $item) {
                            $where_strs[] = '( amazon_goods.goods_channel_id = ' . $item['channel_id'] . " AND amazon_goods.goods_parent_asin = '" . addslashes($item['parent_asin']) . "')";
                        }
                        $where_str = !empty($where_strs) ? " AND (" . implode(' OR ', $where_strs) . ")" : "";
                        $where .= $where_str;
                    } else {
                        $params['where_parent']['goods_parent_asin'] = implode("','", array_column($goods_parent_asin, 'parent_asin'));
                        $where .= " AND amazon_goods.goods_parent_asin IN ('" . $params['where_parent']['goods_parent_asin'] . "')";
                    }
                }

                if (!empty($params['where_parent']['goods_asin'])) {
                    $goods_asin = json_decode(base64_decode($params['where_parent']['goods_asin']), true);
                    if ($params['is_distinct_channel'] == 1) {
                        $where_strs = array();
                        foreach ($goods_asin as $item) {
                            $where_strs[] = '( amazon_goods.goods_channel_id = ' . $item['channel_id'] . " AND amazon_goods.goods_asin = '" . addslashes($item['asin']) . "')";
                        }
                        $where_str = !empty($where_strs) ? " AND (" . implode(' OR ', $where_strs) . ")" : "";
                        $where .= $where_str;
                    } else {
                        $params['where_parent']['goods_asin'] = implode("','", array_column($goods_asin, 'asin'));
                        $where .= " AND amazon_goods.goods_asin IN ('" . $params['where_parent']['goods_asin'] . "')";
                    }
                }

                if (!empty($params['where_parent']['goods_sku'])) {
                    $goods_sku = json_decode(base64_decode($params['where_parent']['goods_sku']), true);
                    if ($params['is_distinct_channel'] == 1) {
                        $params['where_parent']['goods_sku'] = implode(",", array_column($goods_sku, 'goods_id'));
                        $where .= " AND report.amazon_goods_id IN (" . $params['where_parent']['goods_sku'] . ")";
                    } else {
                        $params['where_parent']['goods_sku'] = implode("','", array_column($goods_sku, 'sku'));
                        $where .= " AND report.goods_sku IN ('" . $params['where_parent']['goods_sku'] . "')";
                    }
                }

                if (!empty($params['where_parent']['isku_id'])) {
                    $where .= " AND amazon_goods.goods_isku_id IN (" . $params['where_parent']['isku_id'] . ")";
                }

                if (!empty($params['where_parent']['group_id'])) {
                    $where .= " AND report.goods_group_id  IN (" . $params['where_parent']['group_id'] . ")";
                }

                if (!empty($params['where_parent']['tags_id'])) {
                    $where .= " AND tags_rel.tags_id  IN (" . $params['where_parent']['tags_id'] . ")";
                }

                if (!empty($params['where_parent']['class1'])) {//数据对比 一级类目
                    $class1 = $params['where_parent']['class1'] ? json_decode(base64_decode($params['where_parent']['class1']), true) : "";
                    $where_strs = array();
                    foreach ($class1 as $item) {
                        $where_strs[] = '( report.goods_product_category_name_1 = ' . trim($item['product_category_name_1']) . " AND report.site_id = '" . addslashes($item['site_id']) . "')";
                    }
                    $where_str = !empty($where_strs) ? " AND (" . implode(' OR ', $where_strs) . ")" : "";
                    $where .= $where_str;
                }

                if (!empty($params['where_parent']['class1_name']) && !empty($params['where_parent']['site_id'])) {//维度下钻 一级类目
                    if (is_array($params['where_parent']['class1_name'])) {
                        $class1_name = implode("','", $params['where_parent']['class1_name']);
                    } else {
                        $class1_name = trim($params['where_parent']['class1_name']);
                    }
                    $where .= " AND report.goods_product_category_name_1 IN('{$class1_name}') AND report.site_id = {$params['where_parent']['site_id']}";
                }

                if (!empty($params['where_parent']['head_id'])) {
                    $where .= " AND amazon_goods.isku_head_id  IN (" . $params['where_parent']['head_id'] . ")";
                }

                if (!empty($params['where_parent']['developer_id'])) {
                    $where .= " AND amazon_goods.isku_developer_id IN (" . $params['where_parent']['developer_id'] . ")";
                }
            }

            if ($type == 0) {
                if (!empty($params['where_parent']['user_department_id'])) {
                    $where .= " AND dc.user_department_id IN (" . $params['where_parent']['user_department_id'] . ")";
                }

                if (!empty($params['where_parent']['admin_id'])) {
                    $where .= " AND uc.admin_id IN (" . $params['where_parent']['admin_id'] . ")";
                }

                if (!empty($params['where_parent']['channel_id'])) {
                    $where .= " AND report.channel_id IN (" . $params['where_parent']['channel_id'] . ")";
                }

                if (!empty($params['where_parent']['site_id'])) {
                    $where .= " AND report.site_id IN (" . $params['where_parent']['site_id'] . ")";
                }
            }

            if ($type == 2) {
                if (!empty($params['where_parent']['operators_id'])) {
                    $where .= "  AND report.goods_operation_user_admin_id IN (" . $params['where_parent']['operators_id'] . ")";
                }
            }

        }

        if (isset($params['where_search']) && !empty($params['where_search'])) {//额外的搜索筛选功能，且关系
            if ($type == 1) {
                //分组
                if (!empty($params['where_search']['group_id'])) {
                    $where .= " AND report.goods_group_id  IN (" . $params['where_search']['group_id'] . ")";
                }
                //标签
                if (!empty($params['where_search']['tags_id'])) {
                    $where .= " AND tags_rel.tags_id  IN (" . $params['where_search']['tags_id'] . ")";
                }
                //负责人
                if (!empty($params['where_search']['head_id'])) {
                    $where .= " AND amazon_goods.isku_head_id  IN (" . $params['where_search']['head_id'] . ")";
                }
                //开发人员
                if (!empty($params['where_search']['developer_id'])) {
                    $where .= " AND amazon_goods.isku_developer_id IN (" . $params['where_search']['developer_id'] . ")";
                }
            }
            if ($type == 0) {
                //子账号
                if (!empty($params['where_search']['admin_id'])) {
                    $where .= " AND uc.admin_id IN (" . $params['where_search']['admin_id'] . ")";
                }
            }
            if ($type == 2) {
                //运营人员
                if (!empty($params['where_search']['operators_id'])) {
                    $where .= "  AND report.goods_operation_user_admin_id IN (" . $params['where_search']['operators_id'] . ")";
                }
            }
        }

        if ($params['show_type'] == 2 && $params['limit_num'] > 0 && $params['count_periods'] == 0) {
            $offset = 0;
            $limit = (int)$params['limit_num'];
        }

        if (!empty($compare_data)) {
            $notime_where = $where;
            $params['notime_where'] = $notime_where;
            $params['compare_data'] = $compare_data;
        }


        if ((int)$params['time_type'] === 99) {
            if (isset($params['site_search_time']) && !empty($params['site_search_time'])) {
                $site_where = [];
                foreach ($params['site_search_time'] as $val) {
                    $site_where[] = "(report.site_id = {$val['site_id']} AND report.create_time >= {$val['start_time']} AND report.create_time <= {$val['end_time']})";
                }
                $site_where_str = "(" . implode(' OR ', $site_where) . ")";

                $where .= $where ? " AND {$site_where_str} " : " {$site_where_str} ";
            } else {
                $where .= sprintf(
                    '%s report.create_time>=%d and report.create_time<=%d',
                    $where ? ' AND' : '',
                    (int)$params['search_start_time'],
                    (int)$params['search_end_time']
                );
            }
            $params['origin_where'] .= " AND report.create_time>={$params['search_start_time']} AND report.create_time<={$params['search_end_time']}";
            $params['origin_time'] = '  AND create_time >= ' . $params['search_start_time'] . ' AND create_time <= ' . $params['search_end_time'];
            $params['origin_create_start_time'] = $params['search_start_time'];
            $params['origin_create_end_time'] = $params['search_end_time'];

            $min_ym = date('Ym', (int)$params['search_start_time']);
            $max_ym = date('Ym', (int)$params['search_end_time']);
            $day_param_start_time = (int)$params['search_start_time'];
            $day_param_end_time = (int)$params['search_end_time'] > time() ? (int)strtotime(date('Y-m-d 23:59:59')) : (int)$params['search_end_time'];
            $day_param = ($day_param_end_time + 1 - $day_param_start_time) / 86400;
        } else {
            $ors = [];
            $origin_time = [];
            $time_arr = $this->getSiteLocalTime(array_keys(\App\getAmazonSitesConfig()), (int)$params['time_type'], $params['search_start_time'], $params['search_end_time']);
            foreach ($time_arr as $times) {

                $min_ym = empty($min_ym) ? date('Ym', $times['start']) : ($min_ym > date('Ym', $times['start']) ? date('Ym', $times['start']) : $min_ym);
                $max_ym = empty($max_ym) ? date('Ym', $times['end']) : ($max_ym < date('Ym', $times['end']) ? date('Ym', $times['end']) : $max_ym);
                $ors[] = sprintf(
                    '( report.create_time>=%d and report.create_time<=%d)',
                    (int)$times['start'],
                    (int)$times['end']
                );
                $params['origin_create_start_time'] = $times['start'];
                $params['origin_create_end_time'] = $times['end'];
                $origin_time[] = sprintf(
                    '( create_time>=%d and create_time<=%d)',
                    (int)$times['start'],
                    (int)$times['end']
                );
            }
            $day_param = ($time_arr[0]['end'] + 1 - $time_arr[0]['start']) / 86400;
            if (empty($ors)) {
                return Result::success($result);
            }
            $ors = join(' OR ', $ors);
            $where .= $where ? " AND ({$ors})" : "({$ors})";
            $params['origin_where'] .= " AND ({$ors}) ";
            $origin_time = join(' OR ', $origin_time);
            $params['origin_time'] = " AND ({$origin_time})";

        }
        $params['origin_report_where'] .= str_replace("create_time", "report.create_time", $params['origin_time']);

        $method = [
            'getListByUnGoods',
            'getListByGoods',
            'getListByOperators'
        ][$type];

        $isReadAthena = false;//其他地方无需传该参数

        //需要读取athena 的才使用该方法，其他不使用
        $big_data_user = "255981,33882,108142,22819,26060,34726,45723,53247,47562,57082,59221,255687,137346,255371,121069,83780,62473,74734,82142,337446,90330,95578,95336,99204,114937,101119,101133,114092,121675,346891,255707,98806,96975,105015,96119,95430,213581,219775,240755,243595,203705,185031,217593,245779,256968,220051,201375,243823,247442,268287,261217,310036,262106,306543,269036,21";
        $is_goods_day_report = false;//日报表才读
        if (($params['count_periods'] == 0 || $params['count_periods'] == 1 || $params['count_periods'] == 2) && $params['cost_count_type'] != 2) { //按天,按周或无统计周期
            $is_goods_day_report = true;
        }
        if (empty($compare_data)) {  // 有对比数据需使用PRESTO
            if ($method == 'getListByGoods' and $day_param > 90 and in_array($userInfo['user_id'], explode(",", $big_data_user)) and $is_goods_day_report) {
                $isReadAthena = true;
            }
            if ($method == 'getListByGoods' and $day_param > 15 and $userInfo['user_id'] == 20567) {//20567单独读取
                $isReadAthena = true;
            }
        }

        $limit = ($offset > 0 ? " OFFSET {$offset}" : '') . " LIMIT {$limit}";
        if (!empty($compare_data)) {  // 有对比数据需使用PRESTO
            $dataChannel = 'Presto';
        } else {
            $dataChannel = $searchType === 0 ? 'Presto' : 'ES';
        }

        $className = "\\App\\Model\\DataArk\\AmazonGoodsFinanceReportByOrder{$dataChannel}Model";
        $amazonGoodsFinanceReportByOrderMD = new $className($userInfo['dbhost'], $userInfo['codeno'], $isReadAthena);
        $amazonGoodsFinanceReportByOrderMD->dryRun(env('APP_TEST_RUNNING', false));
        $params['min_ym'] = $min_ym;
        $params['max_ym'] = $max_ym;
        $result = $amazonGoodsFinanceReportByOrderMD->{$method}(
            $where,
            $params,
            $limit,
            $sort,
            $order,
            $countTip,
            $channelIds,
            $currencyInfo,
            $exchangeCode,
            $timeLine,
            $deparmentData,
            $userInfo['user_id'],
            $userInfo['admin_id'],
            $rateInfo,
            $day_param
        );
        if (!isset($result['lists'])) {
            $result = ['lists' => [], 'count' => 0];
        }
        if (isset($params['user_id']) && $params['user_id'] == 266) {
            $logger1 = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('test', 'test');
            $logger1->info('result', [$result]);
        }
        return $result;
    }

    protected function getSiteLocalTime(array $siteIds, int $timeType = 99, $startTime, $endTime): array
    {
        $result = [[
            'end' => (int)$endTime,
            'start' => (int)$startTime,
            'site_id' => '0',
        ]];

        if (!empty($siteIds)) {
            $siteIdsStr = implode(',', $siteIds);
            if ($timeType === 99) {
                $result[0]['site_id'] = $siteIdsStr;
            } else {
                $timeDatas = [];
                $siteTimes = \App\getStartAndEndTimeAllSite($timeType);
                foreach ($siteIds as $siteId) {
                    $key = "{$siteTimes[$siteId]['start']}_{$siteTimes[$siteId]['end']}";

                    if (empty($timeDatas[$key])) {
                        $timeDatas[$key] = [
                            'site_id' => (string)$siteId,
                            'end' => $siteTimes[$siteId]['end'],
                            'start' => $siteTimes[$siteId]['start'],
                        ];
                    } else {
                        $timeDatas[$key]['site_id'] = "{$timeDatas[$key]['site_id']},{$siteId}";
                    }
                }

                $result = array_values($timeDatas);
            }
        }

        return $result;
    }
}
