<?php

namespace App\Controller;

use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Captainbi\Hyperf\Util\Result;

class DataArkController extends AbstractController
{
    protected function init($type = 1)
    {
        $userInfo = $this->request->getAttribute('userInfo');
        $req = $this->request->all();

        if (config('misc.dataark_log_req', false)) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('dataark', 'default');
            $logger->info('request body', [$req, $userInfo]);
        }

        $searchKey = trim(strval($req['searchKey'] ?? ''));
        $searchVal = trim(strval($req['searchVal'] ?? ''));
        $searchType = intval($req['searchType'] ?? 0);
        $params = $req['params'] ?? [];
        $page = intval($req['page'] ?? 1);
        $limit = intval($req['rows'] ?? 100);
        $sort = trim(strval($req['sort'] ?? ''));
        $order = trim(strval($req['order'] ?? ''));
        $channelIds = $req['channelIds'] ?? [];
        $countTip = intval($req['countTip'] ?? 0);
        $currencyInfo = $req['currencyInfo'] ?? [];
        $exchangeCode = $req['exchangeCode'] ?? '1';
        $timeLine = $req['timeLine'] ?? [];
        $deparmentData = $req['deparmentData'] ?? [];
        $rateInfo= $req['rateInfo'] ?? [];
        $offset = ($page - 1) * $limit;

        $result = ['lists' => [], 'count' => 0];

        $where = '';
        if (empty($channelIds)) {
            return Result::success($result);
        }

        if (count($channelIds) > 1) {
            $params['operation_channel_ids'] = implode(',' , $channelIds);
            $where = "report.user_id={$userInfo['user_id']} AND report.channel_id IN (" . implode(',', $channelIds) . ')';
            if ($type == 1) {
                $where .= " and amazon_goods.goods_user_id={$userInfo['user_id']} AND amazon_goods.goods_channel_id IN (" . implode(',', $channelIds) . ')';
            }
        } else {
            $params['operation_channel_ids'] = $channelIds[0];
            $where = "report.user_id={$userInfo['user_id']} AND report.channel_id={$channelIds[0]}";
            if ($type == 1) {
                $where = "amazon_goods.goods_user_id={$userInfo['user_id']} AND amazon_goods.goods_channel_id={$channelIds[0]}";
            }
        }
        $params['origin_where'] = $where;

        if(!empty($searchKey) && !empty($searchVal)){
            //匹配方式 ：eq -> 全匹配  like-模糊匹配
            $matchType =  trim(strval($req['matchType'] ?? ''));
            if($searchKey == 'parent_asin'){
                if($matchType == 'eq'){
                    $where .= " AND report.goods_parent_asin = '" . $searchVal . "'" ;
                }else{
                    $where .= " AND report.goods_parent_asin like '%" . $searchVal . "%'" ;
                }
            }else if($searchKey == 'asin'){
                if($matchType == 'eq'){
                    $where .= " AND report.goods_asin = '" . $searchVal . "'" ;
                }else {
                    $where .= " AND report.goods_asin like '%" . $searchVal . "%'";
                }
            }else if($searchKey == 'sku'){
                if($matchType == 'eq'){
                    $where .= " AND report.goods_sku = '" . $searchVal . "'" ;
                }else {
                    $where .= " AND report.goods_sku like '%" . $searchVal . "%'";
                }
            }else if($searchKey == 'isku'){
                if($matchType == 'eq'){
                    $where .= " AND report.isku = '" . $searchVal . "'" ;
                }else {
                    $where .= " AND report.isku like '%" . $searchVal . "%'";
                }
            }else if($searchKey == 'site_group'){
                $where .= " AND report.area_id = " . intval($searchVal);
            }else if($searchKey == 'channel_id'){
                $where .= " AND report.channel_id = " . intval($searchVal);
            }else if($searchKey == 'site_id'){
                $where .= " AND report.site_id = " . intval($searchVal) ;
            }else if($searchKey == 'class1'){
                if($matchType == 'eq'){
                    $where .= " AND report.goods_product_category_name_1 = '" . $searchVal . "'" ;
                }else{
                    if (strpos($searchVal,'&') !== false){
                        $str_arr = explode("&",$searchVal);
                        foreach ($str_arr as $v){
                            $where .= " AND report.goods_product_category_name_1 like '%" . $v . "%'" ;
                        }
                    }else{
                        $where .= " AND report.goods_product_category_name_1 like '%" . $searchVal . "%'" ;

                    }
                }
            }else if($searchKey == 'group'){
                if($matchType == 'eq'){
                    $where .= " AND report.goods_group_name = '".$searchVal."' " ;
                }else{
                    $where .= " AND report.goods_group_name like '%".$searchVal."%' " ;
                }

            }else if($searchKey == 'tags'){
                if($matchType == 'eq'){
                    $where .= " AND gtags.tag_name = '".$searchVal."' " ;
                }else {
                    $where .= " AND gtags.tag_name like '%" . $searchVal . "%'";
                }
            }else if($searchKey == 'operators'){
                if($matchType == 'eq'){
                     $where .= " AND report.operation_user_admin_name = '" . $searchVal . "'" ;
                }else{
                    $where .= " AND report.operation_user_admin_name like '%" . $searchVal . "%'" ;
                }
            }else if($searchKey == 'title')
            {
                if($matchType == 'eq'){
                    $where .= " AND report.goods_title = '" . $searchVal . "'" ;
                }else {
                    $where .= " AND report.goods_title like '%" . $searchVal . "%'";
                }
            }

        } else if (!empty($searchVal)) {
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

        if ($params['show_type'] == 2 && $params['limit_num'] > 0 && $params['count_periods'] == 0) {
            $offset = 0;
            $limit = (int)$params['limit_num'] ;
        }

        if ((int)$params['time_type'] === 99) {
            $where .= sprintf(
                '%s report.create_time>=%d and report.create_time<=%d',
                $where ? ' AND' : '',
                (int)$params['search_start_time'],
                (int)$params['search_end_time']
            );
            $params['origin_where'] .= " AND report.create_time>={$params['search_start_time']} AND report.create_time<={$params['search_end_time']}";
            $params['origin_time']  = '  AND create_time >= ' .$params['search_start_time'] . ' AND create_time <= ' . $params['search_end_time'] ;
            $min_ym = date('Ym',$params['search_start_time']) ;
            $max_ym = date('Ym',$params['search_end_time']) ;
            $day_param = ($params['search_end_time'] + 1 - $params['search_start_time']) / 86400;
        } else {
            $ors = [];
            $origin_time = [];
            $time_arr = $this->getSiteLocalTime(array_keys(\App\getAmazonSitesConfig()), $params['time_type'], $params['search_start_time'], $params['search_end_time']);
            foreach ($time_arr as $times) {

                $min_ym = empty($min_ym) ? date('Ym',$times['start']) : ($min_ym > date('Ym',$times['start']) ? date('Ym',$times['start']) : $min_ym) ;
                $max_ym = empty($max_ym) ? date('Ym',$times['end']) : ($max_ym < date('Ym',$times['end']) ? date('Ym',$times['end']) : $max_ym) ;
                $ors[] = sprintf(
                    '(report.site_id in (%s) and report.create_time>=%d and report.create_time<=%d)',
                    $times['site_id'],
                    (int)$times['start'],
                    (int)$times['end']
                );
                $origin_time[] = sprintf(
                    '(site_id in (%s) and create_time>=%d and create_time<=%d)',
                    $times['site_id'],
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

        $method = [
            'getListByUnGoods',
            'getListByGoods',
            'getListByOperators'
        ][$type];

        $isReadAthena = false;//其他地方无需传该参数

        //需要读取athena 的才使用该方法，其他不使用
        $big_data_user = "255981,33882,108142,22819,26060,34726,45723,53247,47562,57082,59221,255687,137346,255371,121069,83780,62473,74734,82142,337446,90330,95578,95336,99204,114937,101119,101133,114092,121675,346891,255707,98806,96975,105015,96119,95430,213581,219775,240755,243595,203705,185031,217593,245779,256968,220051,201375,243823,247442,268287,261217,310036,262106,306543,269036,21";
        $is_goods_day_report = false;//日报表才读
        if(($params['count_periods'] == 0 || $params['count_periods'] == 1) && $params['cost_count_type'] != 2){ //按天或无统计周期
            $is_goods_day_report = true;
        }
        if ($method == 'getListByGoods' and $day_param > 90 AND in_array($userInfo['user_id'],explode(",",$big_data_user)) and $is_goods_day_report){
            $isReadAthena = true;
        }



        $limit = ($offset > 0 ? " OFFSET {$offset}" : '') . " LIMIT {$limit}";
        $dataChannel = $searchType === 0 ? 'Presto' : 'ES';
        $className = "\\App\\Model\\DataArk\\AmazonGoodsFinanceReportByOrder{$dataChannel}Model";
        $amazonGoodsFinanceReportByOrderMD = new $className($userInfo['dbhost'], $userInfo['codeno'],$isReadAthena);
        $amazonGoodsFinanceReportByOrderMD->dryRun(env('APP_TEST_RUNNING', false));
        $params['min_ym'] = $min_ym ;
        $params['max_ym'] = $max_ym ;
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

        return Result::success($result);
    }

    public function getUnGoodsDatas()
    {
        return $this->init(0);
    }

    public function getGoodsDatas()
    {
        return $this->init(1);
    }

    public function getOperatorsDatas()
    {
        return $this->init(2);
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
