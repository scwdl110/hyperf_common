<?php

namespace App\Controller;

class DataArkController extends AbstractController
{
    protected $user = [];

    public function __construct()
    {
        parent::__construct();
        $this->user = $this->request->getAttribute('userInfo');
    }

    protected function init($type = 1)
    {
        $req = $this->request->all();
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
        $offset = ($page - 1) * $limit;

        $where = '';
        if (count($channelIds) > 1) {
            $where = "report.user_id={$this->user['user_id']} AND report.channel_id IN (" . implode(',' , $channelIds) .')';
        } else {
            $where = "report.user_id={$this->user['user_id']} AND report.channel_id={$channelIds[0]}";
        }

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
        } else {
            $ors = [];
            foreach ($this->getSiteLocalTime(array_keys(\App\getAmazonSitesConfig()), $params['time_type']) as $times) {
                $ors[] = sprintf(
                    '(report.site_id in (%s) and report.create_time>=%d and report.create_time<=%d)',
                    $times['site_id'],
                    (int)$times['start'],
                    (int)$times['end']
                );
            }

            if (empty($ors)) {
                return json_encode([]);
            }
            $ors = join(' OR ', $ors);
            $where .= $where ? " AND ({$ors})" : "({$ors})";
        }

        $method = [
            'getListByUnGoods',
            'getListByGoods',
            'getListByOperators'
        ][$type];

        $limit = ($offset > 0 ? " OFFSET {$offset}" : '') . " LIMIT {$limit}";
        $dataChannel = $searchType === 0 ? 'Presto' : 'ES';
        $className = "\\App\\Model\\DataArk\\AmazonGoodsFinanceReportByOrder{$dataChannel}Model";
        $amazonGoodsFinanceReportByOrderMD = new $className($this->user['dbhost'], $this->user['codeno']);
        $amazonGoodsFinanceReportByOrderMD->dryRun(env('APP_TEST_RUNNING', false));

        return json_encode($amazonGoodsFinanceReportByOrderMD->{$method}(
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
            $this->user['user_id'],
            $this->user['admin_id']
        ));
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

    protected function getSiteLocalTime(array $siteIds, int $timeType = 99): array
    {
        $result = [[
            'end' => 0,
            'start' => 0,
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