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

        if (!empty($searchVal)) {
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
                return [];
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
        $className = "\\App\\Model\\DataArk\\{$dataChannel}\\AmazonGoodsFinanceReportByOrderModel";
        $amazonGoodsFinanceReportByOrderMD = new $className($this->user['dbhost'], $this->user['codeno']);

        return $amazonGoodsFinanceReportByOrderMD->{$method}(
            $where,
            $params,
            $limit,
            $sort,
            $order,
            0,
            $channelIds,
            $currencyInfo,
            $exchangeCode,
            $timeLine,
            $deparmentData,
            $searchType,
            $this->user['user_id'],
            $this->user['admin_id']
        );
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
