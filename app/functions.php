<?php

namespace App;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;

/**
 * 获得所有站点的时间控件选定的时间范
 *
 * 本系统所有和时间相关的都使用北京时间计算
 * 从亚马逊获取的数据都转换为北京时间戳进行存储，如
 * 美国亚马逊订单创建时间戳为 1618012800
 * 既为美国站时间 2021-04-09 17:00:00
 *
 * 这边存入到数据库时使用以下算法存储时间戳
 * ```php
 * $timestamp = 1618012800;
 * date_default_timezone_set("America/Los_Angeles");
 * $time = date('Y-m-d H:i:s', $timestamp); // 2021-04-09 17:00:00
 *
 * date_default_timezone_set("Asia/Shanghai");
 * // 存入到数据库时使用本变量，上面的 $timestamp 也会存，但本系统所有时间相关处理都使用 $bjtime
 * $bjtime = strtotime($time); // 1617958800
 * ```
 *
 * @param int $type
 * @return array 返回的数组结构为
 * [
 *     (int)siteId => [
 *         'site_id' => int,
 *         'start' => (int)timestamp,
 *         'end' => (int)timestamp,
 *     ],
 * ]
 * todo 添加测试
 */
function getStartAndEndTimeAllSite(int $type = 1): array
{
    static $siteTimeZones = [
        1 => 'America/Los_Angeles',
        2 => 'America/Vancouver',
        3 => 'America/Mexico_City',
        4 => 'Europe/Berlin',
        5 => 'Europe/Madrid',
        6 => 'Europe/Paris',
        7 => 'Asia/Kolkata',
        8 => 'Europe/Rome',
        9 => 'Europe/London',
        10 => 'Asia/Shanghai',
        11 => 'Asia/Tokyo',
        12 => 'Australia/Canberra',
        13 => 'America/Sao_Paulo',
        14 => 'Europe/Istanbul',
        15 => 'Asia/Riyadh',
        16 => 'Europe/Amsterdam',
        17 => 'Asia/Riyadh',
        18 => 'Asia/Singapore',
    ];

    $endFormat = 'Y-m-d 23:59:59';
    $startFormat = 'Y-m-d 00:00:00';

    $now = time();
    $localDate = [];
    $datetime = new \DateTime();
    foreach ($siteTimeZones as $siteId => $timeZone) {
        $datetime->setTimeZone(new \DateTimeZone($timeZone));
        $localDate[$siteId]['site_id'] = $siteId;
        $localDate[$siteId]['start'] = strtotime($datetime->format('Y-m-d 00:00:00P'));
        $localDate[$siteId]['end'] = strtotime($datetime->format('Y-m-d 23:59:59P'));
    }

    $startDays = '';
    switch ($type) {
        case 0: // 全部
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = 0;
            }
            break;
        case 1: // 一年
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = strtotime('-1 year', $localDate[$siteId]['start']);
            }
            break;
        case 2: // 今天
            // 初始化的数组值即为今天
            break;
        case 3: // 昨天
            $startDays = $startDays ?: '-1 day';
        case 4: // 7天
            $startDays = $startDays ?: '-7 day';
        case 14: // 近14天
            $startDays = $startDays ?: '-14 day';
        case 5: // 近15天
            $startDays = $startDays ?: '-15 day';
        case 6: // 近30天
            $startDays = $startDays ?: '-30 day';
        case 7: // 近60天
            $startDays = $startDays ?: '-60 day';
        case 8: // 近90天
            $startDays = $startDays ?: '-90 day';
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = strtotime($startDays, $localDate[$siteId]['start']);
                $localDate[$siteId]['end'] = strtotime('-1 day', $localDate[$siteId]['end']);
            }
            break;
        case 9: // 本月
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = strtotime(date('Y-m-01', $localDate[$siteId]['start']));
            }
            break;
        case 10: // 上个月
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = strtotime('first day of last month', $localDate[$siteId]['start']);
                $localDate[$siteId]['end'] = strtotime('last day of last month', $localDate[$siteId]['end']);
            }
            break;
        case 11: // 今年
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = strtotime('first day of Jan.', $localDate[$siteId]['start']);
            }
            break;
        case 12: // 去年
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = strtotime('first day of Jan. last year', $localDate[$siteId]['start']);
                $localDate[$siteId]['end'] = strtotime('last day of Dec. last year', $localDate[$siteId]['end']);
            }
            break;
        case 13: // 上周
            foreach ($localDate as $siteId => $v) {
                if (date('w', strtotime($localDate[$siteId]['start'])) === '1') {
                    // 今天是周一
                    $localDate[$siteId]['start']  = date($startFormat, time() - 7 * 86400); // 上周一
                    $localDate[$siteId]['end'] = date($endFormat, (strtotime('-1 week Sunday') + 86399)) ;// 上周日
                } else {
                    $localDate[$siteId]['start'] = date($startFormat, strtotime('-2 week Sunday') + 86400); // 上周一
                    $localDate[$siteId]['end'] = date($endFormat, (strtotime('-2 week Sunday') + 7 * 86400 + 86399)); // 上周日
                }
            }
            break;
        case 20: // 本季度
            foreach ($localDate as $siteId => $v) {
                $season = ceil((date('n', strtotime($localDate[$siteId]['start']))) / 3); // 当月是第几季度
                $localDate[$siteId]['start'] = date($startFormat, mktime(0, 0, 0, $season * 3 - 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))));
                $localDate[$siteId]['end'] = date($endFormat, (mktime(0, 0, 0, $season * 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))) - 1));
            }
            break;
        case 21: // 上季度
            foreach ($localDate as $siteId => $v) {
                $season = ceil((date('n', strtotime($localDate[$siteId]['start']))) / 3) - 1;
                $localDate[$siteId]['start'] = date($startFormat, mktime(0, 0, 0, $season * 3 - 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))));
                $localDate[$siteId]['end'] = date($endFormat, (mktime(0, 0, 0, $season * 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))) - 1));
            }
            break;
        case 22: // 近3个月
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = date($startFormat, strtotime('-2 month', strtotime(date('Y-m', strtotime($localDate[$siteId]['start'])))));
            }
            break;
        case 77: // 上周同日
            foreach ($localDate as $siteId => $v) {
                $localDate[$siteId]['start'] = date($startFormat, strtotime('-7 day', strtotime($localDate[$siteId]['start'])));
                $localDate[$siteId]['end'] = date($endFormat, strtotime('-7 day', strtotime($localDate[$siteId]['end'])));
            }
            break;
        case 88: // 本周
            foreach ($localDate as $siteId => $v) {
                // 当天为周一的话，本周既只有一天
                if (date('w', strtotime($localDate[$siteId]['start'])) !== '1') {
                    // 获取本周一日期
                    $localDate[$siteId]['start'] = date($startFormat, strtotime('last Monday', $localDate[$siteId]['start']));
                }
            }
            break;
        default:
            break;
    }

    foreach ($localDate as $siteId => $v) {
        $localDate[$siteId]['start'] = strtotime($localDate[$siteId]['start']);
        $localDate[$siteId]['end'] = strtotime($localDate[$siteId]['end']);
    }

    return $localDate;
}

/**
 * 获取 amazon sites 配置
 *
 * @return array
 */
function getAmazonSitesConfig(): array
{
    static $sites = [];
    // 虽然该配置理论上不会变
    // 但毕竟该配置使用 apollo 拉取的，不能想当然
    // 所以这里还是每次都去获取最新的配置
    $config = ApplicationContext::getContainer()->get(ConfigInterface::class)->get('site', []);
    if (null !== ($data = @json_decode($config['infos'] ?? '', true))) {
        $sites = $data;
    }

    // 对于获取不到配置的，返回空数组或上一次的配置
    return $sites;
}
