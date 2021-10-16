<?php

namespace App;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;
use Psr\Http\Message\ServerRequestInterface;

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
// function getStartAndEndTimeAllSite(int $type = 1): array
// {
//     static $siteTimeZones = [
//         1 => 'America/Los_Angeles',
//         2 => 'America/Vancouver',
//         3 => 'America/Mexico_City',
//         4 => 'Europe/Berlin',
//         5 => 'Europe/Madrid',
//         6 => 'Europe/Paris',
//         7 => 'Asia/Kolkata',
//         8 => 'Europe/Rome',
//         9 => 'Europe/London',
//         10 => 'Asia/Shanghai',
//         11 => 'Asia/Tokyo',
//         12 => 'Australia/Canberra',
//         13 => 'America/Sao_Paulo',
//         14 => 'Europe/Istanbul',
//         15 => 'Asia/Riyadh',
//         16 => 'Europe/Amsterdam',
//         17 => 'Asia/Riyadh',
//         18 => 'Asia/Singapore',
//     ];
//
//     $endFormat = 'Y-m-d 23:59:59';
//     $startFormat = 'Y-m-d 00:00:00';
//
//     $now = time();
//     $localDate = [];
//     $datetime = new \DateTime();
//     foreach ($siteTimeZones as $siteId => $timeZone) {
//         $datetime->setTimeZone(new \DateTimeZone($timeZone));
//         $localDate[$siteId]['site_id'] = $siteId;
//         $localDate[$siteId]['start'] = strtotime($datetime->format('Y-m-d 00:00:00P'));
//         $localDate[$siteId]['end'] = strtotime($datetime->format('Y-m-d 23:59:59P'));
//     }
//
//     $startDays = '';
//     switch ($type) {
//         case 0: // 全部
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = 0;
//             }
//             break;
//         case 1: // 一年
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = strtotime('-1 year', $localDate[$siteId]['start']);
//             }
//             break;
//         case 2: // 今天
//             // 初始化的数组值即为今天
//             break;
//         case 3: // 昨天
//             $startDays = $startDays ?: '-1 day';
//         case 4: // 7天
//             $startDays = $startDays ?: '-7 day';
//         case 14: // 近14天
//             $startDays = $startDays ?: '-14 day';
//         case 5: // 近15天
//             $startDays = $startDays ?: '-15 day';
//         case 6: // 近30天
//             $startDays = $startDays ?: '-30 day';
//         case 7: // 近60天
//             $startDays = $startDays ?: '-60 day';
//         case 8: // 近90天
//             $startDays = $startDays ?: '-90 day';
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = strtotime($startDays, $localDate[$siteId]['start']);
//                 $localDate[$siteId]['end'] = strtotime('-1 day', $localDate[$siteId]['end']);
//             }
//             break;
//         case 9: // 本月
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = strtotime(date('Y-m-01', $localDate[$siteId]['start']));
//             }
//             break;
//         case 10: // 上个月
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = strtotime('first day of last month', $localDate[$siteId]['start']);
//                 $localDate[$siteId]['end'] = strtotime('last day of last month', $localDate[$siteId]['end']);
//             }
//             break;
//         case 11: // 今年
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = strtotime('first day of Jan.', $localDate[$siteId]['start']);
//             }
//             break;
//         case 12: // 去年
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = strtotime('first day of Jan. last year', $localDate[$siteId]['start']);
//                 $localDate[$siteId]['end'] = strtotime('last day of Dec. last year', $localDate[$siteId]['end']);
//             }
//             break;
//         case 13: // 上周
//             foreach ($localDate as $siteId => $v) {
//                 if (date('w', strtotime($localDate[$siteId]['start'])) === '1') {
//                     // 今天是周一
//                     $localDate[$siteId]['start']  = date($startFormat, time() - 7 * 86400); // 上周一
//                     $localDate[$siteId]['end'] = date($endFormat, (strtotime('-1 week Sunday') + 86399)) ;// 上周日
//                 } else {
//                     $localDate[$siteId]['start'] = date($startFormat, strtotime('-2 week Sunday') + 86400); // 上周一
//                     $localDate[$siteId]['end'] = date($endFormat, (strtotime('-2 week Sunday') + 7 * 86400 + 86399)); // 上周日
//                 }
//             }
//             break;
//         case 20: // 本季度
//             foreach ($localDate as $siteId => $v) {
//                 $season = ceil((date('n', strtotime($localDate[$siteId]['start']))) / 3); // 当月是第几季度
//                 $localDate[$siteId]['start'] = date($startFormat, mktime(0, 0, 0, $season * 3 - 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))));
//                 $localDate[$siteId]['end'] = date($endFormat, (mktime(0, 0, 0, $season * 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))) - 1));
//             }
//             break;
//         case 21: // 上季度
//             foreach ($localDate as $siteId => $v) {
//                 $season = ceil((date('n', strtotime($localDate[$siteId]['start']))) / 3) - 1;
//                 $localDate[$siteId]['start'] = date($startFormat, mktime(0, 0, 0, $season * 3 - 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))));
//                 $localDate[$siteId]['end'] = date($endFormat, (mktime(0, 0, 0, $season * 3 + 1, 1, date('Y', strtotime($localDate[$siteId]['start']))) - 1));
//             }
//             break;
//         case 22: // 近3个月
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = date($startFormat, strtotime('-2 month', strtotime(date('Y-m', strtotime($localDate[$siteId]['start'])))));
//             }
//             break;
//         case 77: // 上周同日
//             foreach ($localDate as $siteId => $v) {
//                 $localDate[$siteId]['start'] = date($startFormat, strtotime('-7 day', strtotime($localDate[$siteId]['start'])));
//                 $localDate[$siteId]['end'] = date($endFormat, strtotime('-7 day', strtotime($localDate[$siteId]['end'])));
//             }
//             break;
//         case 88: // 本周
//             foreach ($localDate as $siteId => $v) {
//                 // 当天为周一的话，本周既只有一天
//                 if (date('w', strtotime($localDate[$siteId]['start'])) !== '1') {
//                     // 获取本周一日期
//                     $localDate[$siteId]['start'] = date($startFormat, strtotime('last Monday', $localDate[$siteId]['start']));
//                 }
//             }
//             break;
//         default:
//             break;
//     }
//
//     foreach ($localDate as $siteId => $v) {
//         $localDate[$siteId]['start'] = strtotime($localDate[$siteId]['start']);
//         $localDate[$siteId]['end'] = strtotime($localDate[$siteId]['end']);
//     }
//
//     return $localDate;
// }

function getStartAndEndTimeAllSite($type = 1)
{
//    $now_int = time();
//    $localDate = array();
//    date_default_timezone_set("America/Los_Angeles");
//    $localDate[1]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[1]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("America/Vancouver");
//    $localDate[2]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[2]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("America/Mexico_City");
//    $localDate[3]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[3]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Europe/Berlin");
//    $localDate[4]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[4]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Europe/Madrid");
//    $localDate[5]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[5]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Europe/Paris");
//    $localDate[6]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[6]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Asia/Kolkata");
//    $localDate[7]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[7]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Europe/Rome");
//    $localDate[8]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[8]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Europe/London");
//    $localDate[9]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[9]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Asia/Shanghai");
//    $localDate[10]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[10]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Asia/Tokyo");
//    $localDate[11]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[11]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Australia/Canberra");
//    $localDate[12]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[12]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("America/Sao_Paulo");
//    $localDate[13]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[13]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Europe/Istanbul");
//    $localDate[14]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[14]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    date_default_timezone_set("Asia/Riyadh");
//    $localDate[15]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[15]["end"] = date("Y-m-d 23:59:59", $now_int);
//
//    //荷兰
//    date_default_timezone_set("Europe/Amsterdam");
//    $localDate[16]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[16]["end"] = date("Y-m-d 23:59:59", $now_int);
//    //沙特阿拉伯
//    date_default_timezone_set("Asia/Riyadh");
//    $localDate[17]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[17]["end"] = date("Y-m-d 23:59:59", $now_int);
//    //新加坡
//    date_default_timezone_set("Asia/Singapore");
//    $localDate[18]["start"] = date("Y-m-d 00:00:00", $now_int);
//    $localDate[18]["end"] = date("Y-m-d 23:59:59", $now_int);

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
     $now_int = time();
     $localDate = [];
     $datetime = new \DateTime();
     foreach ($siteTimeZones as $siteId => $timeZone) {
         //$datetime->setTimeZone(new \DateTimeZone($timeZone));   /改为全部使用北京时间
         $localDate[$siteId]['site_id'] = $siteId;
         $localDate[$siteId]['start'] = date('Y-m-d 00:00:00', strtotime($datetime->format('Y-m-d 00:00:00P')));
         $localDate[$siteId]['end'] = date('Y-m-d 23:59:59', strtotime($datetime->format('Y-m-d 23:59:59P')));
     }

    if ($type == 0) { // 全部
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", 0);
            $localDate[$key]["end"]   = date("Y-m-d 23:59:59", time() + 24*3600);
        }
    }
    if ($type == 1) { // 一年
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-1 year", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"]   = date("Y-m-d 23:59:59", strtotime($localDate[$key]["end"]));
        }
    }
    if ($type == 2) { // 今天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime($localDate[$key]["start"]));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime($localDate[$key]["end"]));
        }
    }
    if ($type == 3) { // 昨天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-1 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }
    if ($type == 4) { // 7天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-7 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }
    if ($type == 5) { // 近15天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-15 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }
    if ($type == 6) { // 近30天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-30 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }
    if ($type == 7) { // 近60天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-60 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }
    if ($type == 8) { // 近90天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-90 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }
    if ($type == 9) { // 本月
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime(date("Y-m-01", strtotime($localDate[$key]["start"]))));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime($localDate[$key]["end"]));
        }
    }
    if ($type == 10) { // 上个月
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-1 month", strtotime(date("Y-m", time()))));
            $localDate[$key]["end"]   =  date("Y-m-d 23:59:59", strtotime(-date('d').'day'));
        }
    }
    if ($type == 11) { // 今年
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00",strtotime(date('Y-01-01')));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime($localDate[$key]["end"]));
        }
    }
    if ($type == 12) { // 去年
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00",strtotime(date("Y-01-01", strtotime(date('Y-01-01')) - 1)));
            $localDate[$key]["end"]   = date("Y-m-d 00:00:00",strtotime(date('Y-01-01')) - 1);
        }
    }
    if($type == 13){ //上周
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            if (date('w') == '1') {
                //今天是周一
                $localDate[$key]["start"]  = date('Y-m-d 00:00:00', time()-7*24*3600); //上周一
                $localDate[$key]["end"] = date('Y-m-d 23:59:59', (strtotime("-1 week Sunday") +86399)) ;//上周天
            } else {
                $localDate[$key]["start"] = date('Y-m-d 00:00:00', strtotime("-2 week Sunday")+24*3600); //上周一
                $localDate[$key]["end"] = date('Y-m-d 23:59:59', (strtotime("-2 week Sunday") + 7*86400+86399)) ;//上周日
            }
        }
    }
    if ($type == 14) { // 近14天
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-14 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-1 day", strtotime($localDate[$key]["end"])));
        }
    }

    if ($type == 20) { // 本季度
        foreach ($localDate as $key => $value) {
            $season = ceil((date('n' , strtotime($localDate[$key]["start"])))/3);//当月是第几季度
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", mktime(0, 0, 0,$season*3-3+1,1,date('Y' , strtotime($localDate[$key]["start"]))));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", (mktime(0, 0, 0,$season*3+1,1,date('Y' , strtotime($localDate[$key]["start"]))) - 1) );
        }
    }
    if ($type == 21) { // 上季度
        foreach ($localDate as $key => $value) {
            $season = ceil((date('n' , strtotime($localDate[$key]["start"])))/3)-1;
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00",mktime(0, 0, 0,$season*3-3+1,1,date('Y' ,strtotime($localDate[$key]["start"]))));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", (mktime(0, 0, 0,$season*3+1,1,date('Y' , strtotime($localDate[$key]["start"]))) - 1) );
        }
    }
    if($type == 22){  //近3个月
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-2 month", strtotime(date("Y-m", strtotime($localDate[$key]["start"])))));
            $localDate[$key]["end"]   = date("Y-m-d 23:59:59", strtotime($localDate[$key]["end"]));
        }
    }
    if($type == 23){  //过去6个月
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-6 month", strtotime(date("Y-m-d", strtotime($localDate[$key]["start"])))));
            $localDate[$key]["end"]   = date("Y-m-d 23:59:59", strtotime($localDate[$key]["end"]));
        }
    }
    if ($type == 77) {  // 上周同日
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            $localDate[$key]["start"] = date("Y-m-d 00:00:00", strtotime("-7 day", strtotime($localDate[$key]["start"])));
            $localDate[$key]["end"] = date("Y-m-d 23:59:59", strtotime("-7 day", strtotime($localDate[$key]["end"])));
        }
    }
    if($type == 88){ //本周
        foreach ($localDate as $key => $value) {
            $localDate[$key]["site_id"] = $key;
            if (date('w') == '1') {
                //今天是周一
                $localDate[$key]["start"] = date("Y-m-d 00:00:00", time());
                $localDate[$key]["end"] = date("Y-m-d 23:59:59", time());
            } else {
                //获取上周天日期
                $localDate[$key]["start"] = date("Y-m-d 00:00:00",strtotime("-1 week Sunday") + 24*3600);
                $localDate[$key]["end"] = date("Y-m-d 23:59:59",strtotime(date("Y-m-d",strtotime("today")))+86399);
            }
        }
    }

    // date_default_timezone_set("Asia/Shanghai");
    foreach ($localDate as $key => $value) {
        $localDate[$key]["start"] = strtotime($localDate[$key]["start"]);
        $localDate[$key]["end"] = strtotime($localDate[$key]["end"]);
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
    /*暂时写死$config = ApplicationContext::getContainer()->get(ConfigInterface::class)->get('site', []);
    if (null !== ($data = @json_decode($config['infos'] ?? '', true))) {
        $sites = $data;
    }*/
    $sites = array(
        1 => array( "country_key" => "g_usa", "currency_code" => "USD", "currency_symbol" => "$", "code" => "US", "area" => "NorthAmerica",  "siteUrl" => "https://mws.amazonservices.com", "website" => "https://www.amazon.com","site_group_id" => 1 , "volume_unit"=> "ft³"),
        2 => array( "country_key" => "g_canada", "currency_code" => "CAD", "currency_symbol" => "C$", "code" => "CA", "is_sale" => "NorthAmerica",  "siteUrl" => "https://mws.amazonservices.ca", "website" => "http://www.amazon.ca" ,"site_group_id" => 1 , "volume_unit"=> "m³"),
        3 => array("country_key" => "g_mexico", "currency_code" => "MXN", "currency_symbol" => "Mex$", "code" => "MX", "is_sale" => "NorthAmerica", "siteUrl" => "https://mws.amazonservices.com", "website" => "http://www.amazon.com.mx","site_group_id" => 1 , "volume_unit"=> "dm ³"),
        4 => array("country_key" => "g_germany", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "DE", "is_sale" => "Europe",  "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.de" ,"site_group_id" => 2 , "volume_unit"=> "m³"),
        5 => array("country_key" => "g_spain", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "ES", "is_sale" => "Europe", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.es" ,"site_group_id" => 2 , "volume_unit"=> "m³"),
        6 => array("country_key" => "g_france", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "FR", "is_sale" => "Europe",  "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.fr","site_group_id" => 2 , "volume_unit"=> "m³"),
        7 => array("country_key" => "g_india", "currency_code" => "INR", "currency_symbol" => "₹", "code" => "IN", "is_sale" => "Europe", "siteUrl" => "https://mws.amazonservices.in", "website" => "http://www.amazon.in","site_group_id" => 2 , "volume_unit"=> "0"),
        8 => array("country_key" => "g_italy", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "IT", "is_sale" => "Europe", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.it","site_group_id" => 2 , "volume_unit"=> "m³"),
        9 => array("country_key" => "g_england", "currency_code" => "GBP", "currency_symbol" => "£", "code" => "UK", "is_sale" => "Europe", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.co.uk"  ,"site_group_id" => 2 , "volume_unit"=> "ft³"),
        10 => array("country_key" => "g_china", "currency_code" => "CNY", "currency_symbol" => "￥", "code" => "CN", "is_sale" => "China", "siteUrl" => "https://mws.amazonservices.com.cn", "website" => "http://www.amazon.cn" ,"site_group_id" => 3, "volume_unit"=> ""),
        11 => array("country_key" => "g_japan", "currency_code" => "JPY", "currency_symbol" => "¥", "code" => "JP", "is_sale" => "Japan", "siteUrl" => "https://mws.amazonservices.jp", "website" => "http://www.amazon.co.jp","site_group_id" => 4, "volume_unit"=> "cm³"),
        12 => array("country_key" => "g_australia", "currency_code" => "AUD", "currency_symbol" => "A$", "code" => "AU", "is_sale" => "Australia", "siteUrl" => "https://mws.amazonservices.com.au", "website" => "http://www.amazon.com.au" ,"site_group_id" => 5, "volume_unit"=> "m³"),
        13 => array("country_key" => "g_brazil", "currency_code" => "BRL", "currency_symbol" => "R$", "code" => "BR", "is_sale" => "NorthAmerica",  "siteUrl" => "https://mws.amazonservices.com", "website" => "http://www.amazon.com.br/" ,"site_group_id" => 1 , "volume_unit"=> "m³"),
        14 => array("country_key" => "g_turkey", "currency_code" => "TRY", "currency_symbol" => "₺", "code" => "TR", "is_sale" => "Europe", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "http://www.amazon.com.tr/","site_group_id" => 2 , "volume_unit"=> "m³"),
        15 => array("country_key" => "g_arabic", "currency_code" => "AED", "currency_symbol" => "AED", "code" => "AE", "is_sale" => "Europe",  "siteUrl" => "https://mws.amazonservices.ae", "website" => "http://www.amazon.ae/" ,"site_group_id" => 2 , "volume_unit"=> "m³") ,
        16 => array("country_key"=>"g_netherlands", "currency_code" => "EUR", "currency_symbol" => "€", "code" => "NL", "is_sale" => "Europe", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "https://www.amazon.nl" ,"site_group_id" => 2 , "volume_unit"=> "m³") ,
        17 => array("country_key"=>"g_arabia", "currency_code" => "SAR", "currency_symbol" => "SAR", "code" => "SA", "is_sale" => "Europe", "siteUrl" => "https://mws-eu.amazonservices.com", "website" => "https://www.amazon.sa" ,"site_group_id" => 2 , "volume_unit"=> "m³") ,
        18 => array("country_key"=>"g_singapore", "currency_code" => "SGD", "currency_symbol" => "S$", "code" => "SG", "is_sale" => "Singapore", "siteUrl" => "https://mws-fe.amazonservices.com", "website" => "https://www.amazon.sg","site_group_id" => 6, "volume_unit"=> "m³") ,
    ) ;

    // 对于获取不到配置的，返回空数组或上一次的配置
    return $sites;
}

/**
 * 获取用户信息
 *
 * @return array
 */
function getUserInfo(): array
{
    return ApplicationContext::getContainer()->get(ServerRequestInterface::class)->getAttribute('userInfo', []);
}

function getUserIdMod($user_id){
    $big_selling_users = config("common.big_selling_users");
    $user_id_arr = array();
    if (!empty($big_selling_users)){
        $user_id_arr = explode(',',$big_selling_users);
    }

    if (in_array($user_id,$user_id_arr)){
        return ($user_id+20);
    }
    return ($user_id % 20);
}

