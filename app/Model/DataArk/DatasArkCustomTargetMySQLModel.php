<?php

namespace App\Model\DataArk;

use App\Model\AbstractMySQLModel;

class DatasArkCustomTargetMySQLModel extends AbstractMySQLModel
{
    protected $table = 'datas_ark_custom_target';

    protected $connection = 'erp_report';



    public function getList($where = '' , $data = '*' ,  $limit = '' ,$order = '' ,$group = ''){
        if(empty($where)){
            $where = "status = 1" ;
        }else{
            $where .= " AND status = 1";
        }
        $rt = $this->select($where , $data , '' , $limit , $order ,$group) ;
        return $rt ;
    }

    /**
     * 按类型获取指标列表
     * @param $user_id
     * @param int $type
     * @param string $order
     * @return array
     */
    public function getTargetListByUserIdType($user_id, $type = 0, $order = ''){
        if ($type == 1){
            $target_type = "target_type = 1";
        }elseif ($type == 2){
            $target_type = "target_type = 2";
        }else{
            $target_type = "target_type IN(1, 2)";
        }
        $list = $this->select("user_id = {$user_id} AND {$target_type} AND status = 1", "*", '', '', $order);

        return $list;
    }

    /**
     * 自定义算法、新增指标列表
     * @param array $param
     * @return array
     */
    public function getCustomTargetList($param = []){
        $page = intval($param['page']) ? intval($param['page']) : 1;
        $row = intval($param['row']) ? intval($param['row']) : 10;
        $targetType = intval($param['target_type']);
        $is_default = intval($param['is_default']) ? intval($param['is_default']) : 0;
        $keywords = isset($param['keywords']) && $param['keywords'] ? $param['keywords'] : "";
        $sort = !empty($param['sort']) ? $param['sort'] : "modified_time";
        $order = !empty($param['order']) ? $param['order'] : "DESC";
        $orderBy = "";
        if($sort && $order){
            $orderBy = "," . $sort . ' ' . $order;
        }

        $start = ($page - 1) * $row;
        $limit = "{$start}, {$row}";

        $where = "user_id = {$param['user_id']} AND target_type = {$targetType} AND status = 1";
        if($is_default){
            $where .= " AND is_default = 1";
        }
        if(!empty($keywords)){
            $where .= " AND target_name like '%{$keywords}%'";
        }

        $list = $this->select($where,'*', "", $limit, 'is_default DESC' . $orderBy);
        $count = $this->count($where);
        if ($list){

            if ($targetType == 1){
                $targetId = array_column($list, 'id');

                /** @var datas_ark_custom_target_detail_model $targetDetailModel */
                $targetDetailModel = base::load_model_class('datas_ark_custom_target_detail_model','finance');
                $detailCount = $targetDetailModel->select("user_id = {$param['user_id']} AND datas_ark_custom_target_id IN(".implode(',', $targetId).")", 'count(*) as count, datas_ark_custom_target_id', '', '', '', 'datas_ark_custom_target_id');
                $mapCount = [];
                if ($detailCount){
                    foreach ($detailCount as $val){
                        $mapCount[$val['datas_ark_custom_target_id']] = $val['count'];
                    }
                }
            }

            $countDimensionArr = [
                1 => "sku及店铺",
                2 => "sku",
                3 => "店铺"
            ];
            $formatTypeArr = [
                1 => '整数',
                2 => '小数',
                3 => '百分比',
                4 => '货币'
            ];

            $arkConfig = base::load_config("datas_ark");
            $channelTargetList = $arkConfig['no_goods_target'];
            $goodsTargetList =  $arkConfig['goods_target'];
            $operationTargetList =  $arkConfig['operation_target'];
            $ark_custom_param =  $arkConfig['ark_custom_param'];//变量参数组

            //新增指标
            $new_target = $this->getList("user_id = {$param['user_id']} AND target_type = 1","target_name as name,count_periods,count_dimension,target_key,is_sort,is_can_time_show,1 as is_can_custom");
            $operational_char_arr = array("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".","+", "-", "*", "/", "", "(", ")");
            $channel_target_data = $goods_target_data = $op_target_data = [];
            if($channelTargetList){
                foreach ($channelTargetList as $channel_target){
                    if($channel_target['child']){
                        foreach ($channel_target['child'] as $k=>$v){
                            $channel_target_data[$v['key']] = $v;
                        }
                    }
                }
                if($new_target) {
                    foreach ($new_target as $item) {
                        if (in_array($item['count_dimension'], [1, 3])) {
                            $channel_target_data[$item['target_key']] = $item;
                        }
                    }
                }
            }
            if($goodsTargetList){
                foreach ($goodsTargetList as $goods_target){
                    if($goods_target['child']){
                        foreach ($goods_target['child'] as $k=>$v){
                            $goods_target_data[$v['key']] = $v;
                        }
                    }
                }
                if($new_target) {
                    foreach ($new_target as $item) {
                        if (in_array($item['count_dimension'], [1, 2])) {
                            $goods_target_data[$item['target_key']] = $item;
                        }
                    }
                }
            }
            if($operationTargetList){
                foreach ($operationTargetList as $op_target){
                    if($op_target['child']){
                        foreach ($op_target['child'] as $k=>$v){
                            $op_target_data[$v['key']] = $v;
                        }
                    }
                }
                if($new_target) {
                    foreach ($new_target as $item) {
                        if ($item['count_dimension'] == 1) {
                            $op_target_data[$item['target_key']] = $item;
                        }
                    }
                }
            }
            $user_admin_md = base::load_model("user_admin_model");
            $user_data     = $user_admin_md->select("user_id={$param['user_id']}  and status=1", 'id,username,realname,is_master');
            $user_data = array_column($user_data,null,'id');
            foreach ($list as $key => $val){
                if($targetType == 2){
                    $formula_arr = json_decode($val['formula_json'],true);
                    if ($formula_arr){
                        foreach ($formula_arr as $k => $f_key){
                            if(!in_array($f_key,$operational_char_arr)){
                                if(in_array($f_key,array_keys($ark_custom_param))){
                                    $formula_arr[$k] = $ark_custom_param[$f_key];
                                }else{
                                    if(!is_numeric($f_key)){
                                        if($val['count_dimension'] == 1){
                                            $formula_arr[$k] = $op_target_data[$f_key]['name'];
                                        }elseif ($val['count_dimension'] == 2){
                                            $formula_arr[$k] = $goods_target_data[$f_key]['name'];
                                        }else{
                                            $formula_arr[$k] = $channel_target_data[$f_key]['name'];
                                        }
                                    }
                                }
                            }
                        }
                    }
                    $val['formula_text'] = implode("",$formula_arr);
                }
                $real_name = !empty($user_data[$val['admin_id']]['realname']) ? $user_data[$val['admin_id']]['realname'] : $user_data[$val['admin_id']]['username'];
                $val['creator'] = empty($val['admin_id']) ? '系统' : $real_name;
                $val['count_periods_text'] = $val['count_periods'] == 3 ? "按月" : '';

                $val['count_dimension_text'] = $countDimensionArr[$val['count_dimension']];

                $val['format_type_text'] = $formatTypeArr[$val['format_type']];

                $child_position_text = '';
                if ($val['count_dimension'] == 1)
                {
                    $parent_position_text = $operationTargetList[$val['parent_position']]['name'];
                    $child = $operationTargetList[$val['parent_position']]['child'];
                }
                elseif ($val['count_dimension'] == 2)
                {
                    $parent_position_text = $goodsTargetList[$val['parent_position']]['name'];
                    $child = $goodsTargetList[$val['parent_position']]['child'];
                }else{
                    $parent_position_text = $channelTargetList[$val['parent_position']]['name'];
                    $child = $channelTargetList[$val['parent_position']]['child'];
                }
                if ($child){
                    foreach ($child as $item){
                        if ($val['child_position'] == $item['key']){
                            $child_position_text = $item['name'];
                        }
                    }
                }

                $val['parent_position_text'] = $parent_position_text;
                $val['child_position_text'] = $child_position_text;

                //可编辑
                $val['can_edit'] = true;
                $val['edit_msg'] = '';
                //是否可删除
                $val['can_delete'] = true;
                $val['delete_msg'] = '';
                if ($targetType == 1 && !empty($mapCount[$val['id']]))
                {
                    $val['can_delete'] = false;
                    $val['delete_msg'] = '该指标导入过数据，无法删除';
                }
                if ($param['admin_id'] != $val['admin_id'] && !$param['is_master']){
                    if (!$val['is_default']){
                        $val['can_edit'] = false;
                        $val['edit_msg'] = '不可编辑他人创建的指标';
                    }
                    $val['can_delete'] = false;
                    $val['delete_msg'] = '不可删除他人创建的指标';
                }
                if ($val['is_default'])
                {
                    $val['can_delete'] = false;
                    $val['delete_msg'] = '系统指标不能删除';
                }

                $list[$key] = $val;
            }
        }

        return [
            'list' => $list,
            'count' => $count,
            'page' => $page
        ];
    }

    /**
     * 合并系统默认指标、自定义指标、自定义算法
     * @param array $param
     * @return array|mixed
     * @throws Exception
     */
    public function getSysCustomTargetList($param = []){
        $userId = $param['user_id'];

        //todo 后期看是否加缓存
        $cacheList = [];
        if (!$cacheList){
            $list = $this->getTargetListByUserIdType($userId, 0, 'modified_time DESC, id DESC');
            $customChannelTarget = [];
            $customGoodsTarget = [];
            $customOperationTarget = [];
            if($_REQUEST['is_bi_request'] == 1){
                $arkConfig = base::load_config("analysis_data");
            }else{
                $arkConfig = base::load_config("datas_ark");
            }
            $channelTargetList = $arkConfig['no_goods_target'];
            $goodsTargetList =  $arkConfig['goods_target'];
            $operationTargetList =  $arkConfig['operation_target'];
            $ark_custom_param =  $arkConfig['ark_custom_param'];//变量参数组
            $fba_fields =  $arkConfig['fba_fields'];//fba字段
            //新增指标
            $new_target = $this->getList("user_id = {$param['user_id']} AND target_type = 1 AND status = 1","target_name as name,count_periods,count_dimension,target_key,is_sort,is_can_time_show,1 as is_can_custom");
            $operational_char_arr = array("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".","+", "-", "*", "/", "", "(", ")");
            $channel_target_data = $goods_target_data = $op_target_data = [];
            if($channelTargetList){
                foreach ($channelTargetList as $channel_target){
                    if($channel_target['child']){
                        foreach ($channel_target['child'] as $k=>$v){
                            $channel_target_data[$v['key']] = $v;
                        }
                    }
                }
            }
            if($goodsTargetList){
                foreach ($goodsTargetList as $goods_target){
                    if($goods_target['child']){
                        foreach ($goods_target['child'] as $k=>$v){
                            $goods_target_data[$v['key']] = $v;
                        }
                    }
                }
            }
            if($operationTargetList){
                foreach ($operationTargetList as $op_target){
                    if($op_target['child']){
                        foreach ($op_target['child'] as $k=>$v){
                            $op_target_data[$v['key']] = $v;
                        }
                    }
                }
            }

            if($new_target) {
                foreach ($new_target as $item) {
                    if ($item['count_dimension'] == 1) {
                        $op_target_data[$item['target_key']] = $item;
                        $goods_target_data[$item['target_key']] = $item;
                        $channel_target_data[$item['target_key']] = $item;
                    }elseif ($item['count_dimension'] == 2) {
                        $goods_target_data[$item['target_key']] = $item;
                    }elseif ($item['count_dimension'] == 3) {
                        $channel_target_data[$item['target_key']] = $item;
                    }
                }
            }

            if (!empty($list)){
                foreach ($list as $val){
                    $formula_arr = $val['formula_json'] ? json_decode($val['formula_json'],true) : [];
                    $is_can_count = 1;
                    if ($formula_arr){
                        foreach ($formula_arr as $k => $f_key){
                            if(in_array($f_key,$fba_fields)){
                                $is_can_count = 0;
                            }
                            if(!in_array($f_key,$operational_char_arr)){
                                if(in_array($f_key,array_keys($ark_custom_param))){
                                    $formula_arr[$k] = $ark_custom_param[$f_key];
                                }else{
                                    if(!is_numeric($f_key)){
                                        if($val['count_dimension'] == 1){
                                            $formula_arr[$k] = $op_target_data[$f_key]['name'];
                                        }elseif ($val['count_dimension'] == 2){
                                            $formula_arr[$k] = $goods_target_data[$f_key]['name'];
                                        }else{
                                            $formula_arr[$k] = $channel_target_data[$f_key]['name'];
                                        }
                                    }
                                }
                            }
                        }
                    }
                    $val['formula_text'] = implode("",$formula_arr);

                    $temp = [
                        "name" => $val['target_name'],
                        "msg" => $val['remark'],
                        "key" => $val['target_key'],
                        "is_default" => 0,
                        'is_sort' => $val['is_sort'],
                        'is_can_time_show' => $val['is_can_time_show'],
                        'is_can_custom' => 1,
                        'is_custom' => 1, //自定义
                        'target_type' => $val['target_type'], //1-新增 2-自定义
                        'formula_text' => $val['formula_text'], //公式
                        'count_periods' => $val['count_periods'], //统计周期
                        'is_can_count' => $is_can_count, //是否可以汇总
                        'no_condition_filter' => $is_can_count == 0 ? 1 : 0,//是否可以筛选
                        'fba_status' => $val['count_dimension'] == 2 && $is_can_count == 0 ? 1 : 0,
                        'format_type' => $val['format_type'],
                    ];

                    if ($val['target_type'] == 1)
                    {
                        $temp['count_periods'] = 3;
                    }
                    if ($val['count_dimension'] == 1)
                    {
                        //sku和店铺
                        $customChannelTarget[$val['parent_position']][$val['child_position']][] = $temp;
                        $customGoodsTarget[$val['parent_position']][$val['child_position']][] = $temp;
                        $customOperationTarget[$val['parent_position']][$val['child_position']][] = $temp;
                    }
                    elseif ($val['count_dimension'] == 2)
                    {
                        //sku
                        $customGoodsTarget[$val['parent_position']][$val['child_position']][] = $temp;
                    }
                    elseif ($val['count_dimension'] == 3)
                    {
                        //店铺
                        $customChannelTarget[$val['parent_position']][$val['child_position']][] = $temp;
                    }
                }
            }


            $defaultChannelTargetList = $arkConfig['no_goods_target'];
            if (!empty($customChannelTarget)){
                foreach ($defaultChannelTargetList as $key => $val)
                {
                    if (isset($customChannelTarget[$key]))
                    {
                        $tempParent = $customChannelTarget[$key];
                        $child = $val['child'];
                        $tempCount = 0;
                        foreach ($child as $k => $v)
                        {
                            if (isset($tempParent[$v['key']]))
                            {
                                $insert = $tempParent[$v['key']];

                                $newChild = insertArray($child, $k + 1 + $tempCount, $insert);
                                $defaultChannelTargetList[$key]['child'] = $newChild;
                                $child = $newChild;
                                $tempCount += count($insert);

                                unset($tempParent[$v['key']]);
                            }
                        }

                        if (!empty($tempParent)){
                            foreach ($tempParent as $item){
                                foreach ($item as $i){
                                    array_push($child, $i);
                                }
                            }
                        }

                        $defaultChannelTargetList[$key]['child'] = $child;
                    }
                }
            }

            $defaultGoodsTargetList =  $arkConfig['goods_target'];
            if (!empty($customGoodsTarget)){
                foreach ($defaultGoodsTargetList as $key => $val)
                {
                    if (isset($customGoodsTarget[$key]))
                    {
                        $tempParent = $customGoodsTarget[$key];
                        $child = $val['child'];
                        $tempCount = 0;
                        foreach ($child as $k => $v)
                        {
                            if (isset($tempParent[$v['key']]))
                            {
                                $insert = $tempParent[$v['key']];
                                $newChild = insertArray($child, $k + 1 + $tempCount, $insert);
                                $defaultGoodsTargetList[$key]['child'] = $newChild;
                                $child = $newChild;
                                $tempCount += count($insert);

                                unset($tempParent[$v['key']]);
                            }
                        }

                        if (!empty($tempParent)){
                            foreach ($tempParent as $item){
                                foreach ($item as $i){
                                    array_push($child, $i);
                                }
                            }
                        }

                        $defaultGoodsTargetList[$key]['child'] = $child;
                    }
                }
            }

            $defaultOperationTargetList =  $arkConfig['operation_target'];
            if (!empty($customOperationTarget)){
                foreach ($defaultOperationTargetList as $key => $val)
                {
                    $child = $val['child'];
                    $tempCount = 0;
                    foreach ($child as $k => $v)
                    {
                        if (isset($customOperationTarget[$key][$v['key']]))
                        {
                            $insert = $customOperationTarget[$key][$v['key']];
                            $newChild = insertArray($child, $k + 1 + $tempCount, $insert);
                            $defaultOperationTargetList[$key]['child'] = $newChild;
                            $child = $newChild;
                            $tempCount += count($insert);
                        }
                    }
                }
            }

            $cacheList = [
                'channel_target_list' => $defaultChannelTargetList,
                'goods_target_list' => $defaultGoodsTargetList,
                'operation_target_list' => $defaultOperationTargetList
            ];
        }

        return $cacheList;
    }

    /**
     * 判断指标名称格式判断
     * @param array $param
     * @return array|mixed
     * @throws Exception
     */
    public function chackTargetName($str,$encode='utf-8')
    {
        $len=mb_strlen($str,$encode);
        for($i=0;$i<$len;$i++){
            $tmp=mb_substr($str,$i,1,$encode);
            if(!preg_match("/^[\x{4e00}-\x{9fa5}]+$/u",$tmp) && !preg_match("/^[A-Za-z0-9]+$/",$tmp) && !preg_match('/^[@&\-~\*（）,.，。=_\[\]“”]+$/',$tmp)){
                return false;
            }
        }
        return true;
    }

    public function checkFormulaFormat($formula_arr,$base_targets){
        $arkConfig = base::load_config("datas_ark");
        $ark_custom_param =  $arkConfig['ark_custom_param'];//变量参数组
        $formula_array = $formula_array2 = $base_targets;
        array_push($formula_array,"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".", "+", "-", "*", "/", "(", ")");
        array_push($formula_array2,"(", ")", "");
        $formula_array = array_merge($formula_array,array_keys($ark_custom_param));
        $formula_array2 = array_merge($formula_array2,array_keys($ark_custom_param));
        $formula_array3 = array("+", "-", "*", "/", "", "(", ")");
        $formula_array4 = array("+", "-", "*", "/");
        $is_kh          = 0;
        $a = $formula_arr;
        foreach ($a as $k => $v) {
            if (is_numeric($v)) {//字段为数值 后面必须为运算符号||括号
                if (!in_array($a[$k + 1], $formula_array3)) {
                    $msg = "格式错误，请重新输入";
                    throwMsg($msg,5);
                }
            }
            if (in_array($v, $formula_array2) && $v != '(') {//为字段或者括号 后面必须为 数值 || 指标 || 括号
                if (!in_array($a[$k + 1], $formula_array3)) {
                    $msg = "格式错误，请重新输入";
                    throwMsg($msg,5);
                }
            }
            if (in_array($v, $formula_array4) && !is_numeric($a[$k + 1])) {//为运算符号 必须为
                if (is_null($a[$k + 1])){
                    $msg = "格式错误，请重新输入";
                    throwMsg($msg,5);
                }
                if (!in_array($a[$k + 1], $formula_array2)) {
                    $msg = "格式错误，请重新输入";
                    throwMsg($msg,5);
                }
            }
            if (!is_numeric($v)) {//非数值
                if (!in_array($v, $formula_array)) {//不存在数组里面
                    $msg = "格式错误，请重新输入";
                    throwMsg($msg,3);
                }
            }
            if ($v == '/' && is_numeric($a[$k + 1])) {//除数后面为0 不可保存
                if ($a[$k + 1] == 0) {
                    $msg = "格式错误，请重新输入";
                    throwMsg($msg,3);
                }
            }
            if ($v == '(' || $v == ')') {//括号后面必须正确填值
                if ($v == '(' && in_array($a[$k + 1], array("*", "-", "/", "+", ")", "."))) {
                    throwMsg("格式错误，请重新输入",4);
                }
                if ($v == ')' && in_array($a[$k - 1], array("*", "-", "/", "+", ")", "."))) {
                    throwMsg("格式错误，请重新输入",4);
                }
                if ($v == ')' && $a[$k + 1] == "(") {
                    throwMsg("格式错误，请重新输入",4);
                }
            }
            $is_kh = 1;
        }
        $zfc = '';
        foreach ($a as $key => $val) {
            $zfc .= $val;
        }
        if ($is_kh == 1) {
            $code = $this->isValid($zfc);
            if ($code == 2) {
                throwMsg('验证失败，缺少右括号',2);
            } elseif ($code == 3) {
                throwMsg('"验证通过，缺少左括号',3);
            }
        }
    }

    /**
     * 验证括号是否匹配
     * @param $expstr
     * @return int|string
     */
    public function isValid($expstr)
    {
        $temp = array();
        for ($i = 0; $i < strlen($expstr); $i++) {
            $ch = $expstr[$i];
            switch ($ch) {
                case '(':
                    array_push($temp, '(');
                    break;
                case ')':
                    if (empty($temp) || array_pop($temp) != '(') {
                        return 3;
                    }
            }
        }
        return empty($temp) == true ? 1 : 2;
    }
}
