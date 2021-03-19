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

use App\Model\CrontabModel;
use Captainbi\Hyperf\Exception\BusinessException;
use Captainbi\Hyperf\Util\Auth;
use Captainbi\Hyperf\Util\Log;
use Captainbi\Hyperf\Util\Unique;
use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class SbvService extends BaseService {
    /**
     * 像素类型
     */
    protected $pixType = [
        '1280*720',
        '1920*1080',
        '3840*2160'
    ];

    /**
     * 特效类型
     */
    protected $effectType = [
        '1' => 'fadeblack',
        '2' => 'slideleft',
        '3' => 'slideright',
        '4' => 'slideup',
        '5' => 'slidedown',
        '6' => 'circleclose',
        '7' => 'circleopen',
        '8' => 'dissolve',
        '9' => 'pixelize',
        '10' => 'radial',
    ];

    /**
     * 字体类型
     */
    protected $fontType = [
        'Anton-Regular.ttf',
        'DoHyeon-Regular.ttf',
        'FjallaOne-Regular.ttf',
        'IndieFlower.ttf',
        'JosefinSans-Bold.ttf',
        'JosefinSans-BoldItalic.ttf',
        'JosefinSans-Italic.ttf',
        'JosefinSans-Light.ttf',
        'JosefinSans-LightItalic.ttf',
        'JosefinSans-Regular.ttf',
        'JosefinSans-SemiBold.ttf',
        'JosefinSans-SemiBoldItalic.ttf',
        'JosefinSans-Thin.ttf',
        'JosefinSans-ThinItalic.ttf',
        'Lobster-Regular.ttf',
        'Oswald-Bold.ttf',
        'Oswald-ExtraLight.ttf',
        'Oswald-Light.ttf',
        'Oswald-Medium.ttf',
        'Oswald-Regular.ttf',
        'Oswald-SemiBold.ttf',
        'Pacifico-Regular.ttf',
        'PlayfairDisplay-Black.ttf',
        'PlayfairDisplay-BlackItalic.ttf',
        'PlayfairDisplay-Bold.ttf',
        'PlayfairDisplay-BoldItalic.ttf',
        'PlayfairDisplay-Italic.ttf',
        'PlayfairDisplay-Regular.ttf',
        'Ranga-Bold.ttf',
        'Ranga-Regular.ttf',
        'RobotoSlab-Bold.ttf',
        'RobotoSlab-Light.ttf',
        'RobotoSlab-Regular.ttf',
        'RobotoSlab-Thin.ttf',
        'ZCOOL-Addict-Italic-01.ttf',
        'ZCOOL-Addict-Italic-02.ttf',
        '站酷高端黑修订151105.ttf',
        '站酷酷黑体.ttf',
        '站酷快乐体2016修订版.ttf',
        '站酷庆科黄油体.ttf',
        '站酷文艺体.ttf',
        '站酷小薇LOGO体.otf',

    ];


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

    /**
     * @param $request_data
     * @return array
     */
    public function submitVideo($request_data, $header){
        //验证
        $rule = [
            'display_type' => 'in:0|filled',
            'total_time' => 'required|integer|between:6,45',
            'pix_type' => 'in:0,1|filled',
            'font_list' => 'array|filled',
            'font_list.*.time_start' => 'integer|filled|lt:total_time|gte:0',
            'font_list.*.time_end' => 'integer|filled|gt:font_list.*.time_start|lte:total_time',
            'font_list.*.content' => 'string|filled',
            'font_list.*.font_type' => 'integer|in:0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33|filled',
            'font_list.*.font_size' => 'integer|filled|gt:0',
            'font_list.*.font_bold' => 'integer|in:0,1|filled',
            'font_list.*.font_color' => 'string|filled',
            'font_list.*.x' => 'numeric|filled|gte:0',
            'font_list.*.y' => 'numeric|filled|gte:0',
            'video_list' => 'required|array|min:1',
            'video_list.*.time_start' => 'required|integer|filled|lt:total_time|gte:0',
            'video_list.*.time_end' => 'required|integer|filled|gt:video_list.*.time_start|lte:total_time',
            'video_list.*.pic_url' => 'required|string|filled',
            'video_list.*.effect_type' => 'required|integer|in:0,1,2,3,4,5,6,7,8,9,10|filled',
            'music_url' => 'string|filled',
        ];

        $res = $this->validate($request_data, $rule);
        if($res['code']== 0){
            return $res;
        }

        //获取唯一id
        $snowflakeId = Unique::snowflake();

        if(!isset($header['authorization'][0])){
            return [
                'code' => 0,
                'msg'  => trans('auth.no_authorization'),
            ];
        }
        $authorization = $header['authorization'][0];

        //最终数据
        $crontabArr = [
            'type'=>0,
            'is_success'=>0,
            'snow_flake_id'=>$snowflakeId,
            'json_info'=>[],
            'consumer_id'=>'',
        ];

        //详情数组
        $jsonInfoArr = [];

        //展示类型
        $jsonInfoArr['display_type'] = $request_data['display_type']??0;

        //字体列表
        $request_data['font_list'] = $request_data['font_list']??[];

        //总时长
        $jsonInfoArr['total_time'] = $request_data['total_time']??10;

        //像素
        $jsonInfoArr['pix_type'] = (isset($request_data['pix_type']) && isset($this->pixType[$request_data['pix_type']]))?$this->pixType[$request_data['pix_type']]:"1280*720";

        //音乐
        $jsonInfoArr['music_url'] = '';
        if(isset($request_data['music_url'])){
            $arr = explode(".", $request_data['music_url']);
            if(!in_array(end($arr), ["aac", "mp3"])){
                return [
                    'code' => 0,
                    'msg'  => trans('sbv.music_type'),
                ];
            }
            $jsonInfoArr['music_url'] = $request_data['music_url'];
        }

        //综合数据
        $video_font_list=[];

        //图片
        foreach ($request_data['video_list'] as $video){
            if(!isset($video['time_start']) || !isset($video['time_end']) || !isset($video['pic_url']) || !isset($video['effect_type'])){
                return [
                    'code' => 0,
                    'msg'  => trans('sbv.miss_pic_param'),
                ];
            }
            $arr = explode(".", $video['pic_url']);
            if(!in_array(end($arr), ["jpg", "png"])){
                return [
                    'code' => 0,
                    'msg'  => trans('sbv.picture_type'),
                ];
            }
            $video['effect_type'] = (isset($video['effect_type']) && isset($this->effectType[$video['effect_type']]))?$this->effectType[$video['effect_type']]:"";
            for ($j=$video['time_start'];$j<$video['time_end'];$j++){
                $video_font_list[$j] = [
                    'pic_url' => $video['pic_url'],
                    'effect_type'=> $video['effect_type'],
                ];
            }
        }

        //字体
        foreach ($request_data['font_list'] as $font){
            $font['font_type'] = (isset($font['font_type']) && isset($this->fontType[$font['font_type']]))?$this->fontType[$font['font_type']]:"Anton-Regular.ttf";
            for ($j=$font['time_start'];$j<$font['time_end'];$j++){
                $video_font_list[$j]['content'] = $font['content'];
                $video_font_list[$j]['font_type'] = $font['font_type'];
                $video_font_list[$j]['font_size'] = $font['font_size'];
                $video_font_list[$j]['font_bold'] = $font['font_bold'];
                $video_font_list[$j]['font_color'] = $font['font_color'];
                $video_font_list[$j]['x'] = $font['x'];
                $video_font_list[$j]['y'] = $font['y'];
            }
        }


        //综合数据
        if(count($video_font_list)!=$jsonInfoArr['total_time']){
            return [
                'code' => 0,
                'msg'  => trans('sbv.time_error'),
            ];
        }

        //排序
        sort($video_font_list);
        $i=0;
        //删除多余数据
        foreach ($video_font_list as $k=>$video_font){
            if(!isset($video_font['pic_url']) || !isset($video_font['effect_type'])){
                return [
                    'code' => 0,
                    'msg'  => trans('sbv.miss_pic_param'),
                ];
            }

            //改成适配ffmpeg格式
            if(isset($video_font_list[$k+1]) && ($video_font['pic_url'] == $video_font_list[$k+1]['pic_url']) &&
                ($video_font['effect_type'] == $video_font_list[$k+1]['effect_type']) &&
                ((!isset($video_font['content']) && (!isset($video_font_list[$k+1]['content']))) ||
                (($video_font['font_type'] == $video_font_list[$k+1]['font_type']) &&
                 ($video_font['font_size'] == $video_font_list[$k+1]['font_size']) &&
                 ($video_font['font_bold'] == $video_font_list[$k+1]['font_bold']) &&
                 ($video_font['font_color'] == $video_font_list[$k+1]['font_color']) &&
                 ($video_font['x'] == $video_font_list[$k+1]['x']) &&
                 ($video_font['y'] == $video_font_list[$k+1]['y'])))
            ){
                unset($video_font_list[$k+1]);
            }else{
                $video_font_list[$i]['time_start'] = $i;
                //持续时间
                $video_font_list[$i]['duration'] = $k+1-$i;
                //新索引开始
                $i = $k+1;
            }

        }

        //最终数据
        $video_font_list = array_values($video_font_list);
        $jsonInfoArr['video_font_list'] = $video_font_list;

        $crontabArr['json_info'] = json_encode($jsonInfoArr);

        //jwt配置
        $jwtConfig = $this->config->get("auth.jwt");
        $keyName = $jwtConfig['key'];
        $auth = Auth::jwtDecode($authorization, $keyName);
        if(!$auth){
            return [
                'code' => 0,
                'msg'  => trans('auth.no_authorization'),
            ];
        }
        $crontabArr['consumer_id'] = $auth['consumer_id'];

        Db::beginTransaction();
        try{
            $crontabModel = CrontabModel::create($crontabArr);
            if(!$crontabModel->id){
                throw new BusinessException(10001, trans('sbv.add_error'));
            }
            Db::commit();

        } catch(\Throwable $ex){
            //写入日志
            Db::rollBack();
            Log::getClient()->error($ex->getMessage());
            return [
                'code' => 0,
                'msg'  => trans('common.error'),
            ];
        }

        $data =  [
            'code' => 1,
            'msg'  => 'success',
            'data' => [
                'id' => $snowflakeId,
            ],
        ];

        return $data;
    }
}
