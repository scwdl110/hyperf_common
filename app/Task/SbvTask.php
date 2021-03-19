<?php
namespace App\Task;

use Captainbi\Hyperf\Util\File;
use Captainbi\Hyperf\Util\Log;
use Hyperf\Utils\Coroutine;
use Hyperf\Task\Annotation\Task;
use Swoole\Coroutine\System;

class SbvTask
{
    /**
     * 字体目录
     */
    protected $fontPath = BASE_PATH."/storage/font/";

    /**
     * sbv目录
     */
    protected $sbvPath = BASE_PATH."/storage/sbv/";

    /**
     * @Task
     */
    public function handle($id, $json_info)
    {
        //错误数加一
        if(!$id || !$json_info){
            Log::getCrontabClient()->error("sbv:缺少参数");
            return false;
        }

        $jsonInfoArr = json_decode($json_info, true);
        if(!isset($jsonInfoArr['display_type']) || !isset($jsonInfoArr['total_time']) || !isset($jsonInfoArr['pix_type']) || !isset($jsonInfoArr['music_url']) || !isset($jsonInfoArr['video_font_list'])){
            Log::getCrontabClient()->error("sbv:缺少子参数");
            return false;
        }
        $start_time = time();

        //创建目录
        $sbv_path = $this->mk_dir($this->sbvPath);
        if(!$sbv_path){
            Log::getCrontabClient()->error("sbv:文件目录冲突");
            return false;
        }
        return $sbv_path;exit;

        //执行语句
        $infoArr = [];

        //ffmpeg配置
        $ffmpegConfig = $this->config->get("ffmpeg");

        //最终shell
        $shell = "ffmpeg -y -s ".$jsonInfoArr['pix_type']." -b:v ".$ffmpegConfig['video_bite_rate']." -bufsize ".$ffmpegConfig['video_bite_rate']." -c:v libx264 -pix_fmt yuv420p";

        //视频
        foreach ($jsonInfoArr['video_font_list'] as $video){
            if(!isset($video['time_start']) || !isset($video['duration']) || !isset($video['pic_url']) || !isset($video['effect_type'])
                || !isset($font['content']) || !isset($font['font_type']) || !isset($font['font_size']) || !isset($font['font_bold'])
                || !isset($font['font_color']) || !isset($font['x']) || !isset($font['y'])){
                Log::getCrontabClient()->error("sbv:缺少video_font子参数");
                return false;
            }
            $localPic = $this->url_put_contents($jsonInfoArr['pic_url'], $this->picPath);
            $pic_shell = "ffmpeg -y -i ".$localPic." -s ".$jsonInfoArr['pix_type']." -vf gblur=sigma=50;steps=1 ".$localPic;
            $response = System::exec($shell);
            //差数据库
            if(!$response){
                $infoArr[] = [
                    'shell'     => $music_shell,
                    'is_success'=> 0
                ];
                return false;
            }

            $infoArr[] = [
                'shell'     => $music_shell,
                'is_success'=> 1
            ];

        }

//        //后面加
//        if($jsonInfoArr['display_type'] == 0){
//            //横板
//        }else{
//            //竖版
//        }





        //处理音频
        if($jsonInfoArr['music_url']){
            $localMusic = $this->url_put_contents($jsonInfoArr['music_url'], $this->audioPath);
            $music_shell = "ffmpeg -y -t -".$jsonInfoArr['total_time']." -i ".$localMusic." -ar ".$ffmpegConfig['sample_rate']." -ac ".$ffmpegConfig['channel']." -b:a ".$ffmpegConfig['audio_bite_rate']." ".$localMusic;
            $response = System::exec($shell);
            //差数据库
            if(!$response){
                $infoArr[] = [
                    'shell'     => $music_shell,
                    'is_success'=> 0
                ];
                return false;
            }

            $infoArr[] = [
                'shell'     => $music_shell,
                'is_success'=> 1
            ];
        }



//        //上传视频
//        $kongConfig = $this->config->get("pgsql.kong");
//        $kong = new Pgsql();
//        $client = $kong->getClient($kongConfig);
//        $result = $client->query('SELECT * FROM consumers');
//        $arr = $client->fetchAssoc($result);
//        $fileConfig = $this->config->get("file.s3");
//        $file = new File();
//        $res = $file->upload($fileConfig, BASE_PATH."/1.jpg", $arr['id']);
//        Db::beginTransaction();
//        try{
//            $data = [
//                'url' => $res['url'],
//                'consumer_id' => $arr['id']
//            ];
//            $fileModel = FileModel::create($data);
//
//            Db::commit();
//        } catch(\Throwable $ex){
//            //写入日志
//            Log::getClient()->error($ex->getMessage());exit;
//            Db::rollBack();
//        }
//        var_dump($fileModel->file_id);exit;


        //            if($shell){
//                $response = System::exec($shell);
//            }



        //        foreach ($response['hits']['hits'] as $k=>$v){
//
//            $response_data[] = [
//                "category" => $v['_source']['cate_name'],
//                "category_id" => $v['_source']['cate_id'],
//                'path' => $path
//            ];
//        }

        $end_time = time();

        $run_time = $end_time-$start_time;
        $is_success = 1;
        return [
//            'worker.cid' => $cid,
            // task_enable_coroutine=false 时返回 -1，反之 返回对应的协程 ID
            'task.cid' => Coroutine::id(),
        ];
    }


    /**
     * url写进文件
     * @param string $url
     * @param string $path
     * @return string
     */
    public function url_put_contents(string $url, string $path, string $file_name=""){
        $file = file_get_contents($url);
        if(!$file_name){
            $finalFileName = basename($url);
            $localFile = $path.$finalFileName;
        }else{
            $localFile = $path.$file_name;
        }
        file_put_contents($localFile, $file);
        return $localFile;
    }


    /**
     * 创建唯一目录
     * @param $path
     * @return bool|string
     */
    public function mk_dir($path){
        $path = $path.posix_getpid().Coroutine::id()."/";
        if(!file_exists($path)){
            mkdir($path);
        }else{
            return false;
        }
        return $path;
    }
}