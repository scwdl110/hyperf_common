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
namespace App\CrontabService;

use App\Model\CrontabModel;
use App\Model\BaseModel;
use Captainbi\Hyperf\Util\File;
use Hyperf\Contract\ConfigInterface;
use Hyperf\DbConnection\Db;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Utils\Coroutine;
use Swoole\Coroutine\System;
use Captainbi\Hyperf\Util\Log;

class SbvCrontabService{
    /**
     * 字体目录
     */
    protected $fontPath = BASE_PATH."/storage/font/";

    /**
     * sbv目录
     */
    protected $sbvPath = BASE_PATH."/storage/sbv/";
    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    /**
     * @param $id
     * @param $json_info
     * @param $success_type
     * @return bool
     */
    public function execute($id, $json_info, $success_type){
        //计算时间
        $start_time = time();
        //预防超时or断电
        $where = [
            ['id', "=", $id],
            ['is_success', '=', $success_type]
        ];
        $data = [
            'start_time' => $start_time
        ];
//        $data[BaseModel::UPDATED_AT] = $start_time;
        if($success_type == 0){
            $data['is_success'] = 1;
        }elseif($success_type == 3){
            //TODO
        }else{
            Log::getCrontabClient()->error("sbv:".$id."状态错误");
            return false;
        }
        $res = CrontabModel::query()->where($where)->update($data);
        if(!$res){
            Log::getCrontabClient()->error("sbv:".$id."任务在队列中");
            return false;
        }

        //失败预设值
        $data = [
            'is_success' => 3,
            'error_count'=> Db::raw("error_count+1"),
            'start_time' => 0,
        ];
        $where = [
            ['id', "=", $id]
        ];
        if(!$id || !$json_info){
            $res = CrontabModel::query()->where($where)->update($data);
            if(!$res){
                Log::getCrontabClient()->error("sbv:".$id."缺少参数任务失败");
                return false;
            }
            Log::getCrontabClient()->error("sbv:".$id."缺少参数");
            return false;
        }

        $jsonInfoArr = json_decode($json_info, true);
        if(!isset($jsonInfoArr['display_type']) || !isset($jsonInfoArr['total_time']) || !isset($jsonInfoArr['pix_type']) || !isset($jsonInfoArr['music_url']) || !isset($jsonInfoArr['video_font_list'])){
            $res = CrontabModel::query()->where($where)->update($data);
            if(!$res){
                Log::getCrontabClient()->error("sbv:".$id."缺少子参数任务失败");
                return false;
            }
            Log::getCrontabClient()->error("sbv:".$id."缺少子参数");
            return false;
        }
        //获取宽高
        $fileArr = explode("*", $jsonInfoArr['pix_type']);
        $height = end($fileArr);
        $width = prev($fileArr);

        //创建目录
        $sbv_path = $this->mkDir($this->sbvPath);
        if(!$sbv_path){
            $res = CrontabModel::query()->where($where)->update($data);
            if(!$res){
                Log::getCrontabClient()->error("sbv:".$id."文件目录冲突任务失败");
                return false;
            }
            Log::getCrontabClient()->error("sbv:".$id."文件目录冲突");
            return false;
        }

        //执行语句
        $infoArr = [];

        //ffmpeg配置
        $ffmpegConfig = $this->config->get("ffmpeg");

        //最终shell
        $finalShell = "ffmpeg -y";

        //视频
        $endIndex = count($jsonInfoArr['video_font_list'])-1;
        //临时流
        $tmpFlow = 'v0';
        //特效
        $transition = '';

        $videoCount = count($jsonInfoArr['video_font_list']);
        foreach ($jsonInfoArr['video_font_list'] as $picIndex=>$video){
            if(!isset($video['time_start']) || !isset($video['duration']) || !isset($video['pic_url']) || !isset($video['effect_type'])){
                //删除目录
                $this->delDir($sbv_path);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."缺少video_font子参数任务失败");
                    return false;
                }
                Log::getCrontabClient()->error("sbv:".$id."缺少video_font子参数");
                return false;
            }

            $localPic = $this->urlPutContents($video['pic_url'], $sbv_path, (string)$picIndex);
            if(!$localPic){
                //删除目录(里面已经log)
                $this->delDir($sbv_path);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."第一个urlPutContents任务失败");
                    return false;
                }
                return false;
            }
            $imageArr = getimagesize($localPic);
            if(!$imageArr){
                //删除目录
                $this->delDir($sbv_path);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."getimagesize任务失败");
                    return false;
                }
                Log::getCrontabClient()->error("sbv:".$id."获取图片失败");
                return false;
            }

            if($imageArr[0]>$imageArr[1]){
                $shell = "ffmpeg -y -i ".$localPic." -vf scale=".$width.":-1 ".$localPic;
            }else{
                $shell = "ffmpeg -y -i ".$localPic." -vf scale=-1:".$height." ".$localPic;
            }
            try{
                $response = System::exec($shell);
                if(!$response){
                    //删除目录
                    $this->delDir($sbv_path);
                    Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错");
                    $infoArr[] = [
                        'shell'     => $shell,
                        'is_success'=> 0
                    ];
                    $data['info'] = json_encode($infoArr);
                    $res = CrontabModel::query()->where($where)->update($data);
                    if(!$res){
                        Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错任务失败");
                        return false;
                    }
                    return false;
                }
            }catch (\Exception $e){
                //删除目录
                $this->delDir($sbv_path);
                Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错");
                $infoArr[] = [
                    'shell'     => $shell,
                    'is_success'=> 0
                ];
                $data['info'] = json_encode($infoArr);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错任务失败");
                    return false;
                }
                return false;
            }

            $infoArr[] = [
                'shell'     => $shell,
                'is_success'=> 1
            ];


            //背景图
            $backPic = $this->changFileName($localPic, $picIndex."back");

            $shell = "ffmpeg -y -i ".$localPic." -s ".$jsonInfoArr['pix_type']." -vf gblur=sigma=50:steps=1 ".$backPic;
            try{
                $response = System::exec($shell);
                //差数据库
                if(!$response){
                    //删除目录
                    $this->delDir($sbv_path);
                    Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错");
                    $infoArr[] = [
                        'shell'     => $shell,
                        'is_success'=> 0
                    ];
                    $data['info'] = json_encode($infoArr);
                    $res = CrontabModel::query()->where($where)->update($data);
                    if(!$res){
                        Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错任务失败");
                        return false;
                    }
                    return false;
                }
            }catch (\Exception $e){
                //删除目录
                $this->delDir($sbv_path);
                Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错");
                $infoArr[] = [
                    'shell'     => $shell,
                    'is_success'=> 0
                ];
                $data['info'] = json_encode($infoArr);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错任务失败");
                    return false;
                }
                return false;
            }

            $infoArr[] = [
                'shell'     => $shell,
                'is_success'=> 1
            ];

            //合并图片加字体
            if(isset($video['content']) && isset($video['font_type']) && isset($video['font_size']) && isset($video['font_bold']) &&
                isset($video['font_color']) && isset($video['x']) && isset($video['y'])){
                if($video['font_bold']){
                    $shell = "ffmpeg -y -i ".$backPic." -i ".$localPic." -filter_complex 'overlay=x=(W-w)/2:y=(H-h)/2,drawtext=fontfile=".
                        $this->fontPath.$video['font_type'].":text=".$video['content'].":y=".$video['y'].
                        ":x=".$jsonInfoArr['x'].":fontcolor=".$video['font_color'].":fontsize=".$video['font_size'].",drawtext=fontfile=".
                        $this->fontPath.$video['font_type'].":text=".$video['content'].":y=".$video['y'].
                        ":x=".($video['x']+1).":fontcolor=".$video['font_color'].":fontsize=".$video['font_size'].",setdar=dar=16/9' ".$localPic;
                }else{
                    $shell = "ffmpeg -y -i ".$backPic." -i ".$localPic." -filter_complex 'overlay=x=(W-w)/2:y=(H-h)/2,drawtext=fontfile=".
                        $this->fontPath.$video['font_type'].":text=".$video['content'].":y=".$video['y'].
                        ":x=".$video['x'].":fontcolor=".$video['font_color'].":fontsize=".$video['font_size'].",setdar=dar=16/9' ".$localPic;
                }
            }else{
                $shell = "ffmpeg -y -i ".$backPic." -i ".$localPic." -filter_complex 'overlay=x=(W-w)/2:y=(H-h)/2,setdar=dar=16/9' ".$localPic;
            }

            try{
                $response = System::exec($shell);

                //差数据库
                if(!$response){
                    //删除目录
                    $this->delDir($sbv_path);
                    Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错");

                    $infoArr[] = [
                        'shell'     => $shell,
                        'is_success'=> 0
                    ];
                    $data['info'] = json_encode($infoArr);
                    $res = CrontabModel::query()->where($where)->update($data);
                    if(!$res){
                        Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错任务失败");
                        return false;
                    }
                    return false;
                }
            }catch (\Exception $e){
                //删除目录
                $this->delDir($sbv_path);
                Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错");

                $infoArr[] = [
                    'shell'     => $shell,
                    'is_success'=> 0
                ];
                $data['info'] = json_encode($infoArr);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."的".$shell."出错任务失败");
                    return false;
                }
                return false;
            }


            $infoArr[] = [
                'shell'     => $shell,
                'is_success'=> 1
            ];

            //转场
            if($videoCount == 1){
                $finalShell .= " -loop 1 -t ".$jsonInfoArr['total_time']." -i ".$localPic;
            }else{
                if(!$video['effect_type']){
                    if($picIndex==0){
                        $finalShell .= " -loop 1 -t ".$video['duration']." -i ".$localPic;
                        $transition = "[0:v][1:v]concat=n=2:v=1[".$tmpFlow."];";
                    }elseif($picIndex != $endIndex){
                        $finalShell .= " -loop 1 -t ".$video['duration']." -i ".$localPic;
                        $transition .= "[".$tmpFlow."]";
                        $tmpFlow = "v".$picIndex;
                        $transition .= "[".($picIndex+1).":v]concat=n=2:v=1[".$tmpFlow."];";
                    }else{
                        //最后一个
                        $finalShell .= " -loop 1 -t ".$video['duration']." -i ".$localPic;
                    }
                }else{
                    if($picIndex==0){
                        $finalShell .= " -loop 1 -t 45 -i ".$localPic;
                    }else{
                        if($picIndex == 1 && $picIndex == $endIndex){
                            $finalShell .= " -loop 1 -t ".$video['duration']." -i ".$localPic;
                            $transition = "[0:v][1:v]xfade=transition=".$video['effect_type'].":duration=0:offset=".$video['time_start']."[".$tmpFlow."];";
                        }elseif($picIndex == 1){
                            $finalShell .= " -loop 1 -t 45 -i ".$localPic;
                            $transition = "[0:v][1:v]xfade=transition=".$video['effect_type'].":duration=0:offset=".$video['time_start']."[".$tmpFlow."];";
                        }else{
                            $transition .= "[".$tmpFlow."]";
                            $tmpFlow = "v".$picIndex;
                            $transition .= "[".($picIndex).":v]xfade=transition=".$video['effect_type'].":duration=0:offset=".$video['time_start']."[".$tmpFlow."];";
                        }
                    }
                }
            }

        }

        //音频索引
        $musicIndex = 0;

        //处理音频
        if($jsonInfoArr['music_url']){
            $localMusic = $this->urlPutContents($jsonInfoArr['music_url'], $sbv_path, (string)$musicIndex);
            if(!$localMusic){
                //删除目录(里面已经log)
                $this->delDir($sbv_path);
                return false;
            }
            $finalShell .= " -t ".$jsonInfoArr['total_time']." -i ".$localMusic." -ar ".$ffmpegConfig['sample_rate']." -ac ".$ffmpegConfig['channel']." -b:a ".$ffmpegConfig['audio_bite_rate'];
        }

        //视频最终生成
        $output = $sbv_path."output.mp4";
        $finalShell .= " -s ".$jsonInfoArr['pix_type']." -b:v ".$ffmpegConfig['video_bite_rate']." -bufsize ".$ffmpegConfig['video_bite_rate']." -c:v libx264 -pix_fmt yuv420p -filter_complex '".$transition."[".$tmpFlow."]fps=".$ffmpegConfig['fps']."' ".$output;

        try{
            $response = System::exec($finalShell);var_dump($response);exit;
            //差数据库
            if(!$response){
                //删除目录
                $this->delDir($sbv_path);
                Log::getCrontabClient()->error("sbv:".$id."的".$finalShell."出错");

                $infoArr[] = [
                    'shell'     => $finalShell,
                    'is_success'=> 0
                ];
                $data['info'] = json_encode($infoArr);
                $res = CrontabModel::query()->where($where)->update($data);
                if(!$res){
                    Log::getCrontabClient()->error("sbv:".$id."的".$finalShell."出错任务失败");
                    return false;
                }
                return false;
            }
        }catch (\Exception $e){var_dump(222);exit;
            //删除目录
            $this->delDir($sbv_path);
            Log::getCrontabClient()->error("sbv:".$id."的".$finalShell."出错");

            $infoArr[] = [
                'shell'     => $finalShell,
                'is_success'=> 0
            ];
            $data['info'] = json_encode($infoArr);
            $res = CrontabModel::query()->where($where)->update($data);
            if(!$res){
                Log::getCrontabClient()->error("sbv:".$id."的".$finalShell."出错任务失败");
                return false;
            }
            return false;
        }


        $infoArr[] = [
            'shell'     => $finalShell,
            'is_success'=> 1
        ];


        //上传视频
        $fileConfig = $this->config->get("file.s3");
        $file = new File();
        $res = $file->upload($fileConfig, $output, $id);
        if(!$res){
            //删除目录
            $this->delDir($sbv_path);
            Log::getCrontabClient()->error("sbv:".$id."上传失败");
            $res = CrontabModel::query()->where($where)->update($data);
            if(!$res){
                Log::getCrontabClient()->error("sbv:".$id."上传失败任务失败");
                return false;
            }
            return false;
        }


        //删除目录
        $size = filesize($output);
        $this->delDir($sbv_path);
        $end_time = time();
        $run_time = $end_time-$start_time;
        $data = [
            'is_success' => 2,
            'start_time' => $start_time,
            'end_time'   => $end_time,
            'run_time'   => $run_time,
            'info'       => $infoArr,
            'result'     => $res['url'],
            'size'       => $size,
        ];
        $where = [
            ['id', "=", $id]
        ];

        $data['info'] = json_encode($infoArr);
        $res = CrontabModel::query()->where($where)->update($data);
        if(!$res){
            Log::getCrontabClient()->error("sbv:".$id."的".$finalShell."出错任务失败");
            return false;
        }


        return true;
    }



    /**
     * url写进文件
     * @param string $url
     * @param string $path
     * @param string $file_name
     * @return string
     */
    public function urlPutContents(string $url, string $path, string $file_name){
        try{
            $file = file_get_contents($url);
            if($file_name === ''){
                $finalFileName = basename($url);
                $localFile = $path.$finalFileName;
            }else{
                $fileBaseName = basename($url);
                $fileArr = explode(".", $fileBaseName);
                $extension = end($fileArr);
                $localFile = $path.$file_name.".".$extension;
            }

            file_put_contents($localFile, $file);
        }catch (\Exception $e){
            Log::getCrontabClient()->error("sbv:".$e->getMessage());
            return false;
        }

        return $localFile;
    }


    /**
     * 创建唯一目录
     * @param $path
     * @return bool|string
     */
    public function mkDir($path){
        $path = $path.posix_getpid().Coroutine::id()."/";
        if(!file_exists($path)){
            mkdir($path);
        }else{
            return false;
        }
        return $path;
    }

    /**
     * 删除目录
     * @param $directory
     */
    function delDir($directory){//自定义函数递归的函数整个目录
        if(file_exists($directory)){//判断目录是否存在，如果不存在rmdir()函数会出错
            if($dir_handle=@opendir($directory)){//打开目录返回目录资源，并判断是否成功
                while($filename=readdir($dir_handle)){//遍历目录，读出目录中的文件或文件夹
                    if($filename!='.' && $filename!='..'){//一定要排除两个特殊的目录
                        $subFile=$directory.$filename;//将目录下的文件与当前目录相连
                        if(is_dir($subFile)){//如果是目录条件则成了
                            delDir($subFile);//递归调用自己删除子目录
                        }
                        if(is_file($subFile)){//如果是文件条件则成立
                            unlink($subFile);//直接删除这个文件
                        }
                    }
                }
                closedir($dir_handle);//关闭目录资源
                rmdir($directory);//删除空目录
            }
        }
    }

    /**
     * @param string $localPic
     * @param string $newName
     * @return string
     */
    public function changFileName(string $localPic, string $newName){
        $dirName = dirname($localPic);
        $fileName = basename($localPic);
        $fileArr = explode(".", $fileName);
        $extension = end($fileArr);
        $backPic = $dirName."/".$newName.".".$extension;
        return $backPic;
    }
}
