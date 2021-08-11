<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


use Aws\Exception\MultipartUploadException;
use Aws\S3\MultipartUploader;
use Aws\S3\ObjectUploader;
use Aws\S3\S3Client;
use Hyperf\Utils\ApplicationContext;

class File {
    public function __construct()
    {
        $loggerFactory = ApplicationContext::getContainer()->get("Hyperf\Logger\LoggerFactory");
        $this->logger = $loggerFactory->get('log', 'default');
    }

    public function upload($fileConfig, $file, $fileAttach=''){
        if(!$file || !$fileConfig || !isset($fileConfig['region']) || !isset($fileConfig['bucket']) || !isset($fileConfig['key']) || !isset($fileConfig['secret'])){
            $this->logger->error('file无参数');
            return false;
        }
        $s3Client = new S3Client([
            'version' => 'latest',
            'region'  => $fileConfig['region'],
            'credentials' => [
                'key'    => $fileConfig['key'],
                'secret' => $fileConfig['secret'],
            ]
        ]);

        $source = fopen($file, 'rb');
        $finalFileName = File::getFinalFileName($file, $fileAttach);

        $uploader = new ObjectUploader(
            $s3Client,
            $fileConfig['bucket'],
            $finalFileName,
            $source
        );

        do {
            try {
                $result = $uploader->upload();
                if ($result["@metadata"]["statusCode"] != '200' && (!isset($result["ObjectURL"]) || !$result["ObjectURL"])) {
                    $this->logger->error('提交s3失败');
                    return false;
                }
            } catch (MultipartUploadException $e) {
                rewind($source);
                $uploader = new MultipartUploader($s3Client, $source, [
                    'state' => $e->getState(),
                ]);

            }
        } while (!isset($result));
        fclose($source);
        return [
            'file_id' => 1,
            'url'     => $result["ObjectURL"]
        ];
    }

    public function download(){

    }


    /**
     * 通过url或者本地文件得到输出文件名
     * @param $fileUrl
     * @param $fileAttach
     * @return string
     */
    public static function getFinalFileName($fileUrl, $fileAttach){
        $folder = date("Ymd");
        $fileBaseName = basename($fileUrl);
        $fileArr = explode(".", $fileBaseName);
        $extension = end($fileArr);
        $fileName = prev($fileArr);
        $md5FileName = md5($fileAttach.$fileName);
        $finalFileName = $folder."/".$md5FileName.".".$extension;
        return $finalFileName;
    }

}