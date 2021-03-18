<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;


class Gd {
    /**
     * gd库图片增加字体
     * @param string $pic
     * @param string $size
     * @param string $font
     * @param string $text
     * @return string
     */
    public static function pic_add_font(string $pic, string $size, string $font, string $text){
        // 加载已有图像
        $img = imagecreatefromjpeg($pic);
        //给图片分配颜色
        // imagecolorallocate($img, 0xff, 0xcc, 0xcc);
        //设置字体颜色
        $black = imagecolorallocate($img, 255, 255, 255);
        //将ttf文字写到图片中
        imagettftext($img, $size, 0, 180, 176, $black, $font, $text);
        imagettftext($img, $size, 0, 180, 216, $black, $font, $text);
        //保存图片至指定路径
        imagePNG($img, $pic);
        imagedestroy($img);
        return $pic;
    }

}