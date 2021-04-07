<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

/**
 * AES 加密类
 *
 * @link https://github.com/w7corp/easywechat/blob/29a2739a8cc6e614b240fef28db4aa5c4e208d02/src/Kernel/Support/AES.php
 */
class AES
{
    /**
     * @param string $text
     * @param string $key
     * @param string $iv
     * @param int    $option
     * @param bool   $wrapBase64 是否返回 base64 编码后的 密文
     *
     * @return string
     */
    public static function encrypt(
        string $text,
        string $key,
        string $iv,
        int $option = \OPENSSL_RAW_DATA,
        bool $wrapBase64 = true
    ): string {
        self::validateKey($key);
        self::validateIv($iv);

        $data = openssl_encrypt($text, self::getMode($key), $key, $option, $iv);
        return $wrapBase64 ? base64_encode($data) : $data;
    }

    /**
     * @param string      $cipherText
     * @param string      $key
     * @param string      $iv
     * @param int         $option
     * @param string|null $method
     * @param bool        $wrapBase64  $cipherText 是否 base64 编码后的密文
     *
     * @return string
     */
    public static function decrypt(
        string $cipherText,
        string $key,
        string $iv,
        int $option = \OPENSSL_RAW_DATA,
        ?string $method = null,
        bool $warpBase64 = true
    ): string {
        self::validateKey($key);
        self::validateIv($iv);

        return $wrapBase64
            ? openssl_decrypt(base64_decode($cipherText), $method ?: self::getMode($key), $key, $option, $iv)
            : openssl_decrypt($cipherText, $method ?: self::getMode($key), $key, $option, $iv);
    }

    /**
     * @param string $key
     *
     * @return string
     */
    public static function getMode($key)
    {
        return 'aes-' . (8 * strlen($key)) . '-cbc';
    }

    /**
     * @param string $key
     */
    public static function validateKey(string $key)
    {
        if (!in_array(strlen($key), [16, 24, 32], true)) {
            throw new \InvalidArgumentException(
                sprintf('Key length must be 16, 24, or 32 bytes; got key len (%s).', strlen($key))
            );
        }
    }

    /**
     * @param string $iv
     *
     * @throws \InvalidArgumentException
     */
    public static function validateIv(string $iv)
    {
        if (!empty($iv) && 16 !== strlen($iv)) {
            throw new \InvalidArgumentException('IV length must be 16 bytes.');
        }
    }
}
