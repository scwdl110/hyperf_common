<?php

namespace Captainbi\Hyperf\Util\Encryption\Contracts;

interface EncrypterInterface
{
    /**
     * Encrypt the given value.
     *
     * @param  mixed  $value
     * @param  bool  $serialize
     * @return string
     *
     * @throws \Captainbi\Hyperf\Util\Encryption\Exception\EncryptException
     */
    public function encrypt($value, $serialize = true);

    /**
     * Decrypt the given value.
     *
     * @param  string  $payload
     * @param  bool  $unserialize
     * @return mixed
     *
     * @throws \Captainbi\Hyperf\Util\Encryption\Exception\DecryptException
     */
    public function decrypt($payload, $unserialize = true);
}
