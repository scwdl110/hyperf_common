<?php

namespace App\Model;

class DataArkModel extends AbstractPrestoModel
{
    public function __construct(string $dbhost, string $codeno)
    {
        parent::__construct($dbhost, $codeno);
    }
}
