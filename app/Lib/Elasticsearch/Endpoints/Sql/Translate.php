<?php

namespace App\Lib\Elasticsearch\Endpoints\Sql;

class Translate extends \Elasticsearch\Endpoints\Sql\Translate
{
    use UriTrait;

    public function getURI(): string
    {
        return $this->uri . '/translate';
    }
}
