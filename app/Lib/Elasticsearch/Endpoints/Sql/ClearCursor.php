<?php

namespace App\Lib\Elasticsearch\Endpoints\Sql;

class ClearCursor extends \Elasticsearch\Endpoints\Sql\ClearCursor
{
    use UriTrait;

    public function getURI(): string
    {
        return $this->uri . '/close';
    }
}
