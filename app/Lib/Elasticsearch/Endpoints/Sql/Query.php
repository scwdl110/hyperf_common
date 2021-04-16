<?php

namespace App\Lib\Elasticsearch\Endpoints\Sql;

class Query extends \Elasticsearch\Endpoints\Sql\Query
{
    use UriTrait;

    public function getURI(): string
    {
        return $this->uri;
    }
}
