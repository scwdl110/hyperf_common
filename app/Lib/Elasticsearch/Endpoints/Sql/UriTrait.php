<?php

namespace App\Lib\Elasticsearch\Endpoints\Sql;

trait UriTrait
{
    protected $uri = '/_sql';

    public function __construct(string $uri)
    {
        $this->uri = '/' . ltrim($uri, '/');
    }
}
