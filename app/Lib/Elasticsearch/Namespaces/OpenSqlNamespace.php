<?php

namespace App\Lib\Elasticsearch\Namespaces;

use Elasticsearch\Transport;
use Elasticsearch\Namespaces\SqlNamespace;

class OpenSqlNamespace extends SqlNamespace
{
    public function __construct(Transport $transport, string $uri)
    {
        $this->transport = $transport;
        $this->endpoints = function($class) use ($uri) {
            $fullPath = '\\App\\Lib\\Elasticsearch\\Endpoints\\' . $class;

            return new $fullPath($uri);
        };
    }

    public function query(array $params = [])
    {
        if (!isset($params['format'])) {
            $params['format'] = 'json';
        }

        return parent::query($params);
    }
}
