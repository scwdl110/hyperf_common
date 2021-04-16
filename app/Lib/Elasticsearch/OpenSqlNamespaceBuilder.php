<?php

namespace App\Lib\Elasticsearch;

use Elasticsearch\Transport;
use Elasticsearch\Serializers\SerializerInterface;
use Elasticsearch\Namespaces\NamespaceBuilderInterface;
use App\Lib\Elasticsearch\Namespaces\OpenSqlNamespace;

class OpenSqlNamespaceBuilder implements NamespaceBuilderInterface
{
    protected $uri = '/_sql';

    public function __construct(string $uri)
    {
        $this->uri = $uri;
    }

    public function getName(): string
    {
        return 'opensql';
    }

    public function getObject(Transport $transport, SerializerInterface $serializer)
    {
        return new OpenSqlNamespace($transport, $this->uri);
    }
}
