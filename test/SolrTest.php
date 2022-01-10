<?php

namespace HyperfTest;

use Captainbi\Hyperf\Util\Solr;
use PHPUnit\Framework\TestCase;

class SolrTest extends TestCase
{
    public function testQuery()
    {
        $logger = new class extends \Psr\Log\AbstractLogger {
            public function log($level, $message, array $context = array())
            {
                echo sprintf(
                    "%s %s [%s]\r\n",
                    date('Y-m-d H:i:s'),
                    $message, json_encode($context,  JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE
                ));
            }
        };

        $solr = new Solr([
            'host' => '172.15.26.159',
            'port' => 8484,
            'path' => 'solr/members',
            'timeout' => 5,
            'retry' => 0,
        ], $logger);

        $resp = $solr->query('*:*');
        $this->assertTrue(is_array($resp));
        $this->assertTrue(isset($resp['total'], $resp['list'], $resp['facet'], $resp['stats']));
    }
}
