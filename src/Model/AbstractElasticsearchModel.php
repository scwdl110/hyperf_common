<?php

namespace Captainbi\Hyperf\Model;

use RuntimeException;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;

use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\ConfigInterface;

use Captainbi\Hyperf\Util\Elasticsearch;

abstract class AbstractElasticsearchModel
{
    const WHITESPACE_SPLIT_PATTERN = '/[\x9-\xd\x1c-\x20\x{1680}\x{2000}-\x{2006}\x{2008}-\x{200a}\x{2028}\x{2029}\x{205f}\x{3000}]/u';

    const LUCENE_WHITESPACE_CHARS = "\u{9}\u{a}\u{b}\u{c}\u{d}\u{1c}\u{1d}\u{1e}\u{1f}\u{20}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2008}\u{2009}\u{200a}\u{2028}\u{2029}\u{205f}\u{3000}";

    const LUCENE_QUERY_ESCAPE_CHARS = [
        '+' => '\\\\+',
        '-' => '\\\\-',
        '*' => '\\\\*',
        '^' => '\\\\^',
        ':' => '\\\\:',
        '(' => '\\\\(',
        ')' => '\\\\)',
        '[' => '\\\\[',
        ']' => '\\\\]',
        '{' => '\\\\{',
        '}' => '\\\\}',
        '/' => '\\\\/',
        '"' => '\\"',
        "'" => "\\'",
        '\\' => '\\\\',
    ];

    // 前缀模糊查询
    const LIKE_PREFIX = 1;

    // 后缀模糊查询
    const LIKE_SUFFIX = 2;

    // @var ?\Captainbi\Hyperf\Util\Elasticsearch
    protected $esClient = null;

    // @var ?\Psr\SimpleCache\CacheInterface
    protected $cache = null;

    // @var ?\Psr\Log\LoggerInterface
    protected $logger = null;

    // @var string
    protected $lastSql = '';

    // @var int
    protected $defaultCacheTime = 1800;

    // @var string
    protected $indexName = '';

    protected $searchTimeout = '30s';

    public function __construct(
        string $cluster,
        string $dbhost = '001',
        int $defaultCacheTime = 1800,
        ?CacheInterface $cache = null,
        ?LoggerInterface $logger = null,
        ?int $searchTimeout = 30
    ) {
        $container = ApplicationContext::getContainer();
        $this->cache = $cache ?? $container->get(CacheInterface::class);
        $this->logger = $logger ?? $container->get(LoggerFactory::class)->get('elasticsearch', 'default');

        $config = $container->get(ConfigInterface::class)->get('elasticsearch', []);
        if (empty($config) || !is_array($config) || !isset($config[$cluster][$dbhost])
            || !is_array($config[$cluster][$dbhost])
        ) {
            $this->logger->error('elasticsearch 配置信息不存在', [$config, $cluster, $dbhost]);
            throw new RuntimeException('Missing Elasticsearch config');
        }

        $config = $config[$cluster][$dbhost];
        if (!isset($config['write_host'], $config['tableName'])
            || !is_array($config['write_host']) || !is_string($config['tableName'])
        ) {
            $this->logger->error('elasticsearch 配置信息错误', [$config]);
            throw new RuntimeException('Elasticsearch config invalid');
        }

        $hosts = [];
        foreach ($config['write_host'] as $host) {
            if (is_string($host) && false !== ($url = parse_url($host))) {
                $hosts[] = [
                    'host' => $url['host'],
                    'port' => intval($url['port'] ?? $config['port'] ?? 9200),
                    'user' => trim($url['user'] ?? $config['user'] ?? ''),
                    'pass' => trim($url['pass'] ?? $config['password'] ?? ''),
                    'scheme' => $url['scheme'] ?? 'http',
                ];
            }
        }

        if (empty($hosts)) {
            $this->logger->error('elasticsearch 配置不能为空');
            throw new RuntimeException('Elasticsearch config is required');
        }

        $this->indexName = $config['tableName'];
        $this->defaultCacheTime = $defaultCacheTime;
        $this->esClient = new Elasticsearch(array_merge($config, ['hosts' => $hosts]), $logger);
        if ($searchTimeout && $searchTimeout > 0) {
            $this->searchTimeout = "{$searchTimeout}s";
        }
    }

    protected function getRealField(string $field): string
    {
        return $field;
    }

    public function getLastSql()
    {
        return $this->lastSql;
    }

    public function select(
        $where = '',
        string $column = '*',
        string $table = '',
        string $limit = '',
        string $order = '',
        string $group = '',
        array $match = [],
        array $or_match = [],
        bool $isCache = true,
        ?int $cacheTime = null,
        callable $parseResult = null
    ) {
        if (!$this->setLastSql($where, $column, $table, $limit, $order, $group, $match, $or_match)) {
            return false;
        }

        $cacheKey = $this->getCacheKey();
        $cacheTime = $cacheTime ?? $this->defaultCacheTime;
        if ($isCache) {
            $cacheData = $this->cache->get($cacheKey);
            if (!empty($cacheData)) {
                return $cacheData;
            }
        } else {
            $this->cache->delete($cacheKey);
        }

        $resp = $this->esClient->awsSql()->query([
            'format' => 'json',
            'query' => $this->lastSql,
            'request_timeout' => $this->searchTimeout,
        ]);

        $result = false;
        if (isset($resp['_shards']['successful']) && $resp['_shards']['successful'] > 0) {
            if (is_callable($parseResult)) {
                $result = $parseResult($resp);
            } else {
                if (!empty($group)) {
                    $aggregations = array_values($resp['aggregations']);
                    $result = $aggregations[0]['buckets'];
                } else {
                    $result = [];
                    foreach ($resp['hits']['hits'] as $hits) {
                        $result[] = $hits['_source'];
                    }
                }
            }

            if ($isCache) {
                $this->cache->set($cacheKey, $result, $cacheTime);
            }
        }

        return $result;
    }

    public function count(
        $where = '',
        array $match = [],
        string $table = '',
        array $or_match = [],
        bool $isCache = true,
        ?int $cacheTime = null
    ) {
        return $this->select(
            $where,
            'count(*)',
            $table,
            '',
            '',
            '',
            $match,
            $or_match,
            $isCache,
            $cacheTime,
            function($resp) {
                $aggregations = array_values($resp['aggregations']);
                return $aggregations[0]['value'];
            }
        );
    }

    public function unionQuery(
        $where = '',
        string $column = '',
        array $match = [],
        string $table = '',
        string $group = '',
        bool $isCache = true,
        ?int $cacheTime = null
    ) {
        return $this->select(
            $where,
            $column,
            $table,
            '',
            '',
            $group,
            $match,
            [],
            $isCache,
            $cacheTime,
            function($resp) {
                return $resp['aggregations'];
            }
        );
    }

    public function union_query(
        $where = '',
        string $data = '',
        array $match = [],
        string $table = '',
        string $group = '',
        bool $isCache = true,
        ?int $cacheTime = null
    ) {
        return $this->unionQuery($where, $data, $match, $table, $group, $isCache, $cacheTime);
    }

    public function update($id, array $data, bool $refresh = false)
    {
        return empty($id) || empty($data) ? false : $this->esClient->update([
            'id' => $id,
            'type' => '_doc',
            'refresh' => $refresh,
            'index' => $this->indexName,
            'body' => [
                'doc' => $this->replaceUnixTimestamp($data),
            ],
        ]);
    }

    public function batchUpdate(array $ids, array $datas, bool $refresh = false)
    {
        $body = [];
        $datas = $this->replaceUnixTimestamp($datas) ;
        if (count($datas) == count($datas, 1)) {
            // 一维数组，多个id 同时更新相同数据
            foreach ($ids as $id) {
                $body[] = ['update' => ['_id' => $id]];
                $body[] = ['doc' => $datas];
            }
        } else {
            if (count($ids) !== count($datas)) {
                return false;
            } else {
                foreach ($ids as $k => $id) {
                    $body[] = ['update' => ['_id' => $id]];
                    $body[] = ['doc' => $datas[$k]];
                }
            }
        }

        if (empty($body)) {
            return false;
        }

        return $this->esClient->bulk([
            'index' => $this->indexName,
            'type' => '_doc',
            'body' => $body,
            'refresh' => $refresh,
        ]);
    }

    public function addOne(array $data, $id = null, bool $refresh = false)
    {
        if (!empty($data)) {
            $params = [
                'refresh' => $refresh,
                'index' => $this->indexName,
                'type' => '_doc',
                'body' => $this->replaceUnixTimestamp($data),
            ];

            if (!empty($id)) {
                $params['id'] = $id ;
            } elseif (!empty($data['id'])) {
                $params['id'] = $data['id'];
            }

            return $this->esClient->index($params);
        }

        return false;
    }

    public function addAll(array $datas, array $ids = [], bool $refresh = false)
    {
        if (!empty($datas)) {
            $params = [
                'index' => $this->indexName,
                'type' => '_doc',
                'refresh' => $refresh,
            ];
            $datas = $this->replaceUnixTimestamp($datas) ;

            if (count($datas) === count($datas, 1)) {
                // 一维数组，多个id 同插入相同数据
                foreach ($ids as $id) {
                    $params['body'][]['index'] = ['_id' => $id];
                    $params['body'][] = $datas;
                }
            } elseif (count($datas) === count($ids)) {
                if (empty($ids)) {
                    $ids = array_column($datas, 'id');
                    if (!empty($ids) && count($datas) !== count($ids)) {
                        return false;
                    }
                }

                foreach ($datas as $k => $data) {
                    $index = [];
                    if (!empty($ids[$k])) {
                        $index['_id'] = $ids[$k];
                    }

                    $params['body'][]['index'] = $index;
                    $params['body'][] = $data;
                }
            } else {
                return false;
            }

            return $this->esClient->bulk($params);
        }

        return false;
    }

    public function search(array $query)
    {
        return $this->esClient->search([
            '_index' => $this->indexName,
            '_type' => '_doc',
            'query' => $query,
            'timeout' => $this->searchTimeout,
        ]);
    }

    public function deleteOne($id, bool $refresh = false)
    {
        return empty($id) ? false : $this->esClient->delete([
            'id' => $id,
            'type' => '_doc',
            'refresh' => $refresh,
            'index' => $this->indexName,
        ]);
    }

    public function batchDetele(array $ids, bool $refresh = false)
    {
        if (!empty($ids)) {
            $params = [
                'index' => $this->indexName,
                'type' => '_doc',
                'refresh' => $refresh,
            ];

            foreach ($ids as $id) {
                $params['body'][]['delete'] = ['_id' => $id];
            }

            return $this->esClient->bulk($params);
        }

        return false;
    }

    protected function getCacheKey(): string
    {
        return 'ES_DATAS_' . md5($this->getLastSql());
    }

    protected function setLastSql(
        $where = '',
        string $column = '*',
        string $table = '',
        string $limit = '',
        string $order = '',
        string $group = '',
        array $match = [],
        array $or_match = []
    ): bool {
        if (is_array($where)) {
            $where = $this->getWhereSql($where);
        } elseif (!is_string($where)) {
            return false;
        }

        $where = $this->replaceWhereLike($where);

        if (!empty($match)) {
            $match_sql = $this->getMatchSql($match);
            if ($match_sql) {
                if ($where) {
                    $where = $match_sql;
                } else {
                    $where .= " AND {$match_sql}";
                }
            }
        }

        if (!empty($or_match)) {
            $or_match_sql = $this->getOrMatchSql($or_match);
            if ($or_match_sql) {
                if ($where) {
                    $where = $or_match_sql;
                } else {
                    $where .= " AND {$or_match_sql}";
                }
            }
        }

        if (empty($table)) {
            $table = $this->indexName;
        }

        $column = $this->replaceGroupByFieldInSelectMax($group, $column);
        $where = $where === '' ? '' : " WHERE {$where}";
        $order = $order === '' ? '' : " ORDER BY {$order}";
        $group = $group === '' ? '' : " GROUP BY {$group}";
        $limit = $limit === '' ? '' : " LIMIT {$limit}";

        $this->lastSql = "SELECT {$column} FROM {$table}{$where}{$group}{$order}{$limit}";
        return true;
    }

    protected function getWhereSql(array $where): string
    {
        $sql = '';
        foreach ($where as $key => $val) {
            if ($key && is_string($key) && is_string($val)) {
                $sql .= " AND {$key} = '{$val}' ";
            }
        }

        return $sql ? substr($sql, 4) : '';
    }

    protected function replaceWhereLike(string $where): string
    {
        if (false === stripos($where, ' like ')) {
            return $where;
        }

        $quote = '"';
        $replace = function($match) use (&$quote) {
            list($full, $field, $keyword) = $match;
            $field = $this->getRealField($field);
            // 如果 like 的字段包含 .keyword，则不做 query 转换，继续使用 like 查询
            $fullText = '.keyword' !== mb_substr($field, -8);

            // 如果中间没有包含 % 号，直接返回全匹配结果
            if (!$fullText && false === strpos(trim($keyword, '% '), '%')) {
                return $full;
            }

            $first = $last = '';
            $keyword = trim($keyword);
            $pos = self::wildcardPosition($keyword);

            if ($pos & self::LIKE_SUFFIX) {
                $first = '%';
                $keyword = mb_substr($keyword, 1);
            }

            if ($pos & self::LIKE_PREFIX) {
                $last = '%';
                $keyword = mb_substr($keyword, 0, -1);
            }

            if ($fullText) {
                return $this->fullTextQuery([$field => $keyword], true, $pos);
            }

            // 最多允许出现100个字符，多字节文字算一个字符
            if (mb_strlen($keyword) > 100) {
                // 有截取字符串就需要在后面加通配符
                $last = '%';
                $keyword = mb_substr($keyword, 0, 100);
            }

            // v.a. https://stackoverflow.com/a/11819111/831243
            return "{$field} LIKE {$quote}{$first}" . preg_replace_callback('/(?<!\\\\)(?:\\\\\\\\)*%/', function ($m) {
                return addcslashes($m[0], '%');
            }, $keyword) . "{$last}{$quote}";
        };

        if (false !== strpos($where, '"')) {
            $quote = '"';
            // 替换 like "" 语句
            // v.a. https://github.com/consatan/sqlbuilder/blob/dd41d597ff3ae0bace5e6b5451146bd661830059/src/Builder.php#L39
            $where = preg_replace_callback('/([a-z0-9_\.]+)\s+like\s+"([^\\"]*(?:\\.[^\\"]*)*)"/is', $replace, $where);
        }

        if (false !== strpos($where, "'")) {
            $quote = "'";
            // 替换 like '' 语句
            // v.a. https://github.com/consatan/sqlbuilder/blob/dd41d597ff3ae0bace5e6b5451146bd661830059/src/Builder.php#L42
            $where = preg_replace_callback('/([a-z0-9_\.]+)\s+like\s+\'([^\\\\\']*(?:\\\\.[^\\\\\']*)*)\'/is', $replace, $where);
        }

        return $where;
    }

    protected function getMatchSql(array $match, bool $force = false, bool $andCond = true): string
    {
        if (!$force) {
            return $this->fullTextQuery($match, true);
        }

        if (!empty($match)) {
            $sql = '';
            $cond = $andCond ? 'AND' : 'OR';
            foreach ($match as $k => $val) {
                $val = str_replace(
                    ["'", '"', '*', '-', '#', '&', '(', ')', '（', '）', '+', '/', '.', '×', '�', ',', '%'],
                    [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',  ' ',  ' ', ' ', ' ', ' ', ' ', ' ', ' '],
                    trim($val)
                );

                if (!empty($val)) {
                    foreach (explode(' ' , $val) as $v) {
                        // 限制单个 like 查询不得超过100个字符
                        $v = mb_substr(trim($v), 0, 100);
                        if ($v != '') {
                            $sql .= " {$cond} {$k} LIKE '%" . strtolower($v) . "%'";
                        }
                    }
                }
            }

            return $sql ? '(' . substr($sql, $andCond ? 4 : 3) . ')' : '';
        }

        return '';
    }

    protected function getOrMatchSql(array $match, bool $force = false): string
    {
        return $this->getMatchSql($match, $force, false);
    }

    protected function replaceGroupByFieldInSelectMax(string $group, string $column): string
    {
        if (1 === preg_match('/^[^\(]/', trim($group))) {
            $column = preg_replace('/(\s*,\s*)?(max|min|sum|avg|count)\s*\(\s*' . trim($group) . '\s*\)/i', '$1', $column);
        }

        return $column;
    }

    protected function replaceUnixTimestamp(array $datas)
    {
        if (!empty($datas)) {
            if (count($datas) === count($datas, 1)) {
                // 一维数组，多个id 同时更新相同数据
                foreach ($datas as $k => $data) {
                    if (is_string($data) && strtoupper($data) === 'UNIX_TIMESTAMP()') {
                        $datas[$k] = time();
                    }
                }
            } else {
                foreach ($datas as $k1 => $data) {
                    foreach ($data as $k2 => $da) {
                        if (is_string($da) && strtoupper($da) === 'UNIX_TIMESTAMP()') {
                            $datas[$k1][$k2] = time();
                        }
                    }
                }
            }
        }

        return $datas ;
    }

    /**
     * 获取字符串通配符(%)位置
     *
     * @param string $str
     * @return int
     *   0: 没有通配符
     *   1: 通配符在后 (self::LIKE_PREFIX)
     *   2: 通配符在前 (self::LIKE_SUFFIX)
     *   3: 前后都有通配符 (self::LIKE_PREFIX | self::LIKE_SUFFIX)
     */
    final public static function wildcardPosition($str)
    {
        $pos = 0;
        if ('%' === mb_substr($str, 0, 1)) {
            $pos |= self::LIKE_SUFFIX;
        }

        if ('%' === mb_substr($str, -1)) {
            // 最后2个字符是 \% 的话，要考虑是否转义字符
            // 比如 \% 则转义 % 符号
            // 如果是 \\% 则不转义
            // 但如果是 \\\% 则又是转义字符，所以要知道前面有几个连续的 \ 符号
            // 单数为转义 % 符号，双数不转义
            $escape = 0;
            for ($i = mb_strlen($str) - 2; $i >= 0; $i--) {
                if ('\\' === mb_substr($str, $i, 1)) {
                    $escape++;
                } else {
                    break;
                }
            }

            if (0 === $escape % 2) {
                $pos |= self::LIKE_PREFIX;
            }
        }

        return $pos;
    }

    protected function fullTextQuery($match, $andCond = true, $wildcardPos = 3)
    {
        if (empty($match)) {
            return '';
        }

        $sql = [];
        foreach ($match as $field => $query) {
            $field = $this->getRealField($field);
            // .keyword 字段继续使用 wildcard 查询
            if ('.keyword' === mb_substr($field, -8)) {
                $item = [$field => $query];
                $sql[] = (bool)$andCond ? $this->getMatchSql($item, true) : $this->getOrMatchSql($item, true);
                continue;
            }

            $query = trim($query, self::LUCENE_WHITESPACE_CHARS);
            if ($query) {
                // 转义后根据 lucene 空格分词器规则进行分词
                if (false !== strpos($query, '\\"')) {
                    $query = strtr($query, ['\\"' => '"']);
                }

                if (false !== strpos($query, "\\'")) {
                    $query = strtr($query, ["\\'" => "'"]);
                }

                $chunk = preg_split(self::WHITESPACE_SPLIT_PATTERN, strtr($query, self::LUCENE_QUERY_ESCAPE_CHARS));
                if ($wildcardPos & self::LIKE_SUFFIX) {
                    $chunk[0] = "*{$chunk[0]}";
                }

                if ($wildcardPos & self::LIKE_PREFIX) {
                    // 只有一个词的，做模糊搜索
                    // 和原本的 es like 查询一致，但因为字段的分词器用的是 空格分词器 而不是标准的分词器
                    // 不会对一些字符做过滤，搜索结果更精确
                    //
                    // 多于一个词的，第一个词做后缀查询(wildcard)，最后一个词做前缀查询(wildcard)
                    // 中间的词（如果有的话）做 term 查询
                    // 根据 mysql like 规则，搜索 "%abc def xyz%" 的情况下
                    // 符合上述查询结果，但如果搜索 "%abc  def xyz%" 则结果可能会更多
                    // 因为中间不管几个空格都不影响分词结果，但至少不会出现数据丢失问题
                    //
                    // 该查询方法相比于之前用 es 的多个 like(实际为 wildcard) 查询
                    // 只有第一个词做后缀查询(wildcard)，速度上会快得多
                    $chunk[sizeof($chunk) - 1] .= '*';
                }

                foreach ($chunk as &$val) {
                    $val = "{$field}:{$val}";
                }
                unset($val);

                $sql[] = "QUERY('" . implode(' AND ', $chunk) . "')";
            }
        }

        return $sql ? ('(' . implode((bool)$andCond ? ' AND ' : ' OR ', $sql) . ')') : '';
    }
}
