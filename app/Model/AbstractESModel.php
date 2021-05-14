<?php

namespace App\Model;

use RuntimeException;

use Hyperf\Utils\ApplicationContext;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Contract\ConfigInterface;
use App\Lib\Elasticsearch;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;

abstract class AbstractESModel implements BIModelInterface
{
    protected $lastSql = '';

    protected $dbhost = '001';

    protected $codeno = '001';

    protected $logger = null;

    protected $esClient = null;

    protected $cache = null;

    protected $table = '';

    protected $dryRun = false;

    public function __construct(
        string $dbhost = '',
        string $codeno = '',
        ?LoggerInterface $logger = null,
        ?callable $handler = null,
        ?LoggerInterface $tracer = null
    ) {
        $container = ApplicationContext::getContainer();
        if (null === $logger) {
            $logger = $container->get(LoggerFactory::class)->get('elasticsearch', 'default');
        }
        $this->logger = $logger;

        if ('' === $dbhost) {
            $userInfo = \app\getUserInfo();
            $dbhost = $userInfo['dbhost'] ?? '';
            $codeno = $userInfo['codeno'] ?? '';
        }

        if (!is_numeric($dbhost) || !is_numeric($codeno)) {
            $this->logger->error('错误的 es dbhost 或 codeno', [$dbhost, $codeno]);
            throw new RuntimeException('Invalid dbhost or codeno.');
        }
        $this->dbhost = trim($dbhost);
        $this->codeno = trim($codeno);

        $config = $container->get(ConfigInterface::class)->get('elasticsearch', []);
        if (empty($config)) {
            $this->glogger->error('es 配置信息不存在');
            throw new RuntimeException('Missing Elasticsearch config.');
        }

        $config = $config[$this->dbhost] ?? [];
        if (empty($config)) {
            $this->glogger->error('es 数据库配置不存在', [$config]);
            throw new RuntimeException('Missing Elasticsearch connection config.');
        }

        if ($this->table && strlen($this->table) - 1 === strrpos($this->table, '_')) {
            $this->table = $this->table . $this->dbhost;
        }

        $this->esClient = Elasticsearch::getConnection($config, $this->logger, $handler, $tracer);
    }

    protected function getCache(): CacheInterface
    {
        if (null === $this->cache) {
            $this->cache = ApplicationContext::getContainer()->get(CacheInterface::class);
        }

        return $this->cache;
    }

    protected function parseSql(
        $where = '',
        string $data = '*',
        string $table = '',
        $limit = '',
        string $order = '',
        string $group = '',
        array $match = [],
        array $orMatch = []
    ) {
        $where = is_array($where) ? $this->sqls($where) : $where;
        $table = $table !== '' ? $table : $this->table;
        $matchSql = $orMatchSql = '';

        if ($match) {
            $matchSql = $this->getMatchSql($match);
        }

        if ($orMatch) {
            $orMatchSql = $this->getMatchSql($orMatch, false);
        }

        if ($matchSql) {
            $where = $where ? " AND {$matchSql}" : $matchSql;
        }

        if ($orMatchSql) {
            $where = $where ? " AND {$orMatchSql}" : $orMatchSql;
        }

        $data = $this->splitAggrField($this->replaceGroupByFieldInSelectMax($group, $data));
        $where = empty($where) ? '' : " WHERE {$where}";
        $order = empty($order) ? '' : " ORDER BY {$order}";
        $group = empty($group) ? '' : " GROUP BY {$group}";
        $limit = is_string($limit) || is_numeric($limit) ? trim((string)$limit) : '';

        if (!empty($limit)) {
            // 兼容 $limit = '1', '1, 2', 'limit 1,2', 'limit 1 offset 2', 'offset 1 limit 2' 等形式
            if (false !== strpos($limit, ',')) {
                list($offset, $limit) = explode(',', $limit, 2);
                if (1 === preg_match('/\s*limit\s+(\d+)/i', $offset, $m)) {
                    $offset = $m[1];
                }

                // presto 语法必须 offset 在前，且不支持 limit 1,2 这种写法
                $limit = " OFFSET {$offset} LIMIT {$limit}";
            } else {
                if (is_numeric($limit)) {
                    $limit = " LIMIT {$limit}";
                } elseif (1 === preg_match('/\s*offset\s+(\d+)\s+limit\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[1]} LIMIT {$m[2]}";
                } elseif (1 === preg_match('/\s*limit\s+(\d+)\s+offset\s+(\d+)\s*/i', $limit, $m)) {
                    $limit = " OFFSET {$m[2]} LIMIT {$m[1]}";
                }
            }
        }

        return "SELECT {$data} FROM {$table} {$where} {$group} {$order} {$limit}";
    }

    public function select(
        $where = '',
        string $data = '*',
        string $table = '',
        $limit = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): array {
        $sql = $this->lastSql = $this->parseSql($where, $data, $table, $limit, $order, $group);
        if ($this->logDryRun()) {
            return [];
        }

        $cacheKey = 'ES_SQL_DATAS_' . md5($sql);
        if ($isCache) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }

        $result = $this->esClient->query($sql);

        if ($result === false) {
            $this->logger->error("sql: {$sql} error:执行sql异常");
            throw new RuntimeException('es 查询失败');
        }
        $rt = [];
        if ($result['_shards']['successful'] > 0) {
            if (empty($group)) {
                if(!empty($result['aggregations'])){
                    $val = array() ;
                    foreach($result['aggregations'] as $k => $aggregations){
                        $val[$k] = $aggregations['value'] ;
                    }
                    $rt[] = $val;
                }
            } else {
                $group_num = explode(',' , $group) ;
                if(count($group_num) == 1){
                    foreach ($result['aggregations'] as $key1 => $value1) {
                        foreach($value1['buckets'] as $k => $buckets){
                            $key1_val = $buckets['key'] ;
                            unset($buckets['key']) ;
                            unset($buckets['doc_count']) ;
                            $val = [] ;
                            $key1_k =str_replace('.keyword','',$key1) ;
                            $val[$key1_k] = $key1_val ;
                            foreach($buckets as $k1=> $v1 ){
                                $val[$k1] = $v1['value'] ;
                            }
                            $rt[] = $val;
                        }
                    }
                }else if(count($group_num) == 2){
                    foreach ($result['aggregations'] as $key1 => $value1) {
                        foreach($value1['buckets'] as $k => $buckets){
                            $key1_val = $buckets['key'] ;
                            unset($buckets['key']) ;
                            unset($buckets['doc_count']) ;
                            foreach($buckets as $key2 => $value2){
                                foreach($value2['buckets'] as $k_2=> $buckets2 ){
                                    $key2_val = $buckets2['key'] ;
                                    unset($buckets2['key']) ;
                                    unset($buckets2['doc_count']) ;
                                    $val = [] ;
                                    $key1_k =str_replace('.keyword','',$key1) ;
                                    $key2_k =str_replace('.keyword','',$key2) ;
                                    $val[$key1_k] = $key1_val ;
                                    $val[$key2_k] = $key2_val ;
                                    foreach($buckets2 as $k2=> $v2 ){
                                        $val[$k2] = $v2['value'] ;
                                    }
                                    $rt[] = $val;
                                }
                            }
                        }
                    }
                }else if(count($group_num) == 3){
                    foreach ($result['aggregations'] as $key1 => $value1) {
                        foreach($value1['buckets'] as $k => $buckets){
                            $key1_val = $buckets['key'] ;
                            unset($buckets['key']) ;
                            unset($buckets['doc_count']) ;
                            foreach($buckets as $key2 => $value2){
                                foreach($value2['buckets'] as $k_2=> $buckets2 ){
                                    $key2_val = $buckets2['key'] ;
                                    unset($buckets2['key']) ;
                                    unset($buckets2['doc_count']) ;
                                    foreach($buckets2 as $key3 => $value3){
                                        foreach($value3['buckets'] as $k_3 =>$buckets3 ){
                                            $key3_val = $buckets3['key'] ;
                                            unset($buckets3['key']) ;
                                            unset($buckets3['doc_count']) ;
                                            $val = [] ;
                                            $key1_k =str_replace('.keyword','',$key1) ;
                                            $key2_k =str_replace('.keyword','',$key2) ;
                                            $key3_k =str_replace('.keyword','',$key3) ;
                                            $val[$key1_k] = $key1_val ;
                                            $val[$key2_k] = $key2_val ;
                                            $val[$key3_k] = $key3_val ;
                                            foreach($buckets3 as $k3=> $v3 ){
                                                $val[$k3] = $v3['value'] ;
                                            }
                                            $rt[] = $val;
                                        }
                                    }

                                }
                            }
                        }
                    }
                }
            }
            $this->getCache()->set($cacheKey, $rt, $cacheTTL);
        } else {
            $this->logger->error('查询返回异常响应', [$result, $sql, func_get_args()]);
        }

        return $rt;
    }

    public function getOne(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ) {
        $result = $this->select($where, $data, $table, 1, $order, $group, $isCache, $cacheTTL);
        return $result[0] ?? [];
    }

    public function get_one(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): array {
        return $this->getOne($where, $data, $table, $order, $group, $isCache, $cacheTTL);
    }

    public function count(
        $where = '',
        string $table = '',
        string $group = '',
        string $data = '',
        string $cols = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): int {
        $sql = $this->lastSql = $this->parseSql($where, 'count(*)', $table, '', '', $group);
        if ($this->logDryRun()) {
            return 0;
        }

        $cacheKey = 'ES_SQL_DATAS_' . md5($sql);
        if ($isCache) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }

        $result = $this->esClient->query($sql);
        if ($result === false) {
            $this->logger->error("sql: {$sql} error:执行sql异常");
            throw new RuntimeException('es 查询失败');
        }

        $count = 0;
        if ($result['_shards']['successful'] > 0) {
            $aggregations = array_values($result['aggregations']);
            $count = (int)$aggregations[0]['value'];
            if ($isCache) {
                $this->getCache()->set($cacheKey, $count, $cacheTTL);
            }
        } else {
            $this->logger->error('查询返回异常响应', [$result, $sql, func_get_args()]);
        }

        return $count;
    }

    public function getLastSql(): string
    {
        return $this->lastSql;
    }

    protected function sqls($where, string $font = 'AND'): string
    {
        if (is_array($where)) {
            $sql = '';
            foreach ($where as $key => $val) {
                $sql .= " {$font} `{$key}`='{$val}'";
            }

            return $sql ? substr($sql, sizeof($font) + 1) : '';
        } else {
            return is_string($where) ? $where : '';
        }
    }

    protected function replaceGroupByFieldInSelectMax($group, string $data): string
    {
        if ($group && is_string($group) && 1 === preg_match('/^[^\(]/', trim($group))) {
            $data = preg_replace('/(\s*,\s*)?(max|min|sum|avg|count)\s*\(\s*' . trim($group) . '\s*\)/i', '$1', $data);
        }

        return $data;
    }

    protected function splitAggrField(string $str): string
    {
        return $str;
        // todo
        $sql = $str;

        if (false === strpos($sql, '(')) {
            return $sql;
        }

        $fields = [];
        $right = $left = $prev = 0;
        for ($i = 0, $len = strlen($sql); $i < $len; $i++) {
            switch ($sql[$i]) {
            case ',':
                if ($right === $left) {
                    $fields[] = substr($sql, $prev, $i - $prev);
                    $prev = $i + 1;
                    $left = $right = 0;
                }
                break;
            case '(':
                $left++;
                break;
            case ')':
                $right++;
                break;
            default:
            }
        }

        if ($prev < $i) {
            $fields[] = substr($sql, $prev);
        }

        foreach ($fields as $field) {
            //
        }

        return $sql;
    }

    protected function getMatchSql(array $match, bool $and = true)
    {
        $sql = '';
        $font = $and ? 'AND' : 'OR';
        $search = ["'", '"', '*', '-', '#', '&', '(', ')', '（', '）', '+', '/', '.', '×', '�', ','];
        $replace = [' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '];
        foreach ($match as $key => $val) {
            foreach(explode(' ', str_replace($search, $replace, trim($val))) as $v) {
                $v = strtolower(trim($v));
                if ($v !== '') {
                    $sql .= " {$font} {$key} LIKE '%{$v}%'";
                }
            }
        }

        return $sql ? ' (' . substr($sql, $adn ? 5: 4) . ') ' : '';
    }

    public static function escape(string $val): string
    {
        return \addslashes($val);
    }

    public function dryRun(?bool $dryRun): bool
    {
        if (null !== $dryRun) {
            $this->dryRun = $dryRun;
        }

        return $this->dryRun;
    }

    protected function logDryRun(): bool
    {
        if ($this->dryRun) {
            $this->logger->debug('ES dry run: ' . $this->getLastSql());
            return true;
        }
        return false;
    }
}
