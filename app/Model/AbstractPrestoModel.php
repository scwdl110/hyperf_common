<?php

namespace App\Model;

use RuntimeException;

use App\Lib\Presto;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use Psr\Http\Message\ServerRequestInterface;
use GuzzleHttp\ClientInterface;

use Hyperf\Utils\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Logger\LoggerFactory;

abstract class AbstractPrestoModel implements BIModelInterface
{
    protected $dbhost = '001';

    protected $codeno = '001';

    protected $presto = null;

    protected $logger = null;

    protected $lastSql = '';

    protected $table = '';

    protected $cache = null;

    protected $dryRun = false;

    public function __construct(
        string $dbhost = '',
        string $codeno = '',
        ?LoggerInterface $logger = null,
        ?ClientInterface $httpClient = null
    ) {
        $container = ApplicationContext::getContainer();
        if (null === $logger) {
            $logger = $container->get(LoggerFactory::class)->get('presto', 'default');
        }
        $this->logger = $logger;

        if ('' === $dbhost) {
            $userInfo = $container->get(ServerRequestInterface::class)->getAttribute('userInfo', []);
            $dbhost = $userInfo['dbhost'] ?? '';
            $codeno = $userInfo['codeno'] ?? '';
        }

        if (!is_numeric($dbhost) || !is_numeric($codeno)) {
            $this->logger->error('错误的 presto dbhost 或 codeno', [$dbhost, $codeno]);
            throw new RuntimeException('Invalid dbhost or codeno.');
        }
        $this->dbhost = trim($dbhost);
        $this->codeno = trim($codeno);

        $config = $container->get(ConfigInterface::class)->get('presto', []);
        if (empty($config)) {
            $this->logger->error('presto 配置信息不存在');
            throw new RuntimeException('Missing Presto config.');
        }

        $config = $config[$this->dbhost] ?? [];
        if (empty($config)) {
            $this->logger->error('presto 数据库配置不存在', [$config]);
            throw new RuntimeException('Missing Presto connection config.');
        }

        if ($this->table && strlen($this->table) - 1 === strrpos($this->table, '_')) {
            $this->table = $this->table . $this->dbhost;
        }

        $this->presto = Presto::getConnection($config, $this->logger, $httpClient);
    }

    protected function getCache()
    {
        if (null === $this->cache) {
            $this->cache = ApplicationContext::getContainer()->get(CacheInterface::class);
        }

        return $this->cache;
    }

    public function query(string $sql, array $bindings = [], bool $isCache = false, int $cacheTTL = 300): array
    {
        if ($bindings) {
            $this->lastSql = $psql = "PREPARE {$sql}; EXECUTE " . @join(',', $bindings);
        } else {
            $this->lastSql = $psql = $sql;
        }

        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($psql);
        if ($isCache) {
            $result = $this->getCache()->get($cacheKey);
            if (!empty($result)) {
                return $result;
            }
        }

        $result = $this->presto->query($sql, ...$bindings);
        if (false === $result) {
            $this->logger->error("sql: {$psql} error:执行sql异常");
            return [];
        }

        if ($isCache) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }
        return $result;
    }

    public function fetch(string $sql, array $bindings = [], bool $isCache = false, int $cacheTTL = 300): array
    {
        $result = $this->query($sql, $bindings, $isCache, $cacheTTL);
        return $result[0] ?? [];
    }

    public function getLastSql(): string
    {
        return $this->lastSql;
    }

    /**
     * 兼容 基础BI 的 model 操作
     *
     * 执行sql查询
     * @param $where 		查询条件[例`name`='$name']
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $table 		查询表[默认当前模型表]
     * @param $limit 		返回结果范围[例：10或10,10 默认为空]
     * @param $order 		排序方式	[默认按数据库默认方式排序]
     * @param $group 		分组方式	[默认为空]
     * @param $is_cache     是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @return array		查询结果集数组
     */
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
        $where = is_array($where) ? $this->sqls($where) : $where;
        $table = $table !== '' ? $table : $this->table;

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

        $sql = $this->lastSql = "SELECT {$data} FROM {$table}{$where}{$group}{$order}{$limit}";
        if ($this->logDryRun()) {
            return [];
        }

        $cacheKey = 'PRESTO_SQL_DATAS_' . md5($sql);
        if ($isCache) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }

        $result = $this->presto->query($sql);
        if ($result === false) {
            $this->logger->error("sql: {$sql} error:执行sql异常");
            throw new RuntimeException('presto 查询失败');
        }

        if ($isCache) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }

        return $result;
    }

    /**
     * 兼容 基础BI 的 model 操作
     *
     * @param $where 		查询条件[例`name`='$name']
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $table 		查询表[默认当前模型表]
     * @param $order 		排序方式	[默认按数据库默认方式排序]
     * @param $group 		分组方式	[默认为空]
     * @param $is_cache     是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @return array		查询结果集数组
     */
    public function getOne(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): array {
        $result = $this->select($where, $data, $table, 1, $order, $group, $isCache, $cacheTTL);

        return $result[0] ?? [];
    }

    /** @see $this->getOne */
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

    /**
     * 兼容 基础BI 的 model 操作
     *
     * 计算记录数
     * @param $where 		查询条件[例`name`='$name']
     * @param $table 		查询表[默认当前模型表]
     * @param $group 		分组方式	[默认为空]
     * @param $data 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $cols 		需要查询的字段值[例`name`,`gender`,`birthday`]
     * @param $isCache      是否读取缓存数据
     * @param $cacheTTL     缓存保存时间（秒）
     * @return int		    记录数
     */
    public function count(
        $where = '',
        string $table = '',
        string $group = '',
        string $data = '',
        string $cols = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): int {
        $where = is_array($where) ? $this->sqls($where) : $where;

        if ($group) {
            $result = $this->getOne(
                '',
                "COUNT(*) AS num",
                "(SELECT {$data} FROM {$table} WHERE {$where} GROUP BY {$group} ORDER BY null) AS tmp",
                '',
                '',
                $isCache,
                $cacheTTL
            );
        } elseif (!empty($cols)) {
            $result = $this->getOne($where, "COUNT({$cols}) AS num", $table, '', '', $isCache, $cacheTTL);
        } else {
            $result = $this->getOne($where, "COUNT(*) AS num", $table, '', '', $isCache, $cacheTTL);
        }

        return intval($result['num'] ?? 0);
    }

    /**
     * 将数组转换为SQL语句
     *
     * @param array $where 要生成的数组
     * @param string $font 连接串。
     */
    protected function sqls($where, string $font = 'AND'): string
    {
        if (is_array($where)) {
            $sql = '';
            foreach ($where as $key => $val) {
                $sql .= sprintf(" %s `%s`=%s", $font, $key, Presto::bindValue($val));
            }

            return $sql ? substr($sql, sizeof($font) + 1) : '';
        } else {
            return is_string($where) ? $where : '';
        }
    }

    public static function escape(string $val): string
    {
        return Presto::escape((string)$val);
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
            $this->logger->debug('Presto dry run: ' . $this->getLastSql());
            return true;
        }

        return false;
    }
}
