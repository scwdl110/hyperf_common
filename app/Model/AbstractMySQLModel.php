<?php

namespace App\Model;

use Hyperf\DbConnection\Db;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Psr\Log\LoggerInterface;
use Psr\SimpleCache\CacheInterface;
use Psr\Http\Message\ServerRequestInterface;

abstract class AbstractMySQLModel extends BaseModel implements BIModelInterface
{
    protected $lastSql = '';

    protected $cache = null;

    protected $dbhost = '001';

    protected $codeno = '001';

    protected $dryRun = false;

    protected $logger = null;

    public function __construct(array $attributes = [], string $dbhost = '', string $codeno = '', ?LoggerInterface $logger = null)
    {
        $this->logger = $logger;

        if ('' === $dbhost) {
            $userInfo = ApplicationContext::getContainer()->get(ServerRequestInterface::class)->getAttribute('userInfo', []);
            $dbhost = $userInfo['dbhost'] ?? '';
            $codeno = $userInfo['codeno'] ?? '';
        }

        if (!is_numeric($dbhost) || !is_numeric($codeno)) {
            $this->getLogger()->error('错误的 mysql dbhost 或 codeno', [$dbhost, $codeno]);
            throw new RuntimeException('Invalid dbhost or codeno.');
        }
        $this->dbhost = trim($dbhost);
        $this->codeno = trim($codeno);

        if ($this->connection && strlen($this->connection) - 1 === strrpos($this->connection, '_')) {
            $this->connection = $this->connection . $this->dbhost;
        }

        if ($this->table && strlen($this->table) - 1 === strrpos($this->table, '_')) {
            $this->table = $this->table . $this->codeno;
        }

        parent::__construct($attributes);
    }

    protected function getLogger(): LoggerInterface
    {
        if (null === $this->logger) {
            $this->logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('mysql', 'default');
        }

        return $this->logger;
    }

    /**
     * 获取完整的表名
     *
     * @param bool $includeDatabase 是否包含数据库名
     * @param bool $includeQuote 是否使用 ` 字符将表名包含起来
     * @return string
     */
    public function getTableName(bool $includeDatabase = false, bool $includeQuote = true): string
    {
        $quote = $includeQuote ? '`' : '';
        $table = $this->getConnection()->getTablePrefix() . $this->getTable();

        if ($includeDatabase) {
            $table = $this->connection . "{$quote}.{$quote}" . $table;
        }

        return "{$quote}{$table}{$quote}";
    }

    public static function escape(string $val): string
    {
        // v.a. https://www.php.net/manual/en/function.mysql-real-escape-string.php#101248
        if (!empty($val)) {
            return str_replace(
                ['\\', "\0", "\n", "\r", "'", '"', "\x1a"],
                ['\\\\', '\\0', '\\n', '\\r', "\\'", '\\"', '\\Z'],
                $val
            );
        }

        return $val;
    }

    protected function getCache()
    {
        if (null === $this->cache) {
            $this->cache = ApplicationContext::getContainer()->get(CacheInterface::class);
        }

        return $this->cache;
    }

    public function getLastSql(): string
    {
        return $this->lastSql;
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
        if (is_array($where)) {
            $where = $this->sqls($where);
        }
        $table = $table ?: $this->getTableName();

        $where = $where == '' ? '' : ' WHERE ' . $where;
        $order = $order == '' ? '' : ' ORDER BY ' . $order;
        $group = $group == '' ? '' : ' GROUP BY ' . $group;
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

        $sql = $this->lastSql = "SELECT {$data} FROM {$table} {$where} {$group} {$order} {$limit}";
        if ($this->logDryRun()) {
            return [];
        }

        $cacheKey = 'MYSQL_SQL_DATAS_' . md5($sql);
        if ($isCache) {
            $cacheData = $this->getCache()->get($cacheKey);
            if(!empty($cacheData)){
                return $cacheData;
            }
        }

        $result = Db::connection($this->connection)->select($sql);
        // 返回的是 object[](PDO::FETCH_OBJ，且无法单独设置)
        // 为了实现 BIModelInterface，只能这样转换(by hyperf/database v2.1.10)
        foreach ($result as &$item) {
            $item = (array)$item;
        }

        if ($isCache) {
            $this->getCache()->set($cacheKey, $result, $cacheTTL);
        }

        return $result;
    }

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
                $sql .= sprintf(" %s `%s`='%s'", $font, $key, self::escape($val));
            }

            return $sql ? substr($sql, sizeof($font) + 1) : '';
        } else {
            return is_string($where) ? $where : '';
        }
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
            $this->getLogger()->debug('MySQL dry run: ' . $this->getLastSql());
            return true;
        }

        return false;
    }
}
