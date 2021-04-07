<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Swoole\Coroutine\PostgreSQL;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Utils\ApplicationContext;
use Psr\Log\LoggerInterface;

/**
 * Swoole PostgresSQL 封装类
 * 需要安装 swoole_postgres 扩展
 * v.a. https://github.com/swoole/ext-postgresql
 *
 * @author Chopin Ngo <wushaobin@captainbi.com>
 */
class Pgsql
{
    // @var int 返回所有查询结果
    const RETURN_TYPE_ALL = 1;

    // @var int 返回第一条查询结果
    const RETURN_TYPE_ONE = 2;

    // @var int 返回第一条结果的第一个字段值
    const RETURN_TYPE_COLUMN = 3;

    // @var ?LoggerInterface 日志实例
    protected $logger = null;

    // @var ?PostgreSQL  pgsql 实例
    protected $client = null;

    // @var array 连接参数
    protected $config = [];

    // @var int 最后一条 sql 的受影响行数
    protected $affectedRows = 0;

    // @var string 最后执行的 sql 语句
    protected $lastSql = '';

    // @var array 最后执行的 prepare sql 语句和 bind 数据
    protected $lastPrepare = [];

    /**
     * @param array $config
     * @param ?LoggerInterface $logger
     */
    public function __construct(array $config = [], ?LoggerInterface $logger = null)
    {
        if (null === $logger) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('pgsql', 'default');
        }

        $this->logger = $logger;

        $config = $config ?: $this->getDefaultConfig();
        if (!empty($config)) {
            $dsn = $this->getDSN($config);
            if ($dsn) {
                $this->config = $config;
                $this->client = $this->getConnect($dsn);
            }
        }
    }

    /**
     * 获取默认数据库连接参数
     *
     * @param string $configkey
     * @return array
     */
    protected function getDefaultConfig(string $configKey = 'databases.pgsql'): array
    {
        return ApplicationContext::getContainer()->get(ConfigInterface::class)->get($configKey, []);
    }

    /**
     * 获取 pgsql 连接
     *
     * @param string $dsn
     * @return ?PostgreSQL
     */
    protected function getConnect(string $dsn): ?PostgreSQL
    {
        try {
            $client = new PostgreSQL();
            if (false !== $client->connect($dsn)) {
                return $client;
            }

            $this->logger->error('连接 pgsql 失败: ' . $client->error, [$dsn]);
        } catch (Throwable $t) {
            $this->logger->error('连接 pgsql 异常: ' . $t->getMessage(), [$dsn]);
        }

        return null;
    }

    /**
     * 获取 pgsql 数据库连接 DSN
     *
     * @param array $config
     * @return string
     */
    protected function getDSN(array $config): string
    {
        // pgsql 默认端口
        $config['port'] = $config['port'] ?? 5432;
        // 默认空密码
        $config['password'] = $config['password'] ?? '';
        if (!isset($config['host'], $config['database'], $config['username'])) {
            $this->logger->error('缺少必须的 pgsql 连接参数', $config);
            return '';
        }

        if (!is_numeric($config['port']) || !(is_string($config['host']) && is_string($config['database'])
            && is_string($config['username']) && is_string($config['password']))
        ) {
            $this->logger->error('无效 pgsql 连接参数', $config);
            return '';
        }

        return sprintf(
            'host=%s;port=%d;dbname=%s;user=%s;password=%s',
            $config['host'],
            $config['port'],
            $config['database'],
            $config['username'],
            $config['password']
        );
    }

    /**
     * 获取数据库链接实例
     *
     * @param array $config   数据库连接参数
     * @return ?PostgreSQL    成功返回 PostgreSQL 实例，失败返回 null
     */
    public function getClient(array $config = []): ?PostgreSQL
    {
        if (!empty($config)) {
            $dsn = $this->getDSN($config);
            $client = $dsn ? $this->getConnect($dsn) : null;

            return $client;
        }

        if (null === $this->client || 'ontimeout' === $this->client->error) {
            $config = $this->config ?: $this->getDefaultConfig();
            $dsn = $this->getDSN($config);
            if ($dsn) {
                $this->config = $config;
                $this->client = $this->getConnect($dsn);
            } else {
                $this->client = null;
            }
        }

        return $this->client;
    }

    /**
     * prepare 查询
     *
     * @param string $sql
     * @param array $bind
     * @param int $returnType
     * @return false|mixed  失败返回 false，无数据返回空数组
     */
    private function prepareQuery(string $sql, array $bind, int $returnType = self::RETURN_TYPE_ALL)
    {
        $pg = $this->getClient();
        if (null === $pg) {
            $this->logger->error('获取 pgsql 连接失败');
            return false;
        }

        $res = false;
        if ($bind) {
            // 修复 prepared statement "query_stmt" already exists 错误
            $stmt = sprintf('query_smtm_%.6f%d', microtime(true), rand());
            if ($pg->prepare($stmt, $sql)) {
                $res = $pg->execute($stmt, $bind);
            }

            $replace = [];
            foreach ($bind as $k => $v) {
                if (is_string($v)) {
                    $esc = $pg->escapeLiteral($v);
                    $esc = false === $esc ? $v : $esc;
                } elseif (is_bool($v)) {
                    $esc = $v ? 'true' : 'false';
                } else {
                    $esc = null === $v ? 'null' : (string)$v;
                }

                $replace['$' . ($k + 1)] = $esc;
            }
            // 这里使用输出会出现 varchar 字符没有引号的情况
            // todo 这里用这种简单的方式来替换，不考虑 \$1 这类的转义字符，也不推荐在 sql 中用这样的转义字符
            $this->lastSql = @strtr($sql, $replace);
        } else {
            $res = $pg->query($sql);
            $this->lastSql = $sql;
        }

        $this->lastPrepare = ['sql' => $sql, 'bind' => $bind];
        if ($res) {
            if ($returnType === self::RETURN_TYPE_COLUMN) {
                $result = $pg->fetchRow($res);
                $result = false === $result ? [] : $result[0];
            } else {
                $result = (self::RETURN_TYPE_ALL === $returnType ? $pg->fetchAll($res) : $pg->fetchAssoc($res)) ?: [];
            }

            $this->affectedRows = $pg->affectedRows($res);
            return $result;
        }

        $this->logger->error('执行 pgsql 查询失败: ' . $pg->error, [$sql, $bind]);
        return false;
    }

    /**
     * 查询并返回所有结果
     *
     * @param string $sql
     * @param mixed ...$bind
     * @return false|array
     */
    public function query(string $sql, ...$bind)
    {
        return $this->prepareQuery($sql, $bind, self::RETURN_TYPE_ALL);
    }

    /**
     * 查询并返回所有结果，self::query 的别名方法
     *
     * @param string $sql
     * @param mixed ...$bind
     * @return false|array
     */
    public function all(string $sql, ...$bind)
    {
        return $this->prepareQuery($sql, $bind, self::RETURN_TYPE_ALL);
    }

    /**
     * 查询并返回第一条结果
     *
     * @param string $sql
     * @param mixed ...$bind
     * @return false|array
     */
    public function one(string $sql, ...$bind)
    {
        return $this->prepareQuery($sql, $bind, self::RETURN_TYPE_ONE);
    }

    /**
     * 查询并返回第一条结果的第一个字段值
     *
     * @param string $sql
     * @param mixed ...$bind
     * @return false|[]|mixed 查询失败返回 false，查询无结果返回空数组，有结果返回对应值
     * 无结果返回空数组主要为了和查询结果为null做区分
     */
    public function column(string $sql, ...$bind)
    {
        return $this->prepareQuery($sql, $bind, self::RETURN_TYPE_COLUMN);
    }

    /**
     * 返回最后一次执行受影响的行数
     *
     * @return int
     */
    public function affectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * 返回最后执行的 SQL 语句
     *
     * @return string
     */
    public function lastSql(): string
    {
        return $this->lastSql;
    }

    /**
     * 返回追后执行的 Prepare sql 数据
     *
     * @return array 数据结构如下
     * [
     *     'sql' => string,
     *     'bind' => [scalar, scalar ... scalar],
     * ]
     */
    public function lastPrepare(): array
    {
        return $this->lastPrepare;
    }
}
