<?php
declare(strict_types=1);

namespace Captainbi\Hyperf\Util;

use Captainbi\Hyperf\Exception\BusinessException;
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
        $time = time();
        $this->logger->error($this->time);
        if(!$this->time) {
            $this->time = $time;
        }elseif($time - $this->time > 10 && $this->client){
            //执行select 1
            try{
                $res = $this->client->query("select 1");
                $res = $this->client->fetchRow($res)[0];
                if(!$res){
                    throw new BusinessException();
                }
            }catch (\Exception $e){
                $this->reconnect();
            }

            $this->time = $time;
        }

        if (!empty($config)) {
            $dsn = $this->getDSN($config);
            $client = $dsn ? $this->getConnect($dsn) : null;

            return $client;
        }

        if ($this->needReconnect()) {
            $this->reconnect();
        }

        return $this->client;
    }

    /**
     * 重连
     */
    private function reconnect(){
        $config = $this->config ?: $this->getDefaultConfig();
        $dsn = $this->getDSN($config);
        if ($dsn) {
            $this->config = $config;
            $this->client = $this->getConnect($dsn);
        } else {
            $this->client = null;
            $this->logger->error('pgsql缺少dsn');
        }

        return true;
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

    /**
     * 是否需要重新连接 pgsql
     * 网络异常导致的连接异常或连接丢失才需要重新连接
     *
     * @return bool
     */
    protected function needReconnect(): bool
    {
        if (null === $this->client) {
            return true;
        }

        if (null === $this->client->error) {
            return false;
        }

        $error = trim($this->client->error);
        // ontimeout 是 swoole-ext-postgresql 自定义的错误
        // 其余的错误均来至 postgresql 10.16
        // postgresql 还包含其他网络相关的错误，但 swoole-ext-postgresql 涉及到的网络错误应该就这些了
        // 整理于 2021-12-08
        // if ('ontimeout' === $error
        //     || 0 === strpos($error, 'connection not open')
        //     || 0 === strpos($error, 'connection in wrong state')
        //     || 0 === strpos($error, 'connection pointer is NULL')
        //     || 0 === strpos($error, 'no connection to the server')
        //     || 0 === strpos($error, 'server closed the connection unexpectedly')
        //     || 0 === strpos($error, 'SSL SYSCALL error: ')
        //     || 0 === strpos($error, 'unexpected asyncStatus: ')
        //     || 0 === strpos($error, 'invalid connection state,')
        //     || 0 === strpos($error, 'could not send data to server: ')
        //     || 0 === strpos($error, 'could not get socket error status: ')
        //     || 0 === strpos($error, 'could not receive data from server: ')
        // ) {
        //     return true;
        // }

        $this->logger->error("pgsql 未知的错误类型：{$error}");
        return true;
    }
}
