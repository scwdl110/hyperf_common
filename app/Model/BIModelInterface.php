<?php

namespace App\Model;

/**
 * 兼容旧版基础BI的 model 查询方法
 *
 * @author Chopin Ngo <wushaobin@captainbi.com>
 */
interface BIModelInterface
{
    public static function escape(string $val): string;

    public function getLastSql(): string;

    public function select(
        $where = '',
        string $data = '*',
        string $table = '',
        $limit = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): array;

    public function get_one(
        $where = '',
        string $data = '*',
        string $table = '',
        string $order = '',
        string $group = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): array;

    public function count(
        $where = '',
        string $table = '',
        string $group = '',
        string $data = '',
        string $cols = '',
        bool $isCache = false,
        int $cacheTTL = 300
    ): int;
}
