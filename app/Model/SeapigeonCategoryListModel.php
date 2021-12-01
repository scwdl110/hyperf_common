<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace App\Model;

class SeapigeonCategoryListModel extends BaseModel
{
    protected $table = 'redo_seapigeon_category_list';
    protected $fillable = ['id', 'app_id', 'index_name', 'index_summary_name', 'document_id', 'positive_negative_number', 'is_delete'];
}
