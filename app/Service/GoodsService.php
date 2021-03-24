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
namespace App\Service;
use App\Model\GoodsModel;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Di\Annotation\Inject;

use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class GoodsService extends BaseService {
    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @Inject()
     * @var ConfigInterface
     */
    protected $config;

    public function getGoodsList($user_id = 0){
        $goods_md = new GoodsModel('001','001');
        $goods_list = $goods_md::query()->where(array('user_id'=>$user_id))->first();
        return $goods_list;
    }

}
