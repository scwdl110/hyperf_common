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

use App\Model\AsinInfoModel;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class AsinInfoService extends BaseService {
    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @param $request_data
     * @return array
     */
    public function getAsinInfo($request_data){
        $rule = [
            'id' => 'required|integer',
        ];
        //不写默认
        $message = [
            'id.integer' => '?????',
        ];
        $attribute = [
            'id' => 'xzx'
        ];

        $res = $this->validate($request_data, $rule, $message, $attribute);
        if($res['code']== 0){
            return $res;
        }
        $asinInfo = AsinInfoModel::find($request_data['id']);
        $data =  [
            'code' => 1,
            'msg' => 'success',
            'data' => [
                'num' => 100,
                'list'=> $asinInfo->toArray()
            ]
        ];

        return $data;
    }
}
