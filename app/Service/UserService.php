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
use App\Model\UserModel;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class UserService extends BaseService {
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



    public function getUserInfo($admin_id = null,$user_id = null){
        $user_id = intval($admin_id);
        $user_info = UserModel::query()->where(array('id'=>$user_id , 'status'=>1))->first();
        if(empty($user_info)){
            $user_info = array();
        }
        return $user_info;
    }

}
