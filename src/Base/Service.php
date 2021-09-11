<?php

declare(strict_types=1);
namespace Captainbi\Hyperf\Base;

use Hyperf\Validation\Contract\ValidatorFactoryInterface;
use Hyperf\Di\Annotation\Inject;

class Service
{
    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @param array $request_data
     * @param array $rule
     * @param array $message
     * @return array
     */
    protected function validate(array $request_data, array $rule=[], array $message=[], $attribute=[])
    {
        $validator = $this->validationFactory->make(
            $request_data,
            $rule,
            $message,
            $attribute
        );

        if ($validator->fails()){
            // Handle exception
            $data =  [
                'code' => 0,
                'msg' => $validator->errors()->first()
            ];
            return $data;
        }

        $data =  [
            'code' => 1,
            'msg' => 'success'
        ];
        return $data;
    }
}