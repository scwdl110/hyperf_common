<?php

declare(strict_types=1);
namespace Captainbi\Hyperf\Base;

use Captainbi\Hyperf\Util\Result;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;

class Service
{
    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @param RequestInterface $request
     * @param array $rule
     * @param array $message
     * @param array $attribute
     * @return bool|string
     */
    protected function validate(RequestInterface $request, array $rule, array $message)
    {
        $validator = $this->validationFactory->make(
            $request->all(),
            $rule,
            $message
        );

        if ($validator->fails()){
            // Handle exception
            return Result::fail([], $validator->errors()->first());
        }

        return Result::success();
    }
}