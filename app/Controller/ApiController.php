<?php

declare(strict_types=1);
namespace App\Controller;
use App\Service\UserService;
use App\Service\GoodsService;
use Captainbi\Hyperf\Util\Result;
use Hyperf\HttpServer\Annotation\AutoController;
use Hyperf\HttpServer\Contract\ResponseInterface;
use Hyperf\HttpServer\Contract\RequestInterface;
use Hyperf\Validation\Contract\ValidatorFactoryInterface;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Paginator\Paginator;
use Hyperf\Utils\Collection;
use Hyperf\Di\Annotation\Inject;
use  \Hyperf\Contract\SessionInterface ;
/**
 * @AutoController()
 */


class ApiController extends BaseController
{
    private $redis;

    /**
     * @Inject()
     * @var ValidatorFactoryInterface
     */
    protected $validationFactory;

    /**
     * @Inject()
     * @var UserService
     */
    protected $userService;

    /**
     * @Inject()
     * @var GoodsService
     */
    protected $goodsService;

    public function __construct()
    {
        $container = ApplicationContext::getContainer();
        $this->redis = $container->get(\Redis::class);
    }

    public function test(ResponseInterface $response){
        $user_info = $this->userService->getUserInfo(304);
        return Result::success($user_info , '成功');

    }

    public function test2(){
        $goods_info = $this->goodsService->getGoodsList(304) ;
        return json_encode($goods_info , 256) ;
        return Result::success($goods_info , '成功');
    }

    public function getFinanceData()  {
        $this->redis->set('a' , '1234') ;
        $a = $this->redis->get('a');
        return Result::success(array($a) , '成功');
    }

    public function page(RequestInterface $request)
    {
        $currentPage = (int) $request->input('page', 1);
        $perPage = (int) $request->input('per_page', 2);

        // 这里根据 $currentPage 和 $perPage 进行数据查询，以下使用 Collection 代替
        $collection = new Collection([
            ['id' => 1, 'name' => 'Tom'],
            ['id' => 2, 'name' => 'Sam'],
            ['id' => 3, 'name' => 'Tim'],
            ['id' => 4, 'name' => 'Joe'],
        ]);
        $users = array_values($collection->forPage($currentPage, $perPage)->toArray());
        return new Paginator($users, $perPage, $currentPage);
    }

    public function check(RequestInterface $request , ResponseInterface $response)
    {
        $validator = $this->validationFactory->make(
            $request->all(),
            [
                'foo' => 'required',
                'bar' => 'required',
            ],
            [
                'foo.required' => 'foo is required',
                'bar.required' => 'bar is required',
            ]
        );

        if ($validator->fails()){
            // Handle exception
            $errorMessage = $validator->errors()->first();
            $response->raw($errorMessage);
        }

    }




}