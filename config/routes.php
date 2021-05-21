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
use Hyperf\HttpServer\Router\Router;

Router::get('/favicon.ico', function () {
    return '';
});

Router::post('/dataark/unGoodsDatas', "App\Controller\DataArkController@getUnGoodsDatas");
Router::post('/dataark/goodsDatas', "App\Controller\DataArkController@getGoodsDatas");
Router::post('/dataark/operatorsDatas', "App\Controller\DataArkController@getOperatorsDatas");
