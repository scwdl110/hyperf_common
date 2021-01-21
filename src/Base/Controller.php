<?php

declare(strict_types=1);
namespace Captainbi\Hyperf\Base;


use App\Controller\AbstractController;

class Controller extends AbstractController
{
    /**
     * @var 具体service注入
     */
    protected $service;
}