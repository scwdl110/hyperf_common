<?php


namespace Captainbi\Hyperf\Base;


use Captainbi\Hyperf\Exception\BusinessException;

abstract class Logic
{
    protected $model;
    protected $validate;

    public function __construct()
    {
        $this->model = $this->setModel();
        $this->validate = $this->setValidate();
    }

    abstract protected function setModel();

    abstract protected function setValidate();

    public function get(int $id)
    {
        // 我们假设存在一个 Info 实体
        return $this->model::where('id', $id)->find();
    }

    public function insert($array)
    {
        $pk = $this->model->getPk();
        if (isset($array[$pk])) {
            throw new BusinessException('9999', '新增数据不能提交' . $pk . '参数');
        }
        return $this->save($array);
    }

    public function update($array)
    {
        $pk = $this->model->getPk();
        if (!isset($array[$pk])) {
            throw new BusinessException('9999', '更新数据必须提交' . $pk . '参数');
        }
        return $this->save($array);
    }


    private function save($array)
    {
        validate($this->validate)->check($array);
        $pk = $this->model->getPk();
        if (isset($array[$pk])) {//更新
            return $this->model::update($array);
        } else {//插入
            return $this->model::create($array);
        }
    }

    public function delete($id)
    {
        return $this->model::destroy($id);
    }

}