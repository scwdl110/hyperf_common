<?php
namespace App\Lib;
use Hyperf\Utils\ApplicationContext;
class Redis
{
    protected $redis ;
    public function __construct()
    {
        $container = ApplicationContext::getContainer();
        $this->redis = $container->get(\Redis::class);
    }


    public function keys($name){
        $value = $this->redis->keys($name);
        return $value ;
    }
    /**
    集合添加
     **/
    public function sadd($name,$value){
        return $this->redis->SADD($name,$value);
    }
    /**
    集合大小
     **/
    public function scard($name){
        return $this->redis->SCARD($name);
    }
    /**
    返回集合所有元素
     **/
    public function smembers($name){
        return $this->redis->SMEMBERS($name);
    }

    /**
    返回集合中的n个，并删除
     **/
    public function srandmember($name,$size=100){
        return $this->redis->SRANDMEMBER($name,$size);
    }

    public function spop($name){
        return $this->redis->SPOP($name);
    }

    public function get($name) {
        $value = $this->redis->get($name);
        return unserialize($value);
    }

    public function expireEx($name, $expire= 20){
        if($expire!==0){
            $this->redis->EXPIRE($name, $expire);
        }
    }

    public function ttlEx($name){
        return $this->redis->TTL($name);
    }

    /**
     * 写入缓存
     */
    public function set($name, $value, $expire = 86400) {
        $value = serialize($value);
        if ($expire == 0) {
            $ret = $this->redis->set($name, $value);
        } else {
            $ret = $this->redis->setex($name, $expire, $value);
        }
        return $ret;
    }

    /**
     * 删除缓存
     */
    public function rm($name) {
        return $this->redis->delete($name);
    }


    /**
     * 清除缓存
     */
    public function clear() {
        return $this->redis->flushDB();
    }

    /**
     * 入队
     */
    public function push($key, $name) {
        $res = $this->redis->LPUSH($key, serialize($name));
        return $res;
    }


    public function rpush($key, $name) {
        $res = $this->redis->RPUSH($key, serialize($name));
        return $res;
    }

    public function rpop($name) {
        $data = unserialize($this->redis->RPOP($name));
        return $data;
    }

    public function pop($name) {
        $data = unserialize($this->redis->LPOP($name));
        return $data;
    }

}