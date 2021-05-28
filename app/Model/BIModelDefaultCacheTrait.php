<?php

namespace App\Model;

trait BIModelDefaultCacheTrait
{
    // @var ?bool
    protected $isDefaultCache = null;

    public function setDefaultCache(bool $defaultCache)
    {
        $this->isDefaultCache = $defaultCache;
    }

    public function getDefaultCache(): bool
    {
        return $this->isDefaultCache ?? false;
    }

    public function isCache(?bool $isCache): bool
    {
        return true === $isCache || (null === $isCache && $this->getDefaultCache());
    }
}
