<?php

namespace App\Listener;

use Hyperf\ConfigApollo\Listener\BootProcessListener;

class ApolloBootProcessListener extends BootProcessListener
{
    protected function formatValue($value)
    {
        if (!is_scalar($value)) {
            return $value;
        }

        return parent::formatValue($value);
    }
}
