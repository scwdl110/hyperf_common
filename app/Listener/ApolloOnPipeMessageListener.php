<?php

namespace App\Listener;

use Hyperf\ConfigApollo\Listener\OnPipeMessageListener;

class ApolloOnPipeMessageListener extends OnPipeMessageListener
{
    protected function formatValue($value)
    {
        if (!is_scalar($value)) {
            return $value;
        }

        return parent::formatValue($value);
    }
}
