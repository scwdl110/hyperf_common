<?php

declare(strict_types=1);


return [
    'athena_access_key' => env('ATHENA_ACCESS_KEY', ''),
    'athena_secret_key' => env('ATHENA_SECRET_KEY', ''),
    'athena_region' => env('ATHENA_REGION', 'cn-northwest-1'),
    'athena_version' => env('ATHENA_VERSION', "latest"),
    'athena_encryption_option' => env('ATHENA_ENCRYPTION_OPTION', "SSE_S3"),
    'athena_output_location' => env('ATHENA_OUTPUT_LOCATION', "s3://captain-athena-query-result/Athena-Presto/Athena-ark"),

];
