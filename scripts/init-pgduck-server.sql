-- create minio aws secret
CREATE SECRET s3_secret(
        TYPE s3,
        scope 's3://warehouse',
        use_ssl false,
        key_id 'minio_root',
        secret 'm1n1opwd',
        url_style 'path',
        endpoint 'minio:9000',
        region 'us-west-2'
);

-- create minio aws secret for data
CREATE SECRET s3_secret_data(
        TYPE s3,
        scope 's3://data',
        use_ssl false,
        key_id 'minio_root',
        secret 'm1n1opwd',
        url_style 'path',
        endpoint 'minio:9000',
        region 'us-west-2'
);
