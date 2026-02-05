create table S3Objects
(
    bucket     text,
    key        text,
    cid        text,
    size       bigint,
    updated    timestamp,
    node_id    text,
    expires_at timestamp,
    primary key (bucket, key)
);