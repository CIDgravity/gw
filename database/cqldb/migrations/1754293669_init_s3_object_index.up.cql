create table S3Objects
(
    bucket  text,
    key     text,
    cid     text,
    size    bigint,
    updated timestamp,
    primary key (bucket, key)
);