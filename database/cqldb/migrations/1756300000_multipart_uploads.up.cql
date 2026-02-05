create table MultipartUploads
(
    upload_id  text,
    bucket     text,
    key        text,
    node_id    text,
    created_at timestamp,
    expires_at timestamp,
    status     text,
    primary key (upload_id)
);
