CREATE TABLE IF NOT EXISTS MultihashToGroup (
    Multihash BLOB,
    Group bigint,
    Size INT,
    PRIMARY KEY ( Multihash, Group )
)