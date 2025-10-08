create table if not exists MultihashToGroup
(
    Multihash blob,
    Group     bigint,
    Size      int,
    primary key ( Multihash, Group )
)