create table CidGroups
(
    cid    text,
    groups set<bigint>,
    primary key ( cid )
)