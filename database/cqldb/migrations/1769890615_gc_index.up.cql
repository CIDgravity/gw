-- Reverse index: Group -> Multihashes
-- Enables O(n) enumeration of all blocks in a group for GC operations
-- Without this, finding all blocks in a group requires full table scan of MultihashToGroup
create table if not exists GroupToMultihash
(
    GroupId   bigint,
    Multihash blob,
    Size      int,
    primary key ( GroupId, Multihash )
);

-- Reference counting for multihashes
-- Tracks how many S3 objects reference each block
-- ref_count = 0 means the block is orphaned and can be GC'd
create table if not exists MultihashRefCount
(
    Multihash  blob primary key,
    RefCount   counter
);

-- GC queue for batched processing of orphaned blocks
-- Entries are added when ref_count drops to 0
-- Processed periodically by GC worker
create table if not exists GCQueue
(
    Epoch     bigint,
    Multihash blob,
    GroupId   bigint,
    QueuedAt  timestamp,
    primary key ( Epoch, Multihash )
);
