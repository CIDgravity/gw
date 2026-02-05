-- Add node_id to groups table for scalable multi-node architecture
-- This allows multiple Kuri nodes to share the same database while
-- operating on different groups

alter table groups
    add node_id varchar(255) default null;

-- Create index for efficient node-specific queries
create index groups_node_id_index
    on groups (node_id);

-- Update the group stats view to filter by node_id
drop view if exists group_stats_view;
create view group_stats_view as
select
    node_id,
    count(*) as group_count,
    sum(bytes) AS total_data_size,
    sum(case when g_state < 4 then bytes else 0 end) as non_offloaded_data_size,
    sum(case when g_state = 4 then bytes else 0 end) as offloaded_data_size
from
    groups
group by node_id;
