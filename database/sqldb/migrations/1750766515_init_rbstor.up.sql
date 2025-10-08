create table groups
(
    id               serial primary key,
    blocks           bigint  not null,
    bytes            bigint  not null,

    /* States
     * 0 - writable
     * 1 - full
     * 2 - vrcar done
     * 3 - has commp
     * 4 - offloaded
     * 5 - reload
     */
    g_state          integer not null,

    /* jbob */
    jb_recorded_head bigint  not null,

    /* vrcar */
    piece_size       bigint,
    commp            bytea,
    car_size         bigint,
    root             bytea
);

create index groups_g_state_index
    on groups (g_state);

create view group_stats_view as
select
    count(*) as group_count,
    sum(bytes) AS total_data_size,
    sum(case when g_state < 4 then bytes else 0 end) as non_offloaded_data_size,
    sum(case when g_state = 4 then bytes else 0 end) as offloaded_data_size
from
    groups;

create table offloads
(
    group_id integer primary key
        constraint offloads_groups_id_fk
            references groups
            on update cascade on delete cascade
);