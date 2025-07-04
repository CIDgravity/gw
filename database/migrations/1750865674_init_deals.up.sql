create table deals
(
    uuid                         uuid primary key,
    start_time                   timestamp        default now() not null,

    client_addr                  text    not null,
    provider_addr                bigint  not null,

    group_id                     bigint  not null,
    price_afil_gib_epoch         bigint  not null,
    verified                     boolean not null,
    keep_unsealed                boolean not null,

    start_epoch                  bigint  not null,
    end_epoch                    bigint  not null,

    signed_proposal_bytes        bytea   not null,

    deal_id                      bigint,
    deal_pub_ts                  text,
    sector_start_epoch           bigint,

    /* deal state */
    proposed                     integer not null default 0, /* 1 when the deal is successfully proposed */
    published                    integer not null default 0, /* publish cid is set, and we have validated the message is landed on chain with some finality */
    sealed                       integer not null default 0, /* deal state SectorStartEpoch set */

    failed                       integer not null default 0, /* 1 when the deal is unsuccessful for ANY reason */
    rejected                     integer not null default 0,

    failed_expired               integer not null default 0, /* 1 when the deal is failed AND the proposal start has passed TODO */

    error_msg                    text,

    /* status queries */
    last_state_query             bigint           default 0 not null,
    last_state_query_error       text,

    /* data transfer */
    car_transfer_start_time      bigint,
    car_transfer_attempts        bigint  not null default 0,

    car_transfer_last_end_time   bigint,
    car_transfer_last_bytes      bigint,

    /* sp deal state */
    sp_status                    text, /* boost checkpoint name */
    sp_sealing_status            text,
    sp_sig_proposal              text,
    sp_pub_msg_cid               text,

    sp_recv_bytes                bigint,
    sp_txsize                    bigint, /* todo swap for car_size in group? */

    /* market deal state checks */
    last_deal_state_check        bigint  not null default 0,

    /* retrieval checks */
    last_retrieval_check         bigint  not null default 0,
    last_retrieval_check_success bigint  not null default 0,
    retrieval_probes_success     bigint  not null default 0,
    retrieval_probes_fail        bigint  not null default 0,

    retrieval_probe_prev_error   text,

    retrieval_probe_prev_ms      bigint,
    retrieval_probe_prev_ttfb_ms bigint,

    sector_number                bigint
);

create table deals_archive as
select *
from deals
where false;

/* SP tracker */
create table providers
(
    id                  bigint primary key,

    in_market           boolean not null,

    ping_ok             boolean not null default false,

    boost_deals         boolean not null default false,
    booster_http        boolean not null default false,
    booster_bitswap     boolean not null default false,

    indexed_success     bigint  not null default 0,
    indexed_fail        bigint  not null default 0,

    retrprobe_success   bigint  not null default 0,
    retrprobe_fail      bigint  not null default 0,
    retrprobe_blocks    bigint  not null default 0,
    retrprobe_bytes     bigint  not null default 0,

    ask_ok              boolean not null default false,
    ask_price           bigint  not null default 0,
    ask_verif_price     bigint  not null default 0,
    ask_min_piece_size  bigint  not null default 0,
    ask_max_piece_size  bigint  not null default 0,

    addr_info_graphsync text,
    addr_info_bitswap   text,
    addr_info_http      text
);

create table offloads_s3
(
    group_id bigint primary key
);

create table external_path
(
    group_id bigint primary key,
    module   text,
    path     text
);

create table repairs
(
    group_id          bigint primary key,
    retrievable_deals bigint           not null,
    worker            bigint,
    last_attempt      bigint default 0 not null
);

create view bad_providers_new_reject_view as
select d.provider_addr AS sp_id
from deals d
where d.start_time >= now() - interval '2 hours'
group by d.provider_addr
having count(*) > 2
   and count(case when d.rejected = 1 then 1 end) = count(*);

create view sp_deal_stats_view as
select d.provider_addr                             as sp_id,
       count(*)                                    as total_deals,
       count(case when d.published = 1 then 1 end) as published_deals,
       count(case when d.sealed = 1 then 1 end)    as sealed_deals,
       count(case when d.failed = 1 then 1 end)    as failed_deals,
       count(case when d.rejected = 1 then 1 end)  as rejected_deals,
       case
           when count(case when d.rejected = 0 then 1 else null end) >= 4
               and count(case when d.rejected = 0 then 1 else null end) * 4
                    < count(case when d.failed = 1 and d.rejected = 0 then 1 else null end) * 5
               then 1
           else 0
           end                                     as failed_all
from deals d
         join
     groups g ON d.group_id = g.id
where d.start_time >= now() - interval '3 days'
group by d.provider_addr;

create view sp_retr_stats_view as
select d.provider_addr as sp_id,
       count(case
                 when d.last_retrieval_check < (d.last_retrieval_check_success + 3600 * 24) then 1
           end)        as retrievable_deals,
       count(case
                 when d.last_retrieval_check > (d.last_retrieval_check_success + 3600 * 24) then 1
           end)        as unretrievable_deals
from deals d
where d.last_retrieval_check > 0
group by d.provider_addr;

create table bad_providers_new_reject as
select *
from bad_providers_new_reject_view
where false;

create table sp_deal_stats as
select *
from sp_deal_stats_view
where false;

create table sp_retr_stats as
select *
from sp_retr_stats_view
where false;

create table good_providers
(
    id                 bigint primary key,
    ping_ok            boolean,

    boost_deals        boolean,
    booster_http       boolean,
    booster_bitswap    boolean,

    indexed_success    bigint,
    indexed_fail       bigint,

    retrprobe_success  bigint,
    retrprobe_fail     bigint,
    retrprobe_blocks   bigint,
    retrprobe_bytes    bigint,

    ask_price          bigint,
    ask_verif_price    bigint,
    ask_min_piece_size bigint,
    ask_max_piece_size bigint
);

create index idx_providers_eligible on providers (in_market, ping_ok, ask_ok, ask_min_piece_size, ask_max_piece_size);

create index idx_deals_provider on deals (provider_addr, group_id, rejected, start_time);

create index idx_deals_group on deals (group_id, rejected, start_time);

create index idx_deals_retrieval on deals (last_retrieval_check, last_retrieval_check_success);

create index idx_deals_start_time_rejected_failed
    on deals (rejected, failed, start_time);
