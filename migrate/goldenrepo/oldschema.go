// The DDL below is copied verbatim from the last pebble+sqlite revision
// (rbstor/db.go and rbdeal/deal_db.go at commit 31e7708), so generated
// golden repositories match what real old deployments contain.
package goldenrepo

const oldRbstorSchema = `

/* groups */

create table if not exists groups
(
    id        integer not null
        constraint groups_pk
            primary key autoincrement,
    blocks      integer not null,
    bytes integer not null,    
    /* States
	 * 0 - writable
     * 1 - full
     * 2 - vrcar done
     * 3 - has commp
     * 4 - offloaded
     * 5 - reload
     */
    g_state     integer not null,
    
    /* jbob */
    jb_recorded_head integer not null,
    
    /* vrcar */
    piece_size integer,
    commp blob,
    car_size integer,
    root blob
);

create index if not exists groups_id_index
    on groups (id);

create index if not exists groups_g_state_index
    on groups (g_state);

CREATE VIEW IF NOT EXISTS group_stats_view AS
SELECT 
    COUNT(*) AS group_count,
    SUM(bytes) AS total_data_size,
    SUM(CASE WHEN g_state < 4 THEN bytes ELSE 0 END) AS non_offloaded_data_size,
    SUM(CASE WHEN g_state = 4 THEN bytes ELSE 0 END) AS offloaded_data_size
FROM 
    groups;

create table if not exists offloads
(
	group_id  integer not null
	    constraint offloads_groups_id_pk
	    		    primary key
		constraint offloads_groups_id_fk
			references groups
				on update cascade on delete cascade
);

create index if not exists offloads_group_id_index
	on offloads (group_id);
`

const oldDealsSchema = `
/* deals */
create table if not exists deals (
    uuid text not null constraint deals_pk primary key,
    start_time integer default (strftime('%s','now')) not null,

    client_addr text not null,
    provider_addr integer not null,

    group_id integer not null,
    price_afil_gib_epoch integer not null,
    verified integer not null,
    keep_unsealed integer not null,

    start_epoch integer not null,
    end_epoch integer not null,

    signed_proposal_bytes blob not null,

    deal_id integer,
    deal_pub_ts text,
    sector_start_epoch integer,

    /* deal state */
    proposed integer not null default 0, /* 1 when the deal is successfully proposed */
    published integer not null default 0, /* publish cid is set, and we have validated the message is landed on chain with some finality */
    sealed integer not null default 0, /* deal state SectorStartEpoch set */

    failed integer not null default 0, /* 1 when the deal is unsuccessful for ANY reason */
    rejected integer not null default 0,

    failed_expired integer not null default 0, /* 1 when the deal is failed AND the proposal start has passed TODO */

    error_msg text,

    /* status queries */
    last_state_query integer default 0 not null,
    last_state_query_error text,

    /* data transfer */
    car_transfer_start_time integer,
    car_transfer_attempts integer not null default 0,

    car_transfer_last_end_time integer,
    car_transfer_last_bytes integer,

    /* sp deal state */
    sp_status text, /* boost checkpoint name */
    sp_sealing_status text,
    sp_sig_proposal text,
    sp_pub_msg_cid text,

    sp_recv_bytes integer,
    sp_txsize integer, /* todo swap for car_size in group? */

    /* market deal state checks */
    last_deal_state_check integer not null default 0,

    /* retrieval checks */
    last_retrieval_check integer not null default 0,
    last_retrieval_check_success integer not null default 0,
    retrieval_probes_success integer not null default 0,
    retrieval_probes_fail integer not null default 0,

    retrieval_probe_prev_error text,

    retrieval_probe_prev_ms integer,
    retrieval_probe_prev_ttfb_ms integer
);

CREATE TABLE IF NOT EXISTS deals_archive AS SELECT * FROM deals WHERE 0;

/* SP tracker */
create table if not exists providers (
    id integer not null constraint providers_pk primary key,
    
    in_market integer not null,
    
    ping_ok integer not null default 0,
    
    boost_deals integer not null default 0,
    booster_http integer not null default 0,
    booster_bitswap integer not null default 0,
    
    indexed_success integer not null default 0,
    indexed_fail integer not null default 0,

    retrprobe_success integer not null default 0,
    retrprobe_fail integer not null default 0,
    retrprobe_blocks integer not null default 0,
    retrprobe_bytes integer not null default 0,
    
    ask_ok integer not null default 0,
    ask_price integer not null default 0,
    ask_verif_price integer not null default 0,
    ask_min_piece_size integer not null default 0,
    ask_max_piece_size integer not null default 0,

    addr_info_graphsync text,
    addr_info_bitswap text,
    addr_info_http text
);

create table if not exists offloads_s3
(
    group_id integer not null
        constraint offloads_s3_pk
            primary key
);
create table if not exists external_path
(
    group_id integer not null
        constraint offloads_s3_pk
            primary key,
    module text,
    path text
);

create table if not exists repairs
(
    group_id          integer           not null
        constraint repairs_pk
            primary key,
    retrievable_deals integer           not null,
    worker            integer,
    last_attempt      integer default 0 not null
);

drop view if exists sp_deal_stats_view;
drop view if exists sp_retr_stats_view;
drop view if exists bad_providers_new_reject_view;
drop view if exists good_providers_view;

CREATE VIEW IF NOT EXISTS bad_providers_new_reject_view AS
    SELECT 
        d.provider_addr AS sp_id
    FROM 
        deals d
    WHERE 
        d.start_time >= strftime('%s', 'now', '-2 hours')
    GROUP BY
        d.provider_addr
    HAVING 
        COUNT(*) > 2
        AND COUNT(CASE WHEN d.rejected = 1 THEN 1 ELSE NULL END) = COUNT(*);

CREATE VIEW IF NOT EXISTS sp_deal_stats_view AS
    SELECT
        d.provider_addr AS sp_id,
        COUNT(*) AS total_deals,
        COUNT(CASE WHEN d.published = 1 THEN 1 ELSE NULL END) AS published_deals,
        COUNT(CASE WHEN d.sealed = 1 THEN 1 ELSE NULL END) AS sealed_deals,
        COUNT(CASE WHEN d.failed = 1 THEN 1 ELSE NULL END) AS failed_deals,
        COUNT(CASE WHEN d.rejected = 1 THEN 1 ELSE NULL END) AS rejected_deals,
        CASE
            WHEN COUNT(CASE WHEN d.rejected = 0 THEN 1 ELSE NULL END) >= 4 
                AND COUNT(CASE WHEN d.rejected = 0 THEN 1 ELSE NULL END) * 4 
                    < COUNT(CASE WHEN d.failed = 1 AND d.rejected = 0 THEN 1 ELSE NULL END) * 5 
            THEN 1
            ELSE 0
            END AS failed_all
    FROM
        deals d
            JOIN
        groups g ON d.group_id = g.id
    WHERE
        d.start_time >= strftime('%s', 'now', '-3 days')
    GROUP BY
        d.provider_addr;

CREATE VIEW IF NOT EXISTS sp_retr_stats_view AS
SELECT
    d.provider_addr AS sp_id,
    COUNT(CASE WHEN d.last_retrieval_check < (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS retrievable_deals,
    COUNT(CASE WHEN d.last_retrieval_check > (d.last_retrieval_check_success + 3600*24) THEN 1 ELSE NULL END) AS unretrievable_deals
FROM
    deals d
WHERE
        d.last_retrieval_check > 0
GROUP BY
    d.provider_addr;

CREATE TABLE IF NOT EXISTS bad_providers_new_reject AS SELECT * FROM bad_providers_new_reject_view WHERE 0;
CREATE TABLE IF NOT EXISTS sp_deal_stats AS SELECT * FROM sp_deal_stats_view WHERE 0;
CREATE TABLE IF NOT EXISTS sp_retr_stats AS SELECT * FROM sp_retr_stats_view WHERE 0;

CREATE TABLE IF NOT EXISTS good_providers (
	id INTEGER PRIMARY KEY,
	ping_ok INTEGER,
	
	boost_deals INTEGER,
	booster_http INTEGER,
	booster_bitswap INTEGER,
	
	indexed_success INTEGER,
	indexed_fail INTEGER,
	
	retrprobe_success INTEGER,
	retrprobe_fail INTEGER,
	retrprobe_blocks INTEGER, 
	retrprobe_bytes INTEGER,
	
	ask_price INTEGER,
	ask_verif_price INTEGER,
	ask_min_piece_size INTEGER,
	ask_max_piece_size INTEGER
);

CREATE INDEX IF NOT EXISTS idx_providers_eligible ON providers(in_market, ping_ok, ask_ok, ask_min_piece_size, ask_max_piece_size);
CREATE INDEX IF NOT EXISTS idx_deals_provider ON deals(provider_addr, group_id, rejected, start_time);
CREATE INDEX IF NOT EXISTS idx_deals_group ON deals(group_id, rejected, start_time);
CREATE INDEX IF NOT EXISTS idx_deals_retrieval ON deals(last_retrieval_check, last_retrieval_check_success);

CREATE INDEX IF NOT EXISTS idx_deals_start_time_rejected_failed
    ON deals (rejected, failed, start_time);


CREATE TABLE IF NOT EXISTS schema_version (
    version_number INTEGER PRIMARY KEY,
    description TEXT,
    applied_on DATETIME DEFAULT CURRENT_TIMESTAMP
);
`
