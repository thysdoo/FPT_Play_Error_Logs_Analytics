#create
creat_amt_tbl = f""" CREATE TABLE IF NOT EXISTS stage_core.tbl_amt__agg__log_errors 
                     ON CLUSTER cluster_fptplay_bigdata (
                                `time_frames` DateTime,
                                `error_event` Nullable(String),
                                `error_code` Nullable(String),
                                `platform_group` Nullable(String),
                                `platform_sub_group` Nullable(String),
                                `platform` Nullable(String),
                                `content_type` Nullable(String),
                                `group_channel` Nullable(String),
                                `total_errors` AggregateFunction(sum, Float32),
                                `issue_id_cnt` AggregateFunction(uniqExact, String),
                                `users_cnt` AggregateFunction(uniqExact, String),
                                `item_id_cnt` AggregateFunction(uniqExact, String),
                                `item_name_cnt` AggregateFunction(uniqExact, String),
                                `device_id_ott_cnt` AggregateFunction(uniqExact, String),
                                `mac_cnt` AggregateFunction(uniqExact, String)
                                )
                    ENGINE = ReplicatedAggregatingMergeTree('{base_path}/{shard}/{database}/{table}','{replica}')
                    PARTITION BY toYYYYMM(time_frames)
                    ORDER BY (time_frames, error_event, error_code, platform_group, platform_sub_group, platform, content_type, group_channel)
                    TTL time_frames + INTERVAL 30 DAY 
                    SETTINGS index_granularity = 4096, allow_nullable_key = 1
                    """

create_dis_tbl = f""" CREATE TABLE IF NOT EXISTS stage_core.tbl_dis__agg__log_errors 
                     ON CLUSTER cluster_fptplay_bigdata (
                                `time_frames` Nullable(DateTime),
                                `error_event` Nullable(String),
                                `error_code` Nullable(String),
                                `platform_group` Nullable(String),
                                `platform_sub_group` Nullable(String),
                                `platform` Nullable(String),
                                `content_type` Nullable(String),
                                `group_channel` Nullable(String),
                                `total_errors` AggregateFunction(sum, Float32),
                                `issue_id_cnt` AggregateFunction(uniqExact, String),
                                `users_cnt` AggregateFunction(uniqExact, String),
                                `item_id_cnt` AggregateFunction(uniqExact, String),
                                `item_name_cnt` AggregateFunction(uniqExact, String),
                                `device_id_ott_cnt` AggregateFunction(uniqExact, String),
                                `mac_cnt` AggregateFunction(uniqExact, String)
                                )
                    ENGINE = Distributed('cluster_fptplay_bigdata', 'stage_core', 'tbl_amt__agg__log_errors', abs(cityHash64(coalesce(error_code,'')) % 5))
                    """



amt = 
CREATE TABLE stage_core.tbl_amt__agg__log_errors
(
    `time_frames` Nullable(DateTime),
    `error_event` Nullable(String),
    `error_code` Nullable(String),
    `platform_group` Nullable(String),
    `platform_sub_group` Nullable(String),
    `platform` Nullable(String),
    `content_type` Nullable(String),
    `group_channel` Nullable(String),
    `total_errors` AggregateFunction(sum, Float32),
    `issue_id_cnt` AggregateFunction(uniqExact, String),
    `users_cnt` AggregateFunction(uniqExact, String),
    `item_id_cnt` AggregateFunction(uniqExact, String),
    `item_name_cnt` AggregateFunction(uniqExact, String),
    `device_id_ott_cnt` AggregateFunction(uniqExact, String),
    `mac_cnt` AggregateFunction(uniqExact, String)
)
ENGINE = ReplicatedAggregatingMergeTree('{base_path}/{shard}/stage_core/tbl_amt__agg__log_errors', '{replica}')
PARTITION BY toYYYYMM(time_frames)
ORDER BY (time_frames, error_event, error_code, platform_group, platform_sub_group, platform, content_type, group_channel)
TTL coalesce(time_frames, now()) + INTERVAL 30 DAY  
SETTINGS index_granularity = 4096, allow_nullable_key = 1

dis = 
statement
CREATE TABLE stage_core.tbl_dis__agg__log_errors
(
    `time_frames` Nullable(DateTime),
    `error_event` Nullable(String),
    `error_code` Nullable(String),
    `platform_group` Nullable(String),
    `platform_sub_group` Nullable(String),
    `platform` Nullable(String),
    `content_type` Nullable(String),
    `group_channel` Nullable(String),
    `total_errors` AggregateFunction(sum, Float32),
    `issue_id_cnt` AggregateFunction(uniqExact, String),
    `users_cnt` AggregateFunction(uniqExact, String),
    `item_id_cnt` AggregateFunction(uniqExact, String),
    `item_name_cnt` AggregateFunction(uniqExact, String),
    `device_id_ott_cnt` AggregateFunction(uniqExact, String),
    `mac_cnt` AggregateFunction(uniqExact, String)
)
ENGINE = Distributed('cluster_fptplay_bigdata', 'stage_core', 'tbl_amt__agg__log_errors', abs(cityHash64(coalesce(error_code, '')) % 5))
