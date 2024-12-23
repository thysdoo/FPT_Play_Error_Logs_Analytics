create_tbl_rmt__trans__log_ott_errors = f"""CREATE TABLE IF NOT EXISTS stage_core.tbl_rmt__trans__log_ott_errors 
                                            ON CLUSTER cluster_fptplay_bigdata
                                                (
                                                    `received_at` DateTime,
                                                    `error_event` Nullable(String),
                                                    `error_code` Nullable(String),
                                                    `error_mess` Nullable(String),
                                                    `issue_id` Nullable(String),
                                                    `platform_group` Nullable(String),
                                                    `platform_sub_group` Nullable(String),
                                                    `platform` Nullable(String),
                                                    `user_id` Nullable(String),
                                                    `profile_id` Nullable(String),
                                                    `contract` Nullable(String),
                                                    `content_type` Nullable(String),
                                                    `item_id` Nullable(String),
                                                    `item_name` Nullable(String),
                                                    `group_channel` Nullable(String),
                                                    `device_id_ott` Nullable(String),
                                                    `device_name` Nullable(String),
                                                    `mac` Nullable(String),
                                                    `netmode` Nullable(String),
                                                    `isp` Nullable(String),
                                                    `os_version` Nullable(String),
                                                    `fptplay_version` Nullable(String),
                                                    `_hash` String
                                                ) 
                                                ENGINE = ReplicatedReplacingMergeTree('{base_path}/{shard}/stage_core/tbl_rmt__trans__log_ott_errors', '{replica}',received_at)
                                                ORDER BY (received_at, error_code, issue_id, user_id, device_id_ott,_hash) 
                                                TTL received_at + INTERVAL 7 DAY 
                                                SETTINGS index_granularity = 8192, allow_nullable_key = 1 
                                                """

create_tbl_dis__trans__log_ott_errors  = f"""CREATE TABLE IF NOT EXISTS stage_core.tbl_dis__trans__log_ott_errors 
                                            ON CLUSTER cluster_fptplay_bigdata (
                                                    `received_at` DateTime,
                                                    `error_event` Nullable(String),
                                                    `error_code` Nullable(String),
                                                    `error_mess` Nullable(String),
                                                    `issue_id` Nullable(String),
                                                    `platform_group` Nullable(String),
                                                    `platform_sub_group` Nullable(String),
                                                    `platform` Nullable(String),
                                                    `user_id` Nullable(String),
                                                    `profile_id` Nullable(String),
                                                    `contract` Nullable(String),
                                                    `content_type` Nullable(String),
                                                    `item_id` Nullable(String),
                                                    `item_name` Nullable(String),
                                                    `group_channel` Nullable(String),
                                                    `device_id_ott` Nullable(String),
                                                    `device_name` Nullable(String),
                                                    `mac` Nullable(String),
                                                    `netmode` Nullable(String),
                                                    `isp` Nullable(String),
                                                    `os_version` Nullable(String),
                                                    `fptplay_version` Nullable(String),
                                                    `_hash` String
                                            ) ENGINE = Distributed('cluster_fptplay_bigdata', 'stage_core', 'tbl_rmt__trans__log_ott_errors', abs(cityHash64(received_at) % 5))
                                            """