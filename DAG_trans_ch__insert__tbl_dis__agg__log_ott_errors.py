from datetime import datetime, timedelta
import pendulum
import json

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from extra.utils.kubernetes import set_k8s_exec_config
from airflow.sensors.external_task import ExternalTaskSensor 
from extra.utils.alert import send_email_on_failure                        #send mail khi lỗi

#PART1: SCHEDULE
#1.1. Default
#DAG default agruments 
default_args = {
    'owner': 'thuydtm14@fpt.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email':['thuydtm14@fpt.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': send_email_on_failure
}

#1.3. Set DAG 
dag = DAG(
    'trans_ch__insert__tbl_dis__agg__log_ott_errors',
    default_args=default_args,
    description="""Insert transformed data into tbl_dis__trans__log_ott_errors and aggregated data into tbl_dis__agg__log_ott_errors""",
    schedule_interval='*/10 * * * *',
    start_date=pendulum.datetime(2024, 12, 7, tz=("Asia/Ho_Chi_Minh")),
    catchup=True,
    on_failure_callback=send_email_on_failure,                     
    tags=["thuydtm14","core", "insert", "clickhouse","log_ott_errors","log_error_outliers"],
)   
 
#PART2: CREATING TASKS
#2.1. Insert data into rmt_tbl
insert_raw_data_into_trans_tbl = ClickHouseOperator(
    task_id="insert_raw_data_into_trans_tbl",
    clickhouse_conn_id="clickhouse__haproxy_111_123__v1__4bi",
    sql= """WITH
                            tab as (
                            SELECT 
                                received_at,
                                event as error_event,
                                error_code,
                                error_message as error_mess,
                                issue_id,
                                platform_group,
                                platform_sub_group,
                                platform,
                                user_id,
                                profile_id,
                                contract,
                                content_type,
                                item_id,
                                item_name,
                                content_channel_group as group_channel,
                                if(source_log != 'iptv-box', device_id, '') as device_id_ott, 
                                device_name,
                                if(source_log = 'iptv-box', device_id, '') as mac,
                                net_mode as netmode,
                                isp,
                                os_version,
                                fptplay_version,
                                _hash
                                FROM stage_core.tbl_dis__raw__qos__cus_log_ott_box__rt
                                WHERE 1=1
                                AND received_at >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND received_at < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND log_id = '515'

                            UNION ALL
                            SELECT 
                                received_at,
                                event as error_event,
                                error_code,
                                error_message as error_mess,
                                issue_id,
                                platform_group,
                                platform_sub_group,
                                platform,
                                user_id,
                                profile_id,
                                contract,
                                content_type,
                                item_id,
                                item_name,
                                content_channel_group as group_channel,
                                if(source_log != 'iptv-box', device_id, '') as device_id_ott, 
                                device_name,
                                if(source_log = 'iptv-box', device_id, '') as mac,
                                net_mode as netmode,
                                isp,
                                os_version,
                                fptplay_version,
                                _hash
                                FROM stage_core.tbl_dis__raw__qos__cus_log_ott_mobile__rt
                                WHERE 1=1
                                AND received_at >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND received_at < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND log_id = '515'  

                            UNION ALL
                            SELECT 
                                received_at,
                                event as error_event,
                                error_code,
                                error_message as error_mess,
                                issue_id,
                                platform_group,
                                platform_sub_group,
                                platform,
                                user_id,
                                profile_id,
                                contract,
                                content_type,
                                item_id,
                                item_name,
                                content_channel_group as group_channel,
                                if(source_log != 'iptv-box', device_id, '') as device_id_ott, 
                                device_name,
                                if(source_log = 'iptv-box', device_id, '') as mac,
                                net_mode as netmode,
                                isp,
                                os_version,
                                fptplay_version,
                                _hash
                                FROM stage_core.tbl_dis__raw__qos__cus_log_ott_smarttv__rt
                                WHERE 1=1
                                AND received_at >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND received_at < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND log_id = '515'

                            UNION ALL
                            SELECT 
                                received_at,
                                event as error_event,
                                error_code,
                                error_message as error_mess,
                                issue_id,
                                platform_group,
                                platform_sub_group,
                                platform,
                                user_id,
                                profile_id,
                                contract,
                                content_type,
                                item_id,
                                item_name,
                                content_channel_group as group_channel,
                                if(source_log != 'iptv-box', device_id, '') as device_id_ott, 
                                device_name,
                                if(source_log = 'iptv-box', device_id, '') as mac,
                                net_mode as netmode,
                                isp,
                                os_version,
                                fptplay_version,
                                _hash
                                FROM stage_core.tbl_dis__raw__qos__cus_log_ott_web__rt
                                WHERE 1=1
                                AND received_at >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND received_at < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND log_id = '515'

                            UNION ALL
                            SELECT 
                                received_at,
                                event as error_event,
                                error_code,
                                error_message as error_mess,
                                issue_id,
                                platform_group,
                                platform_sub_group,
                                platform,
                                user_id,
                                profile_id,
                                contract,
                                content_type,
                                item_id,
                                item_name,
                                content_channel_group as group_channel,
                                if(source_log != 'iptv-box', device_id, '') as device_id_ott, 
                                device_name,
                                if(source_log = 'iptv-box', device_id, '') as mac,
                                net_mode as netmode,
                                isp,
                                os_version,
                                fptplay_version,
                                _hash
                                FROM stage_core.tbl_dis__raw__qos__cus_log_iptv_box__rt
                                WHERE 1=1
                                AND received_at >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND received_at < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                AND log_id = '515'
                            )
                            INSERT INTO stage_core.tbl_dis__trans__log_ott_errors
                            SELECT * FROM tab
                            """ ,
    query_id="{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}",
    dag = dag,
    executor_config={                                                                                                 #cấu hình tài nguyên dành cho task
    "mem_requests": "0.5Gi", 
    "mem_limits": "0.5Gi", 
    "cpu_requests": 0.01,  
    "cpu_limits": 0.1
    }
)

#2.2. Clear data in amt_tbl before insert data
clear_data_in_amt_tbl = ClickHouseOperator(
    task_id="clear_data_in_amt_tbl",
    clickhouse_conn_id="clickhouse__haproxy_111_123__v1__4bi",
    sql= """ALTER TABLE stage_core.tbl_amt__agg__log_errors ON CLUSTER cluster_fptplay_bigdata
                    DELETE WHERE time_frames >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                             AND time_frames < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                    """,
    query_id="{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}",
    dag = dag,
    executor_config={                                                                                                 #cấu hình tài nguyên dành cho task
    "mem_requests": "0.5Gi", 
    "mem_limits": "0.5Gi", 
    "cpu_requests": 0.01,  
    "cpu_limits": 0.1
    }
)

#2.3. Insert trans data into amt_tbl
insert_trans_data_into_amt_tbl = ClickHouseOperator(
    task_id="insert_trans_data_into_amt_tbl",
    clickhouse_conn_id="clickhouse__haproxy_111_123__v1__4bi",
    sql= """INSERT INTO stage_core.tbl_dis__agg__log_ott_errors
                                 WITH cte AS (SELECT
                                                toStartOfInterval(received_at, INTERVAL 1 minute) AS time_frames,
                                                error_event,
                                                error_code,
                                                platform_group,
                                                platform_sub_group,
                                                platform,
                                                content_type,
                                                group_channel, 
                                                ifNull(issue_id, '') AS issue_id, -- Thay thế NULL bằng chuỗi rỗng
                                                ifNull(user_id, '') AS user_id,
                                                ifNull(item_id, '') AS item_id,
                                                ifNull(item_name, '') AS item_name,
                                                ifNull(device_id_ott, '') AS device_id_ott,
                                                ifNull(mac, '') AS mac,
                                                CAST(COUNT(1) AS Float32) AS errors_cnt
                                        FROM
                                                stage_core.tbl_dis__trans__log_ott_errors
                                        GROUP BY  
                                                time_frames,
                                                error_event,
                                                error_code,
                                                platform_group,
                                                platform_sub_group,
                                                platform,
                                                content_type,
                                                group_channel, 
                                                issue_id, 
                                                user_id, 
                                                item_id, 
                                                item_name, 
                                                device_id_ott, 
                                                mac
                                                ) 
                                        SELECT        
                                                    time_frames,
                                                    error_event,
                                                    error_code,
                                                    platform_group,
                                                    platform_sub_group,
                                                    platform,
                                                    content_type,
                                                    group_channel,
                                                    sumState(errors_cnt) total_errors,
                                                    uniqExactState(issue_id) AS issue_id_cnt,
                                                    uniqExactState(user_id) AS users_cnt,
                                                    uniqExactState(item_id) AS item_id_cnt,
                                                    uniqExactState(item_name) AS item_name_cnt,
                                                    uniqExactState(device_id_ott) AS device_id_ott_cnt,
                                                    uniqExactState(mac) AS mac_cnt
                                        FROM        cte 
                                        WHERE       1=1
                                        AND         time_frames >= (toDateTime('{{ data_interval_start.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                        AND         time_frames < (toDateTime('{{ data_interval_end.in_tz("Asia/Ho_Chi_Minh").strftime('%Y-%m-%d %H:%M:%S') }}') - INTERVAL 10 MINUTE)
                                        GROUP BY    time_frames,
                                                    error_event,
                                                    error_code,
                                                    platform_group,
                                                    platform_sub_group,
                                                    platform,
                                                    content_type,
                                                    group_channel
                                        """ ,
    query_id="{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}",
    dag = dag,
    executor_config={                                                                                                 #cấu hình tài nguyên dành cho task
    "mem_requests": "0.5Gi", 
    "mem_limits": "0.5Gi", 
    "cpu_requests": 0.01,  
    "cpu_limits": 0.1
    }
)

#2.2 Dummy
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

#PART3: DEPENDENCIES
start >> insert_raw_data_into_trans_tbl >> clear_data_in_amt_tbl >> insert_trans_data_into_amt_tbl >> end