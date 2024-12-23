# FPT_Play_Error_Logs_Analytics
**Objective**: Analyze system error logs to identify outliers in specific time frames and evaluate their impact on user experience and customer retention.

**Contributions**:		              
- Data Collection and Storage: Collected raw log data in real-time and created physical tables in ClickHouse with appropriate engines to efficiently store and query the data.
- Data Automation: Automated data insertion, transformation, and aggregation processes using Airflow by designing and deploying custom DAGs.
- Outlier Detection: Applied statistical techniques, including STL Decomposition and IQR, to identify time frames with significant outliers in error occurrence.
- Error Analysis: Analyzed errors causing outliers by type, content, device, and platform, and assessed their impact on user experience and service churn.
- Visualization and Reporting: Designed interactive Superset dashboards to visualize error trends, outlier analysis, and insights for different error categories, supporting both immediate issue resolution and long-term system optimization.

**Tools Used**: SQL (Clickhouse), Superset, Airflow.
