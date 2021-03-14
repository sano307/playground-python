import boto3

from redshift_data_client import RedshiftDataClient


rs_data_client = RedshiftDataClient(
    data_client=boto3.client("redshift-data", region_name="ap-northeast-1"),
    cluster_identifier="REDSHIFT_CLUSTER_IDENTIFIER",
    database="DATABASE_NAME",
    db_user="DATABASE_USERNAME",
)

sql = "SELECT DISTINCT(tablename) FROM pg_table_def WHERE schemaname = 'public'"

def main():
    records = rs_data_client.execute_query(sql)['Records']
    print(records)


if __name__ == '__main__':
    main()
