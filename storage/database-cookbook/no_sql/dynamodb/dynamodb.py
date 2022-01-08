import boto3


def connection(region_name: str, endpoint_url: str, table_name: str):
    dynamodb = boto3.resource('dynamodb',
                              region_name=region_name,
                              endpoint_url=endpoint_url)
    return = dynamodb.Table(table_name)


def insert_db(pk, task_id, task_name, env, date_to_run, start_time, end_time, execution_time):
    return connection().put_item(
        Item={
            "pk": pk,
            "task_id": task_id,
            "task_name": task_name,
            "environment": env,
            "date_to_run": date_to_run,
            "start_time": start_time,
            "end_time": end_time,
            "execution_time": execution_time
        }
    )


def main():
    connection(region_name='region_name', 
               endpoint_url='http://endpoint_url',
               table_name='table')
    insert_db(str("prod.luigi.etl_metrics"),
              str(self.task_id),
              self.task_name,
              self.env,
              str(self.date_to_run),
              str(self.start_time),
              str(self.end_time),
              str(self.execution_time)) 


if __name__ = '__main__':
    main()
