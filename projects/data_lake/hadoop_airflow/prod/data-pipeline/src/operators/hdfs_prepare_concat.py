from airflow.hooks.base import BaseHook

from hooks.hdfs.hdfs_helper import HdfsHelper


class HdfsPrepareConcat(BaseHook):

    def __init__(self, hdfs_conn, *args, **kwargs):
        super(HdfsPrepareConcat, self).__init__(*args, **kwargs)
        self.hdfs_conn = hdfs_conn

    def execute(self, **context):
        template = '-' * 79
        hdfs = HdfsHelper(hdfs_conn=self.hdfs_conn)
        self.log.info(f'\n{template}\nGetting paths\n{template}')
        list_path = [
            hdfs.generate_hdfs_path(dag_id=context["dag_name"],
                                    env=context["env"],
                                    layer=context["layer"],
                                    date=date)
            for date in context['list_all_dates']
        ]
        self.log.info(list_path)

        self.log.info(f'\n{template}\nRemoving _SUCCESS\n{template}')
        hdfs.remove_empty_files(list_hdfs_path=list_path)

        self.log.info(f'\n{template}\nGetting paths to concat\n{template}')
        if context['agg_by'] is 'month':
            hdfs.create_dir_first_day(list_hdfs_path=list_path)

        self.log.info(list_path)

        if context['agg_by'] is 'month':
            self.log.info(f'\n{template}\nCreating folder YEAR/MONTH/01 each month\n{template}')
            list_path_to_mv = hdfs \
                .remove_path_first_day(list_path)

            self.log.info(f'list_path_to_mv = {list_path_to_mv}')
            self.log.info(f'\n{template}\nMoving files to first day each month\n{template}')
            hdfs \
                .mv_files_to_first_day(list_path_to_mv)
