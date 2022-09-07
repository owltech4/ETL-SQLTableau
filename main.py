import config as conf
import os
from table_definition import TESTE_table
import workflow_etl as etl


def import_teste():
    path_file_hyper = os.getcwd() + "\\" + 'teste.hyper'
    query = conf.sqlserver_conf['query_row_list_TESTE_pagination']
    table_for_import = TESTE_table
    number_row = 10000
    etl_command = conf.execute_etl_TESTE_sql['insert_table_temporary'].slipt('|')
    database_class, conn = etl.execute_sql_etl()
    etl.workflow_pagination(
        path_file_hyper=path_file_hyper,
        query=query,
        table_for_import=table_for_import,
        number_row=number_row,
        database_class=database_class,
        conn=conn
    )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    import_teste()
