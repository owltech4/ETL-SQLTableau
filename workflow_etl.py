import os
from datetime import datetime
import config as conf
import sqlserver
import csv_generate
import convert_csv_to_hyper

project_name = 'Teste tableau'
server_address = conf.tableau_parameter['server_address']
site_name = conf.tableau_parameter['site_name']
token_name = conf.tableau_parameter['token_name']
token_value = conf.tableau_parameter['token_value']
host = conf.sqlserver_conf['host']
database = conf.sqlserver_conf['database']
username = conf.sqlserver_conf['username']
password = conf.sqlserver_conf['password']


def workflow_pagination(path_file_hyper, query,
                        table_for_import, number_row=10000,
                        database_class=None, conn=None):
    """
    Controla o fluxo das etapas para que seja gerado o arquivo hyper e enviado ao servidor
    :param database_class: classe responsavel pelas operações dentro do banco de dados
    :param conn: conexão mativa disponível para operações que precisa que a conexão permaneça aberta
    :param number_row: quantidade de linhas para paginação
    :param path_file_hyper: nome do arquivo com o caminho absoluto
    :param query: consulta sql para geração dos dados
    :param table_for_import: tabela com as definições necessárias para criação da estrutura do hyper
    :return:
    """
    if database_class is None:
        database_class = sqlserver.SqlServer(host=host,
                                             database=database,
                                             username=username,
                                             password=password)

    print(f'Tempo inicial para consultar os dados {datetime.now()}')

    rowInitial = 1
    rowFinal = number_row

    print(f'Pagination of row start {datetime.now()}')
    list_sql_row = database_class.query_return_rows(
        sql_query=query.replace(f'{rowInitial}', str(rowInitial)).replace(f'{rowFinal}', str(rowFinal))
        , conn=conn)
    print(f'Pagination of row finish {datetime.now()}')
    increment_csv = 0
    delimiter = '|'
    csv_file_template = path_file_hyper.split('.')[0]
    csv_list_file = []
    while list_sql_row.__len__() > 0:
        increment_csv = increment_csv + 1
        csv_file = csv_file_template + f'_{increment_csv}.csv'
        csv_generate.save_csv(list_sql_row=list_sql_row,
                              csv_file=csv_file,
                              delimiter=delimiter)
        csv_list_file.append(csv_file)

        rowInitial = rowInitial + number_row
        rowFinal += number_row

        print(f'Pagination of row start {datetime.now()}')

        list_sql_row = database_class.query_return_rows(
            sql_query=query.replace(f'{rowInitial}', str(rowInitial)).replace(f'{rowFinal}', str(rowFinal))
            , conn=conn)
        print(f'Pagination of row finish {datetime.now()}')
    print(f'Tempo termino para consultar os dados {datetime.now()}')

    print(f'Tempo inicial para gerar o hyper {datetime.now()}')
    row_count = convert_csv_to_hyper.generate_hyper(csv_path_with_file_name=csv_list_file,
                                                    hyper_path_with_file_name=path_file_hyper,
                                                    delimiter=delimiter,
                                                    table_for_import=table_for_import)
    print(f'Remover os arquivos gerados csv')
    for file in csv_list_file:
        os.remove(file)

    print(f'Number of row => {row_count}')

    if row_count > 0:
        convert_csv_to_hyper.send_data_server(server_address=server_address,
                                              site_name=site_name,
                                              token_name=token_name,
                                              token_value=token_value,
                                              project_name=project_name)
    print(f'Tempo final para gerar o hyper {datetime.now()}')

    if conn:
        conn.close()


def workflow(path_file_hyper, query, table_for_import):
    """
    Controla o fluxo das etapas para que seja gerado o arquivo hyper e enviado ao servidor
    :param path_file_hyper: nome do arquivo com o caminho absoluto
    :param query: consulta sql para geração dos dados
    :param table_for_import: tabela com as definições necessárias para criação da estrutura do hyper
    :return:
    """

    database_class = sqlserver.SqlServer(host=host,
                                         database=database,
                                         username=username,
                                         password=password)
    print(f'Tempo inicial para consultar os dados {datetime.now()}')
    list_sql_row = database_class.query_return_rows(sql_query=query)
    print(f'Tempo termino para consultar os dados {datetime.now()}')

    print(f'Tempo inicial para gerar o hyper {datetime.now()}')
    row_count = convert_csv_to_hyper.generate_hyper_rows(list_rows=list_sql_row,
                                                         hyper_path_with_file_name=path_file_hyper,
                                                         table_for_import=table_for_import)
    print(f'Number of rows -> {row_count}')

    if row_count > 0:
        convert_csv_to_hyper.send_data_server(server_address=server_address,
                                              site_name=site_name,
                                              token_name=token_name,
                                              token_value=token_value,
                                              hyper_name=path_file_hyper,
                                              project_name=project_name)
    print(f'Tempo final para gerar hyper {datetime.now()}')


def execute_sql_etl(etl_command):
    """
    Executa os comandos de etl recebidos
    :param etl_command:
    :return:
    """
    print(f'Start etl {datetime.now()}')
    database_class = sqlserver.SqlServer(host=host,
                                         database=database,
                                         username=username,
                                         password=password)
    conn = database_class.__conn__()

    for query in etl_command:
        try:
            print(f'Execute command etl -> {query}')
            database_class.execute_scalar_query(query, conn)

        except Exception as ex:
            raise ex
    print(f'Finish ETL {datetime.now()}')
    return database_class, conn
