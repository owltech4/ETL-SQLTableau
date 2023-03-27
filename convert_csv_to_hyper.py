import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    escape_string_literal, Inserter
from context_ssl import no_ssl_verification


def generate_hyper(csv_path_with_file_name, hyper_path_with_file_name, delimiter, table_for_import):
    path_to_database = hyper_path_with_file_name

    process_parameters = {
        # Limits the number of Hyper event log files to two
        "log_file_max_count": "2",
        # limits the size of Hyper event log files to 100MB
        "log_file_size_limit": "100M"
    }

    with HyperProcess(Telemetry = Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
        connection_parameters = {"lc_time": "pt_Br"}

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        parameter=connection_parameters) as connection:
            connection.catalog.create_table(table_definition=table_for_import)

            path_to_csv = csv_path_with_file_name

            for item in path_to_csv:
                connection.execute_command(
                    command=f"COPY {table_for_import.table_name} from {escape_string_literal(item)} with"
                            f"(format csv, NULL, 'NULL', delimiter '{delimiter}')")

            count = connection.execute_scalar_query(
                query=f"SELECT COUNT(*) FROM {table_for_import.table_name}")
            return count


def generate_hyper_rows(list_rows,
                        hyper_path_with_file_name,
                        table_for_import):
    path_to_database = hyper_path_with_file_name
    process_parameters = {
        # Limits the number of Hyper event log files to two
        "log_file_max_count": "2",
        "log_file_size_limit": "100M"
    }

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
        connection_parameters = {"lc_time": "pt_Br"}

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        parameters=connection_parameters) as connection:
            connection.catalog.create_table(table_definition=table_for_import)

            with Inserter(connection, table_for_import) as inserter:
                for row in list_rows:
                    inserter.add_row(row)
                inserter.execute()

            row_count = connection.execute_scalar_query(
                query=f"SELECT COUNT(*) FROM {table_for_import.table_name}"
            )

    return row_count


def send_data_server(server_address, site_name, token_name, hyper_name, project_name):
    if server_address:
        with no_ssl_verification():
            tableau_auth = TSC.PersonalAccessTokenAuth(token_name,
                                                       personal_access_token=token_value,
                                                       site_id=site_name)
            server = TSC.Server(server_address, use_server_version=True)
            with server.auth.sign_in(tableau_auth):

                publish_mode = TSC.Server.PublishMode.Overwrite

                for project in TSC.Pager(server.projects):
                    if project.name == project_name:
                        project_id = project.id

                # Create the datasource object with the project_id
                datasource = TSC.DatasourceItem(project_id)
                datasource = server.datasources.publish(datasource, hyper_name, publish_mode)
                print("Datasource published. Datasource id: {0}".format(datasource.id))

    return server
