from Autenticador import get_user_pass

username_sql, password_sql = get_user_pass(tool='sql')

sqlserver_conf = {
    'host': 'SPPRC011',
    'database': 'DB00',
    'username': username_sql,
    'password': password_sql,
    'port': '1433',

    'query_row_list_TESTE_pagination': '''
    SELECT NOM_CLIENTE,
    DAT_COMPRA,
    [IDADE_PESSOA],
    QTD_PRODUTOS,
    NOM_TIPO from #temp_Teste r where r.rownum between {rowInitial} and {rowFinal}
    '''

}

execute_etl_TESTE_sql = {
    'insert_table_temporary': '''SELECT x.*, rownum=row_number() over (order by x.NOM_CLIENTE) into #temp_Teste from(
        SELECT DISTINCT
            A.NOM_CLIENTE
            , DATEDIFF (YEAR, A.DAT_NAS_PERS, CURRENT_TIMESTAMP) AS IDADE_PESSOA
            , A.QTD_PRODUTOS
            , SIT.NOM_TIPO
            
            FROM TB_D_PESSOA AS A
            
        LEFT JOIN TB_D_TIPO_SITUACAO (NOLOCK) SIT
            ON A.COD_TIPO_SITUACAO = SIT.COD_TIPO_SITUACAO
        
        GROUP BY
            A.NOM_CLIENTE
            , A.QTD_PRODUTOS
            , SIT.NOM_TIPO
) as X

'''
}

tableau_parameter = {

    'server_address': 'https://tableau.com',
    'site_name': 'Corporativo',
    'token_name': 'owltech',
    'token_value': 'ABCDEFG'

}

