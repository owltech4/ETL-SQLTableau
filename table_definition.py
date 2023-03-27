from tableauhyperapi import TableDefinition, SqlType, NULLABLE, NOT_NULLABLE, TableName

TESTE_table = TableDefinition(
    table_name="TESTE",
    columns=[
        TableDefinition.Column("NOME CLIENTE", SqlType.text(), NULLABLE),
        TableDefinition.Column("DATA DA COMPRA", SqlType.text(), NULLABLE),
        TableDefinition.Column("IDADE PESSOA", SqlType.text(), NULLABLE),
        TableDefinition.Column("QUANTIDADE DE PRODUTOS", SqlType.text(), NULLABLE),
        TableDefinition.Column("SITUACAO DO CLIENTE", SqlType.text(), NULLABLE),
    ]
)

