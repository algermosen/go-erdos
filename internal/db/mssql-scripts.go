package db

import "fmt"

// SQL query constants.
const (
	mssqlQueryTableMappings = `
SELECT 
    s.name AS [schema],
    t.name AS [table],
    c.name AS [column],
    c.column_id AS [column_position],
    tp.name AS [data_type],
    c.max_length AS [max_length],
    c.precision,
    c.scale,
    c.is_nullable AS [is_nullable],
    c.is_identity AS [is_identity],
    c.is_computed AS [is_computed]
FROM 
    sys.tables t
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
JOIN 
    sys.columns c ON t.object_id = c.object_id
JOIN 
    sys.types tp ON c.user_type_id = tp.user_type_id
WHERE 
    t.type = 'U'
`

	mssqlqQeryAnalyzeDependencies = `
	SELECT DISTINCT
        fk.TABLE_SCHEMA AS ChildSchema,
        fk.TABLE_NAME AS ChildTable,
        pk.TABLE_SCHEMA AS ParentSchema, 
        pk.TABLE_NAME AS ParentTable 
    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
    FULL JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME 
    FULL JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
    WHERE pk.TABLE_NAME IS NOT NULL
    ORDER BY fk.TABLE_NAME ASC;
	`

	tableListQuery = `
	SELECT 
		TABLE_SCHEMA,
		TABLE_NAME 
	FROM 
		INFORMATION_SCHEMA.TABLES 
	WHERE 
		TABLE_TYPE = 'BASE TABLE' 
		AND TABLE_CATALOG = DB_NAME();
	`
)

func GetCreateSchemaQuery(schemaName string) string {
	return fmt.Sprintf(`
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '%s')
BEGIN
    EXEC('CREATE SCHEMA %s')
END
`, schemaName, schemaName)
}
