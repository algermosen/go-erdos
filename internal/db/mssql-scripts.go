package db

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
	SELECT 
		fk.TABLE_SCHEMA AS ChildSchema,
		fk.TABLE_NAME AS ChildTable,
		pk.TABLE_SCHEMA AS ParentSchema, 
		pk.TABLE_NAME AS ParentTable 
	FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
	INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME 
	INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME;
	`
)
