SELECT 
    s.name AS 'schema',
    t.name AS 'table',
    t.object_id AS 'table_id',
    c.name AS 'column',
    c.column_id AS 'column_position',
    tp.name AS 'data_type',
    c.max_length AS 'max_length',
    c.precision,
    c.scale,
    c.is_nullable AS 'is_nullable',
    c.is_identity AS 'is_identity',
    c.is_computed AS 'is_computed'
FROM 
    sys.tables t
JOIN 
    sys.schemas s ON t.schema_id = s.schema_id
JOIN 
    sys.columns c ON t.object_id = c.object_id
JOIN 
    sys.types tp ON c.user_type_id = tp.user_type_id
WHERE 
    t.type = 'U' -- User-defined tables