package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/algermosen/go-erdos/internal/apperrors"
	"github.com/algermosen/go-erdos/util"
)

// MSSQLDriver implements the DatabaseDriver interface for Microsoft SQL Server.
type MSSQLDriver struct{}

// NewMSSQLDriver creates a new instance of MSSQLDriver.
func NewMSSQLDriver() *MSSQLDriver {
	return &MSSQLDriver{}
}

// Connect establishes a connection to the MSSQL database.
func (m *MSSQLDriver) Connect(connectionString string) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		// Use our custom error type with ErrDBConnection error code.
		return nil, apperrors.New(apperrors.ErrDBConnection, "failed to connect to MSSQL", err)
	}

	// Verify the connection with a ping.
	if err := db.Ping(); err != nil {
		return nil, apperrors.New(apperrors.ErrDBConnection, "MSSQL ping failed", err)
	}
	return db, nil
}

// DumpSchema returns a placeholder string for the schema dump.
// In a real implementation, this would query system views like INFORMATION_SCHEMA.TABLES, etc.
func (m *MSSQLDriver) DumpSchema(db *sql.DB) (string, error) {
	// Placeholder: Replace with actual schema extraction logic.
	deps, err := m.analyzeDependencies(db)
	if err != nil {
		return "", fmt.Errorf("MSSQL error analyzing dependencies: %w", err)
	}

	rows, err := db.Query(tableListQuery)
	if err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "failed to query table list", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return "", apperrors.New(apperrors.ErrDBQuery, "failed to scan table list", err)
		}
		fullTableName := NewTableName(schema, table)
		if _, exists := deps[fullTableName]; !exists {
			deps[fullTableName] = make([]TableName, 0)
		}
	}
	if err := rows.Err(); err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "error iterating table list", err)
	}

	sortedTables, err := sortTablesByDependencies(deps)
	if err != nil {
		return "", fmt.Errorf("MSSQL error sorting dependencies: %w", err)
	}

	mappings, err := m.getTableMappings(db)
	if err != nil {
		return "", fmt.Errorf("MSSQL error fetching mappings: %w", err)
	}

	var builder strings.Builder
	var schemas = []string{"dbo", "sys", "INFORMATION_SCHEMA"}
	for i, table := range sortedTables {
		fmt.Printf("\033[1A\033[K[Dumping schemas (%d/%d)]\n", i+1, len(sortedTables))
		schema, _ := table.GetParts()
		if !slices.Contains(schemas, schema) {
			builder.WriteString(GetCreateSchemaQuery(schema))
			schemas = append(schemas, schema)
		}
		stm, err := m.assembleCreateStatements(TableMapping{table: mappings[table]})
		if err != nil {
			return "", fmt.Errorf("MSSQL error assembling statement of [%s]: %w", table, err)
		}
		builder.WriteString(stm)
	}

	fmt.Println()
	return builder.String(), nil
}

// DumpData returns a placeholder string for the data dump.
// You would typically iterate over tables and generate INSERT statements for each row.
func (m *MSSQLDriver) DumpData(db *sql.DB, skip []string) (string, error) {
	// Query to get the list of tables with their schema.
	// getting this list is also used in the schema dump. Consider refactoring to avoid duplication.
	rows, err := db.Query(tableListQuery)
	if err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "failed to query table list", err)
	}
	defer rows.Close()

	var tables []TableName
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return "", apperrors.New(apperrors.ErrDBQuery, "failed to scan table list", err)
		}
		fullTableName := NewTableName(schema, table)
		tables = append(tables, fullTableName)
	}
	if err := rows.Err(); err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "error iterating table list", err)
	}

	mappings, err := m.getTableMappings(db)
	if err != nil {
		return "", fmt.Errorf("MSSQL error fetching mappings: %w", err)
	}

	progressCh := make(chan int, len(tables))
	errChan := make(chan error, len(tables))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var result strings.Builder

	// Progress updater goroutine.
	go func(total int) {
		processed := 0
		for p := range progressCh {
			processed += p
			// Clear the previous line and print updated progress.
			fmt.Printf("\033[1A\033[K[Dumping data (%d/%d)]\n", processed, total)
		}
	}(len(tables))

	// Dump each table concurrently with a 1-minute timeout per table.
	for _, table := range tables {
		wg.Add(1)
		go func(tbl TableName) {
			defer wg.Done()
			// Create a new context for this cycle with a 1-minute timeout.
			ctxCycle, cancelCycle := context.WithTimeout(context.Background(), time.Minute)
			defer cancelCycle()

			_, tableName := tbl.GetParts()
			if slices.Contains(skip, tableName) {
				progressCh <- 1
				return
			}

			// Assuming you update dumpTableData to accept a context:
			dump, err := m.dumpTableData(ctxCycle, db, tbl.String(), mappings[tbl])
			if err != nil {
				errChan <- err
				return
			}
			mu.Lock()
			result.WriteString(dump)
			mu.Unlock()
			progressCh <- 1
		}(table)
	}

	wg.Wait()
	close(progressCh)
	close(errChan)
	if err, ok := <-errChan; ok {
		return "", err
	}

	fmt.Println()

	return result.String(), nil
}

type insertBuffer []string

func (b *insertBuffer) flush() string {
	// Return empty string if there's nothing to flush.
	if b == nil || len(*b) == 0 {
		return ""
	}
	// Join the buffered values.
	result := strings.Join(*b, ",\n") + ";\n"
	// Reset the underlying slice.
	*b = (*b)[:0]
	return result
}

// dumpTableData generates INSERT statements for all rows of a single table.
func (m *MSSQLDriver) dumpTableData(ctx context.Context, db *sql.DB, table string, colInfo []columnDef) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", apperrors.New(apperrors.ErrDataDump, fmt.Sprintf("failed to query data for table %s", table), err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return "", apperrors.New(apperrors.ErrDataDump, fmt.Sprintf("failed to get columns for table %s", table), err)
	}

	var builder, insertStmtBuilder strings.Builder
	builder.WriteString(fmt.Sprintf("-- Data dump for table: %s\n", table))
	// Build column list (formatted with square brackets)
	var colNames []string
	for _, col := range columns {
		colNames = append(colNames, FormatObjectName(col))
	}
	colList := strings.Join(colNames, ", ")
	batch := 50
	batchCount := batch
	insertHead := fmt.Sprintf("INSERT INTO %s (%s) VALUES \n", table, colList)
	// Process each row
	insertValues := make(insertBuffer, 0, batch)
	for rows.Next() {
		// Optional: check for context cancellation
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		if batchCount == batch {
			insertStmtBuilder.WriteString(insertHead)
		}
		// Prepare a slice for the row values.
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return "", apperrors.New(apperrors.ErrDataDump, fmt.Sprintf("failed to scan row for table %s", table), err)
		}

		// Format each value appropriately.
		var valueStrs []string
		for i, val := range values {
			// Check if the current column (by index) is a geography type.
			if len(colInfo) > i && strings.EqualFold(colInfo[i].dataType, "geography") {
				// TODO: Implement geography type handling.
				valueStrs = append(valueStrs, "NULL")
				// // Expecting v to be []byte for geography. Convert to hex.
				// b, ok := val.([]byte)
				// if !ok {
				// 	// Fallback to a NULL if conversion fails.
				// 	valueStrs = append(valueStrs, "NULL")
				// 	continue
				// }
				// hexVal := fmt.Sprintf("%X", b)
				// // Use SQL Server's geography::STGeomFromWKB function.
				// valueStrs = append(valueStrs, fmt.Sprintf("geography::STGeomFromWKB(0x%s,4326)", hexVal))
				continue
			}
			// Normal conversion for other types.
			if val == nil {
				valueStrs = append(valueStrs, "NULL")
			} else {
				switch v := val.(type) {
				case []byte:
					// Convert []byte to string, escape single quotes.
					str := strings.ReplaceAll(string(v), "'", "''")
					valueStrs = append(valueStrs, fmt.Sprintf("'%s'", str))
				case string:
					escaped := strings.ReplaceAll(v, "'", "''")
					valueStrs = append(valueStrs, fmt.Sprintf("'%s'", escaped))
				case time.Time:
					formattedTime := v.Format("2006-01-02 15:04:05")
					valueStrs = append(valueStrs, fmt.Sprintf("'%s'", formattedTime))
				case bool:
					if v {
						valueStrs = append(valueStrs, "1")
					} else {
						valueStrs = append(valueStrs, "0")
					}
				default:
					valueStrs = append(valueStrs, fmt.Sprint(v))
				}
			}
		}

		// Build the INSERT statement.
		batchCount--
		insertValues = append(insertValues, fmt.Sprintf("(%s)", strings.Join(valueStrs, ", ")))

		if batchCount <= 0 {
			insertStmt := insertValues.flush()
			batchCount = batch
			insertStmtBuilder.WriteString(insertStmt)
		}
	}

	if err := rows.Err(); err != nil {
		return "", apperrors.New(apperrors.ErrDataDump, fmt.Sprintf("error iterating rows for table %s", table), err)
	}

	if len(insertValues) > 0 {
		insertStmt := insertValues.flush()
		insertStmtBuilder.WriteString(insertStmt)
	}

	isIdentity := false
	for _, col := range colInfo {
		if col.isIdentity {
			isIdentity = true
			break
		}
	}

	if isIdentity {
		builder.WriteString(fmt.Sprintf("SET IDENTITY_INSERT %s ON;\n", table))
		builder.WriteString(insertStmtBuilder.String())
		builder.WriteString(fmt.Sprintf("SET IDENTITY_INSERT %s OFF;\n", table))
	} else {
		builder.WriteString(insertStmtBuilder.String())
	}

	// Separate dumps for readability.
	builder.WriteString("\nGO;\n\n")
	return builder.String(), nil
}

// DumpConstraints returns a placeholder string for the constraints dump.
// In a real implementation, you might query INFORMATION_SCHEMA for keys, indexes, etc.
func (m *MSSQLDriver) DumpConstraints(db *sql.DB) (string, error) {
	var builder strings.Builder
	builder.WriteString("-- Constraints Dump\n\n")

	// --- Primary Keys ---
	const queryPrimaryKeys = `
SELECT 
    tc.TABLE_SCHEMA,
    tc.TABLE_NAME,
    tc.CONSTRAINT_NAME,
    kcu.COLUMN_NAME,
    kcu.ORDINAL_POSITION
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu 
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;
`
	rows, err := db.Query(queryPrimaryKeys)
	if err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "error fetching primary key constraints", err)
	}
	defer rows.Close()

	// Group primary key columns by a composite key: schema.table.constraint.
	type primaryKeyInfo struct {
		schema         string
		table          string
		constraintName string
		columns        []string
	}
	pkMap := make(map[string]*primaryKeyInfo)
	for rows.Next() {
		var schema, table, constraintName, column string
		var ordinal int // not used directly but needed for ordering
		if err := rows.Scan(&schema, &table, &constraintName, &column, &ordinal); err != nil {
			return "", apperrors.New(apperrors.ErrDBQuery, "error scanning primary key row", err)
		}
		key := fmt.Sprintf("%s.%s.%s", schema, table, constraintName)
		if pk, exists := pkMap[key]; exists {
			pk.columns = append(pk.columns, column)
		} else {
			pkMap[key] = &primaryKeyInfo{
				schema:         schema,
				table:          table,
				constraintName: constraintName,
				columns:        []string{column},
			}
		}
	}
	if err := rows.Err(); err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "error iterating primary key rows", err)
	}

	// Build primary key ALTER statements.
	counter := 0
	for _, pk := range pkMap {
		counter++
		fmt.Printf("\033[1A\033[K[Dumping PKs (%d/%d)]\n", counter, len(pkMap))
		fullTableName := FormatObjectName(pk.schema, pk.table)
		// Use the constraint name as provided.
		constraintName := FormatObjectName(pk.constraintName)
		var colNames []string
		for _, col := range pk.columns {
			colNames = append(colNames, FormatObjectName(col))
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);\n",
			fullTableName, constraintName, strings.Join(colNames, ", "))
		builder.WriteString(stmt)
	}

	println()
	builder.WriteString("\n")

	// --- Foreign Keys ---
	// This query retrieves foreign key details including column-level information.
	const queryForeignKeys = `
SELECT 
    fk.TABLE_SCHEMA AS ChildSchema,
    fk.TABLE_NAME AS ChildTable,
    fk.CONSTRAINT_NAME AS ForeignKey,
    pk.TABLE_SCHEMA AS ParentSchema, 
    pk.TABLE_NAME AS ParentTable,
    fkc.COLUMN_NAME AS ChildColumn,
    pkc.COLUMN_NAME AS ParentColumn,
    rc.UPDATE_RULE,
    rc.DELETE_RULE,
    fkc.ORDINAL_POSITION
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkc ON fk.CONSTRAINT_NAME = fkc.CONSTRAINT_NAME
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkc ON pk.CONSTRAINT_NAME = pkc.CONSTRAINT_NAME 
    AND fkc.ORDINAL_POSITION = pkc.ORDINAL_POSITION
ORDER BY fk.TABLE_SCHEMA, fk.TABLE_NAME, fk.CONSTRAINT_NAME, fkc.ORDINAL_POSITION;
`
	fkRows, err := db.Query(queryForeignKeys)
	if err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "error fetching foreign key constraints", err)
	}
	defer fkRows.Close()

	type foreignKeyInfo struct {
		childSchema    string
		childTable     string
		constraintName string
		parentSchema   string
		parentTable    string
		childColumns   []string
		parentColumns  []string
		updateRule     string
		deleteRule     string
	}
	fkMap := make(map[string]*foreignKeyInfo)
	for fkRows.Next() {
		var childSchema, childTable, constraintName, parentSchema, parentTable, childColumn, parentColumn, updateRule, deleteRule string
		var ordinal int
		if err := fkRows.Scan(&childSchema, &childTable, &constraintName, &parentSchema, &parentTable, &childColumn, &parentColumn, &updateRule, &deleteRule, &ordinal); err != nil {
			return "", apperrors.New(apperrors.ErrDBQuery, "error scanning foreign key row", err)
		}
		key := fmt.Sprintf("%s.%s.%s", childSchema, childTable, constraintName)
		if fk, exists := fkMap[key]; exists {
			fk.childColumns = append(fk.childColumns, childColumn)
			fk.parentColumns = append(fk.parentColumns, parentColumn)
		} else {
			fkMap[key] = &foreignKeyInfo{
				childSchema:    childSchema,
				childTable:     childTable,
				constraintName: constraintName,
				parentSchema:   parentSchema,
				parentTable:    parentTable,
				childColumns:   []string{childColumn},
				parentColumns:  []string{parentColumn},
				updateRule:     updateRule,
				deleteRule:     deleteRule,
			}
		}
	}
	if err := fkRows.Err(); err != nil {
		return "", apperrors.New(apperrors.ErrDBQuery, "error iterating foreign key rows", err)
	}

	// Build foreign key ALTER statements.
	counter = 0
	for _, fk := range fkMap {
		counter++
		fmt.Printf("\033[1A\033[K[Dumping FKs (%d/%d)]\n", counter, len(fkMap))
		childTableName := FormatObjectName(fk.childSchema, fk.childTable)
		parentTableName := FormatObjectName(fk.parentSchema, fk.parentTable)
		constraintName := FormatObjectName(fk.constraintName)
		var childCols, parentCols []string
		for _, col := range fk.childColumns {
			childCols = append(childCols, FormatObjectName(col))
		}
		for _, col := range fk.parentColumns {
			parentCols = append(parentCols, FormatObjectName(col))
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE %s ON DELETE %s;\n",
			childTableName,
			constraintName,
			strings.Join(childCols, ", "),
			parentTableName,
			strings.Join(parentCols, ", "),
			fk.updateRule,
			fk.deleteRule,
		)
		builder.WriteString(stmt)
	}

	println()
	return builder.String(), nil
}

type columnDef struct {
	schema         string
	table          string
	columnName     string
	columnPosition int
	dataType       string
	maxLength      int
	precision      int
	scale          int
	isNullable     bool
	isIdentity     bool
	isComputed     bool
}

func (m *MSSQLDriver) getTableMappings(db *sql.DB) (TableMapping, error) {
	query := mssqlQueryTableMappings

	rows, err := db.Query(query)
	if err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error fetching table structures", err)
	}
	defer rows.Close()

	tableMap := make(TableMapping)
	for rows.Next() {
		var cd columnDef

		err := rows.Scan(
			&cd.schema,
			&cd.table,
			&cd.columnName,
			&cd.columnPosition,
			&cd.dataType,
			&cd.maxLength,
			&cd.precision,
			&cd.scale,
			&cd.isNullable,
			&cd.isIdentity,
			&cd.isComputed,
		)
		if err != nil {
			return nil, apperrors.New(apperrors.ErrDBQuery, "error scanning table structures", err)
		}
		key := NewTableName(cd.schema, cd.table)
		tableMap[key] = append(tableMap[key], cd)
	}

	if err := rows.Err(); err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error iterating table structures", err)
	}

	return tableMap, nil
}

func (m *MSSQLDriver) assembleCreateStatements(tm TableMapping) (string, error) {
	var builder strings.Builder
	for key, columns := range tm {
		builder.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", key))

		for i, col := range columns {
			builder.WriteString(util.TabSpace)

			colDef := m.buildColumnDefinition(col)

			if i < len(columns)-1 {
				colDef += ","
			}
			builder.WriteString(colDef + "\n")
		}
		builder.WriteString(");\n\n")
	}
	return builder.String(), nil
}

func (m *MSSQLDriver) buildColumnDefinition(cd columnDef) string {
	colDef := fmt.Sprintf("[%s] %s", cd.columnName, m.formatColumnType(cd))
	if !cd.isNullable {
		colDef += " NOT NULL"
	}
	if cd.isIdentity {
		colDef += " IDENTITY(1,1)"
	}
	return colDef
}

func (m *MSSQLDriver) analyzeDependencies(db *sql.DB) (DependencyTree, error) {
	query := mssqlqQeryAnalyzeDependencies

	rows, err := db.Query(query)
	if err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error fetching database dependencies", err)
	}
	defer rows.Close()

	dependencies := make(DependencyTree)
	for rows.Next() {
		var childSchema, child, parentSchema, parent string
		var childSchemaNull, childNull sql.NullString
		if err := rows.Scan(&childSchemaNull, &childNull, &parentSchema, &parent); err != nil {
			return nil, apperrors.New(apperrors.ErrDBQuery, "error scanning dependency row", err)
		}

		// Handle NULL values.
		if childSchemaNull.Valid {
			childSchema = childSchemaNull.String
		}
		if childNull.Valid {
			child = childNull.String
		}

		childName := NewTableName(childSchema, child)
		parentName := NewTableName(parentSchema, parent)

		if !childName.IsEmpty() {
			dependencies[childName] = append(dependencies[childName], parentName)
		}

		dependencies[parentName] = make([]TableName, 0)
	}

	if err := rows.Err(); err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error iterating dependency rows", err)
	}

	return dependencies, nil
}

func sortTablesByDependencies(deps DependencyTree) ([]TableName, error) {
	tableDegree := make(map[TableName]int) // number of dependent tables

	for table, parents := range deps {
		tableDegree[table] = len(parents)
	}

	var queue []TableName
	for table, deg := range tableDegree {
		if deg == 0 {
			queue = append(queue, table)
		}
	}

	var sorted []TableName
	totalLenght := len(deps)
	for len(queue) > 0 {
		table := queue[0]
		queue = queue[1:]
		sorted = append(sorted, table)

		delete(deps, table)

		for child, parents := range deps {
			if table.String() == NewTableName("", "ExceptionLogs").String() {
				// fmt.Printf("Queue(%d) Child: %s | Parents: %v\n", len(queue), child, parents)
			}

			if slices.Contains(parents, table) {

				tableDegree[child]--
				if tableDegree[child] == 0 {
					queue = append(queue, child)
				}
			}
		}
	}

	// Check if we processed all tables.
	if len(sorted) != totalLenght {
		return nil, apperrors.New(apperrors.ErrMigrateProcess, "cyclic dependency or incomplete dependency graph detected", nil)
	}

	return sorted, nil
}

func validateSkipList(deps DependencyTree, skipList []string) error {
	for table, parents := range deps {
		for _, parent := range parents {
			if slices.Contains(skipList, parent.String()) {
				msg := fmt.Sprintf("cannot skip table %s because it is referenced by table %s", parent, table)
				return apperrors.New(apperrors.ErrMigrateProcess, msg, nil)
			}
		}
	}
	return nil
}

func (m *MSSQLDriver) formatColumnType(cd columnDef) string {
	dt := strings.ToLower(cd.dataType)
	// Example handling for character types; you can extend this logic.
	switch dt {
	case "char", "varchar", "nchar", "nvarchar":
		if cd.maxLength > 0 {
			// For 'nchar' and 'nvarchar', max_length is in bytes (2 bytes per character), so adjust if needed.
			return fmt.Sprintf("%s(%d)", cd.dataType, cd.maxLength)
		}
		return cd.dataType + "(max)"
	default:
		// For numeric or other types, you might want to include precision and scale.
		if dt == "decimal" || dt == "numeric" {
			return fmt.Sprintf("%s(%d,%d)", cd.dataType, cd.precision, cd.scale)
		}
		return cd.dataType
	}
}

// fmt.Print("\033[1A\033[K") // moves up and then deletes the line
