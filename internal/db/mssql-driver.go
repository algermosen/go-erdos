package db

import (
	"database/sql"
	"fmt"
	"strings"

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

	sortedTables, err := sortTablesByDependencies(deps)
	if err != nil {
		return "", fmt.Errorf("MSSQL error sorting dependencies: %w", err)
	}

	mappings, err := m.getTableMappings(db)
	if err != nil {
		return "", fmt.Errorf("MSSQL error fetching mappings: %w", err)
	}

	var builder strings.Builder
	for _, table := range sortedTables {
		stm, err := m.assembleCreateStatements(tableMapping{table: mappings[table]})
		if err != nil {
			return "", fmt.Errorf("MSSQL error assembling statement of [%s]: %w", table, err)
		}
		builder.WriteString(stm)
	}

	return builder.String(), nil
}

// DumpData returns a placeholder string for the data dump.
// You would typically iterate over tables and generate INSERT statements for each row.
func (m *MSSQLDriver) DumpData(db *sql.DB) (string, error) {
	// Placeholder: Replace with actual data dumping logic.
	return "-- MSSQL Data Dump Placeholder", nil
}

// DumpConstraints returns a placeholder string for the constraints dump.
// In a real implementation, you might query INFORMATION_SCHEMA for keys, indexes, etc.
func (m *MSSQLDriver) DumpConstraints(db *sql.DB) (string, error) {
	// Placeholder: Replace with actual constraints extraction logic.
	return "-- MSSQL Constraints Dump Placeholder", nil
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

type tableMapping map[string][]columnDef

func (m *MSSQLDriver) getTableMappings(db *sql.DB) (tableMapping, error) {
	query := mssqlQueryTableMappings

	rows, err := db.Query(query)
	if err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error fetching table structures", err)
	}
	defer rows.Close()

	tableMap := make(tableMapping)
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
		key := FormatObjectName(cd.schema, cd.table)
		tableMap[key] = append(tableMap[key], cd)
	}

	if err := rows.Err(); err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error iterating table structures", err)
	}

	return tableMap, nil
}

func (m *MSSQLDriver) assembleCreateStatements(tm tableMapping) (string, error) {
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
		if err := rows.Scan(&childSchema, &child, &parentSchema, &parent); err != nil {

			return nil, apperrors.New(apperrors.ErrDBQuery, "error scanning dependency row", err)
		}

		childName := FormatObjectName(childSchema, child)
		parentName := FormatObjectName(parentSchema, parent)
		dependencies[childName] = append(dependencies[childName], parentName)

		if _, exist := dependencies[parentName]; !exist {
			dependencies[parentName] = []string{}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, apperrors.New(apperrors.ErrDBQuery, "error iterating dependency rows", err)
	}

	return dependencies, nil
}

func sortTablesByDependencies(deps DependencyTree) ([]string, error) {
	tableDegree := make(map[string]int) // number of dependent tables

	for table, parents := range deps {
		tableDegree[table] = len(parents)
	}

	var queue []string
	for table, deg := range tableDegree {
		if deg == 0 {
			queue = append(queue, table)
		}
	}

	var sorted []string
	for len(queue) > 0 {
		table := queue[0]
		queue = queue[1:]
		sorted = append(sorted, table)

		for child, parents := range deps {

			if slices.Contains(parents, table) {
				tableDegree[child]--
				if tableDegree[child] == 0 {
					queue = append(queue, child)
				}
			}
		}
	}

	// Check if we processed all tables.
	if len(sorted) != len(deps) {
		return nil, apperrors.New(apperrors.ErrMigrateProcess, "cyclic dependency or incomplete dependency graph detected", nil)
	}

	return sorted, nil
}

func validateSkipList(deps DependencyTree, skipList []string) error {
	for table, parents := range deps {
		for _, parent := range parents {
			if slices.Contains(skipList, parent) {
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
