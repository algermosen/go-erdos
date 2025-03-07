package db

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// DatabaseDriver defines the interface for all database drivers.
// This interface abstracts the operations needed for the migration process.
type DatabaseDriver interface {
	// Connect opens a connection to the database using the provided connection string.
	Connect(connectionString string) (*sql.DB, error)

	// DumpSchema returns the SQL statements for creating the database schema.
	DumpSchema(db *sql.DB) (string, error)

	// DumpData returns the SQL statements for inserting the database data.
	DumpData(db *sql.DB) (string, error)

	// DumpConstraints returns the SQL statements for recreating constraints such as primary keys, foreign keys, etc.
	DumpConstraints(db *sql.DB) (string, error)
}

type DependencyTree map[TableName][]TableName
type TableMapping map[TableName][]columnDef

type TableName string

func NewTableName(schema, table string) TableName {
	if schema == "" {
		schema = "dbo"
	}
	return TableName(FormatObjectName(schema, table))
}

func (t TableName) String() string {
	return string(t)
}

func (t TableName) GetParts() (string, string) {
	// Define a regex that matches strings like "[schema].[table]"
	re := regexp.MustCompile(`^\[([^]]+)\]\.\[([^]]+)\]$`) // ([^]]+) matches any character except ']'
	matches := re.FindStringSubmatch(t.String())
	if len(matches) == 3 {
		return matches[1], matches[2]
	}
	return "", ""
}

func (t TableName) IsEmpty() bool {
	_, table := t.GetParts()
	return strings.TrimSpace(table) == ""
}

func FormatObjectName(parts ...string) string {
	var formatted []string
	for _, part := range parts {
		formatted = append(formatted, fmt.Sprintf("[%s]", part))
	}
	return strings.Join(formatted, ".")
}
