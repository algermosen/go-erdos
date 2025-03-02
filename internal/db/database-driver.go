package db

import "database/sql"

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
