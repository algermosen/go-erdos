package apperrors

type ErrCode int

const (
	// General errors
	ErrUnknown           ErrCode = iota // An unspecified error.
	ErrInvalidInput                     // The input provided to a function is not valid.
	ErrConfigNotFound                   // The configuration file or settings could not be located.
	ErrUnsupportedOption                // A provided flag or option is not supported.
	ErrOperationTimeout                 // An operation took too long and timed out.

	// Database related errors
	ErrDBConnection        // Failed to connect to the database.
	ErrDBQuery             // An error occurred while executing a database query.
	ErrSchemaDump          // An error occurred while dumping the schema.
	ErrDataDump            // An error occurred while dumping the data.
	ErrConstraintDump      // An error occurred while dumping constraints (foreign keys, indexes, etc.).
	ErrTransaction         // An error occurred during a database transaction.
	ErrUnsupportedDatabase // The specified database type is not supported.

	// You can extend with more specific errors as needed.
	ErrFileWrite         // Error encountered while writing to a file.
	ErrFileRead          // Error encountered while reading from a file.
	ErrCLIParsing        // Error parsing command-line arguments.
	ErrConcurrency       // An error related to concurrent processing (e.g., goroutine synchronization issues).
	ErrResourceExhausted // Insufficient system resources (e.g., memory, file handles) to complete an operation.
	ErrMigrateProcess    // An error during the migration process (e.g., script conversion issues).
)
