package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/algermosen/go-erdos/internal/apperrors"
	"github.com/algermosen/go-erdos/internal/db"
	"github.com/algermosen/go-erdos/util"
	"github.com/spf13/cobra"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dumps the database schema and/or data",
	Long: `This command allows dumping a database's schema, content, or data. 
Supported database types: PostgreSQL, SQLite, and MSSQL.

Options:
- "all" (default): Dumps both schema and data.
- "content": Dumps only the schema (table structures, constraints).
- "data": Dumps only the data (INSERT statements).`,
	Run: func(cmd *cobra.Command, args []string) {
		// Retrieve flag values
		connStr, _ := cmd.Flags().GetString("conn")
		dbType, _ := cmd.Flags().GetString("dbtype")
		include, _ := cmd.Flags().GetString("include")
		skip, _ := cmd.Flags().GetString("skip")
		skipData, _ := cmd.Flags().GetString("skip-data")
		outputFile, _ := cmd.Flags().GetString("output")

		// Validate required parameters
		if util.IsEmpty(connStr) {
			appLogger.Error(apperrors.New(apperrors.ErrInvalidInput, "--conn flag is required", nil))
			os.Exit(1)
		}

		// Process the skip tables list
		skipTables := util.SplitAndTrim(skip, ",")
		skipDataTables := util.SplitAndTrim(skipData, ",")

		fmt.Println("Starting database dump with the following parameters:")
		fmt.Println(" - Connection String:", connStr)
		fmt.Println(" - Database Type:", dbType)
		fmt.Println(" - Include:", include)
		fmt.Println(" - Skip Tables:", skipTables)
		fmt.Println(" - Skip Data From:", skipDataTables)
		fmt.Println(" - Output File:", outputFile)

		options := dumpOptions{
			connStr:        connStr,
			dbType:         dbType,
			include:        include,
			outputFile:     outputFile,
			skipTables:     skipTables,
			skipDataTables: skipDataTables,
		}

		handleDump(options)
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)

	// Define flags
	dumpCmd.Flags().String("include", "all", "What to include in the dump (options: all, content, data) (default: all)")
	dumpCmd.Flags().String("skip", "", "Comma-separated list of objects/tables to ignore")
	dumpCmd.Flags().String("skip-data", "", "Comma-separated list of objects/tables which data need to be ignored")
	dumpCmd.Flags().String("output", "./output/dump.sql", "File to save the database dump (default: dump.sql)")
}

func handleDump(options dumpOptions) error {
	switch options.dbType {
	case "postgres":
		return dumpPostgres(options)
	case "sqlite":
		return dumpSQLite(options)
	case "mssql":
		return dumpMSSQL(options)
	default:
		msg := fmt.Sprintf("unsupported database type '%s'", options.dbType)
		return apperrors.New(apperrors.ErrInvalidInput, msg, nil)
	}
}

// Placeholder function for PostgreSQL dumping
func dumpPostgres(options dumpOptions) error {
	log.Println("Dumping PostgreSQL database...")
	// Implement logic using pg_dump or go-pg
	return nil
}

// Placeholder function for SQLite dumping
func dumpSQLite(options dumpOptions) error {
	log.Println("Dumping SQLite database...")
	// Implement logic using native SQLite backup
	return nil
}

// Placeholder function for MSSQL dumping
func dumpMSSQL(options dumpOptions) error {
	log.Println("[Dumping MSSQL database]")
	driver := db.MSSQLDriver{}
	db, err := driver.Connect(options.connStr)
	if err != nil {
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	defer db.Close()
	log.Println("[Database connected]")
	var dump strings.Builder

	schema, err := driver.DumpSchema(db)
	if err != nil {
		log.Fatalf("Failed to retrieve tables: %v", err)
	}
	dump.WriteString(schema + "\nGO;\n\n")

	data, err := driver.DumpData(db, options.skipDataTables)
	if err != nil {
		log.Fatalf("Failed to retrieve tables: %v", err)
	}
	dump.WriteString(data + "\nGO;\n\n")

	constraints, err := driver.DumpConstraints(db)
	if err != nil {
		log.Fatalf("Failed to retrieve tables: %v", err)
	}
	dump.WriteString(constraints + "\nGO;\n\n")

	file, err := os.OpenFile(options.outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("Failed to open (or create) schema dump file: %v", err)
	}
	defer file.Close()

	_, err = file.Write([]byte(dump.String()))
	if err != nil {
		log.Fatalf("Failed to write dump file: %v", err)
	}
	log.Printf("[Dump written to %s]", options.outputFile)

	return nil
}

type dumpOptions struct {
	connStr, dbType, include, outputFile string
	skipTables, skipDataTables           []string
}
