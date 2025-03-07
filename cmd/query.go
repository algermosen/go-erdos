package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/algermosen/go-erdos/internal/db"
	"github.com/spf13/cobra"
)

// queryCmd represents the query command.
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Executes a SQL query from a file against a database",
	Long:  "Executes a SQL query from a file against a specified database. Currently supports MSSQL.",
	Run: func(cmd *cobra.Command, args []string) {
		// Retrieve flag values.
		connStr, _ := cmd.Flags().GetString("conn")
		dbType, _ := cmd.Flags().GetString("dbtype")
		queryFile, _ := cmd.Flags().GetString("query-file")

		// Validate required flags.
		if connStr == "" {
			log.Fatal("Error: --conn flag is required")
		}
		if queryFile == "" {
			log.Fatal("Error: --query-file flag is required")
		}

		// For now, only support MSSQL.
		if strings.ToLower(dbType) != "mssql" {
			log.Fatalf("Currently, only MSSQL is supported. Provided: %s", dbType)
		}

		// Read the SQL query from the specified file.
		queryData, err := os.ReadFile(queryFile)
		if err != nil {
			log.Fatalf("Failed to read query file: %v", err)
		}
		statements := splitSQLStatements(string(queryData))

		// Connect to the database.
		driver := db.NewMSSQLDriver()
		sqlDB, err := driver.Connect(connStr)
		if err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
		defer sqlDB.Close()
		log.Println("[Database connected]")
		log.Println("")

		// Execute the query.
		for i, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}

			fmt.Print("\033[1A\033[K") // moves up and then deletes the line
			fmt.Printf("Executing statement %d/%d\n", i+1, len(statements))
			// Use context with timeout for each statement.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			_, err = sqlDB.ExecContext(ctx, stmt)
			cancel()
			if err != nil {
				log.Fatalf("Error executing statement %d: %v\nStatement: %s", i+1, err, stmt)
			}
		}

	},
}

func splitSQLStatements(sqlContent string) []string {
	// A simple splitting by semicolon.
	// Note: This approach may need improvements for complex SQL scripts.
	statements := strings.Split(sqlContent, "GO;")
	var result []string
	for _, s := range statements {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().String("query-file", "", "Path to the file containing the SQL query to execute")
}
