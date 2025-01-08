package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Define command-line flags
	sourceDBConn := flag.String("source", "", "Source database connection string")
	targetDBConn := flag.String("target", "", "Target database connection string")

	// Parse command-line arguments
	flag.Parse()

	// Validate that both flags are provided
	if *sourceDBConn == "" || *targetDBConn == "" {
		log.Fatalf("Both --source and --target flags are required")
	}

	// Run with provided connection strings
	run(*sourceDBConn, *targetDBConn)
}

func run(source, target string) {
	sourceDB, err := sql.Open("mssql", source)
	if err != nil {
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	defer sourceDB.Close()

	// Connect to target database
	targetDB, err := sql.Open("mssql", target)
	if err != nil {
		log.Fatalf("Failed to connect to target database: %v", err)
	}
	defer targetDB.Close()

	// Retrieve table list from source database
	tables, err := getTables(sourceDB)
	if err != nil {
		log.Fatalf("Failed to retrieve tables: %v", err)
	}

	ignoreTables := []string{"ApiLogs", "Logs"}

	// Retrieve existing tables in the target database
	existingTables, err := getTables(targetDB)
	if err != nil {
		log.Fatalf("Failed to retrieve existing tables from target database: %v", err)
	}

	for _, table := range tables {
		fmt.Printf("Processing table: %s\n", table)

		// Check if the table is already created in the target database
		if contains(existingTables, table) {
			fmt.Printf("Table %s already exists in target database. Skipping creation.\n", table)
		} else {
			// Get schema for the table
			schema, err := getTableSchema(sourceDB, table)
			if err != nil {
				log.Fatalf("Failed to retrieve schema for table %s: %v", table, err)
			}

			// Create table in target database
			if _, err := targetDB.Exec(schema); err != nil {
				log.Fatalf("Failed to create table %s in target database: %v", table, err)
			}
			fmt.Printf("Table %s created successfully.\n", table)
		}

		// Skip ignored tables
		if contains(ignoreTables, table) {
			fmt.Printf("Skipping data copy for ignored table: %s\n", table)
			continue
		}

		// Copy data from source to target
		if err := copyTableData(sourceDB, targetDB, table); err != nil {
			if strings.Contains(err.Error(), "no source data") {
				fmt.Printf("No data to copy for table %s.\n", table)
				continue
			} else {
				log.Fatalf("Failed to copy data for table %s: %v", table, err)
			}
		}
		fmt.Printf("Data copied successfully for table: %s\n", table)
	}

	fmt.Println("Database copy completed successfully.")
}

func contains(slice []string, element string) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}
	return false
}

func getTables(db *sql.DB) ([]string, error) {
	query := `
     	SELECT name TABLE_NAME
		FROM sys.tables
		WHERE type = 'U';
    `
	// query := `
	//     SELECT TABLE_NAME
	//     FROM INFORMATION_SCHEMA.TABLES
	//     WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'
	// `

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("error scanning table name: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %v", err)
	}

	return tables, nil
}

func getTableSchema(db *sql.DB, table string) (string, error) {
	var schema string

	// Query to generate the CREATE TABLE statement with IF NOT EXISTS and column capacities
	query := fmt.Sprintf(`
		SELECT 'IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''%s'' AND TABLE_SCHEMA = ''dbo'')
		BEGIN
			CREATE TABLE dbo.[%s] (
				' + STRING_AGG(
					'[' + COLUMN_NAME + ']' + ' ' +
					DATA_TYPE +
					CASE
						WHEN DATA_TYPE IN ('text', 'geography') THEN ''
						WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN '(MAX)'
						WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
						WHEN DATA_TYPE IN ('decimal', 'numeric') THEN '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')'
						ELSE ''
					END, ', '
				) WITHIN GROUP (ORDER BY ORDINAL_POSITION) + '
			)
		END'
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = '%s'
		GROUP BY TABLE_NAME;
	`, table, table, table)

	// Execute the query
	err := db.QueryRow(query).Scan(&schema)
	if err != nil {
		fmt.Println(schema)
		return "", err
	}

	return schema, nil
}

func removeItem(slice []string, index int) []string {
	if index < 0 || index >= len(slice) {
		return slice // Return original slice if index is out of bounds
	}
	return append(slice[:index], slice[index+1:]...)
}

func removeInterfaceItem(slice []interface{}, index int) []interface{} {
	if index < 0 || index >= len(slice) {
		return slice // Return original slice if index is out of bounds
	}
	return append(slice[:index], slice[index+1:]...)
}

func removeItemByValue(slice []string, value string) ([]string, int) {
	for i, v := range slice {
		if v == value {
			return append(slice[:i], slice[i+1:]...), i
		}
	}
	return slice, -1 // Return original slice and -1 if value is not found
}

func copyTableData(sourceDB, targetDB *sql.DB, table string) error {
	rows, err := sourceDB.Query(fmt.Sprintf("SELECT * FROM [%s]", table))
	if err != nil {
		return fmt.Errorf("no source data: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}

		columns, index := removeItemByValue(columns, "GeoLocation")
		placeholders = removeItem(placeholders, index)
		values = removeInterfaceItem(values, index)

		insertQuery := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES (%s)", table, join(surroundWithBrackets(columns), ", "), join(placeholders, ", "))

		if _, err := targetDB.Exec(insertQuery, values...); err != nil {
			fmt.Println(insertQuery)
			fmt.Println(values)
			return err
		}
	}
	return nil
}

func join(elements []string, delimiter string) string {
	result := ""
	for i, element := range elements {
		if i > 0 {
			result += delimiter
		}
		result += element
	}
	return result
}

func surroundWithBrackets(slice []string) []string {
	result := make([]string, len(slice))
	for i, s := range slice {
		result[i] = "[" + s + "]"
	}
	return result
}
