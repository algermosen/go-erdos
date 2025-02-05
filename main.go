package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Define command-line flags
	sourceDBConn := flag.String("source", "", "Source database connection string")
	targetDBConn := flag.String("target", "", "Target database connection string")
	skipTables := flag.String("skip", "", "Comma-separated list of tables to skip")
	bulkSize := flag.Int("bulk", 1000, "Number of rows to insert in bulk operations")

	// Parse command-line arguments
	flag.Parse()

	// Validate that required flags are provided
	if *sourceDBConn == "" || *targetDBConn == "" {
		log.Fatalf("Both --source and --target flags are required")
	}

	// Parse the skipTables flag into a string slice
	skipTablesSlice := []string{}
	if *skipTables != "" {
		skipTablesSlice = strings.Split(*skipTables, ",")
	}

	// Start timer
	startTime := time.Now()

	// Run with provided connection strings, skip list, and bulk size
	run(*sourceDBConn, *targetDBConn, skipTablesSlice, *bulkSize)

	// End timer
	elapsedTime := time.Since(startTime)
	fmt.Printf("Database copied in %s\n", elapsedTime)
}

func run(source, target string, ignoreTables []string, bulkSize int) {
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
			continue
		} else {
			// Get schema for the table
			schema, err := getTableSchema(sourceDB, table)
			if err != nil {
				log.Fatalf("Failed to retrieve schema for table %s: %v", table, err)
			}

			// Create table in target database
			if _, err := targetDB.Exec(schema); err != nil {
				fmt.Printf(schema)
				log.Fatalf("Failed to create table %s in target database: %v", table, err)
			}
			fmt.Printf("Table %s created successfully.\n", table)
		}

		// Skip ignored tables
		if contains(ignoreTables, table) {
			fmt.Printf("Table %s ignore. Skipping data copy.\n", table)
			continue
		}

		// Copy data from source to target
		if err := copyTableData(sourceDB, targetDB, table, bulkSize); err != nil {
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
						WHEN DATA_TYPE IN ('text', 'geography', 'xml') THEN ''
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

func copyTableData(sourceDB, targetDB *sql.DB, table string, bulkSize int) error {
	rows, err := sourceDB.Query(fmt.Sprintf("SELECT * FROM [%s]", table))
	if err != nil {
		return fmt.Errorf("no source data: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// columnsAfterRemoval, index := removeItemByValue(columns, "GeoLocation")

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	insertQuery := fmt.Sprintf("INSERT INTO [%s] (%s) VALUES ", table, join(surroundWithBrackets(columns), ", "))

	var batchValues []interface{}
	var rowPlaceholderGroups []string
	currentParamCount := 0

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// values = removeInterfaceItem(values, index)

		rowPlaceholders := make([]string, len(values))
		for i := range values {
			rowPlaceholders[i] = fmt.Sprintf("$%d", len(batchValues)+i+1)
		}
		// rowPlaceholders = removeItem(rowPlaceholders, index)

		batchValues = append(batchValues, values...)
		rowPlaceholderGroups = append(rowPlaceholderGroups, fmt.Sprintf("(%s)", join(rowPlaceholders, ", ")))

		currentParamCount += len(values)
		if currentParamCount >= bulkSize-len(values) { // Batch limit reached
			finalQuery := insertQuery + join(rowPlaceholderGroups, ", ")
			if _, err := targetDB.Exec(finalQuery, batchValues...); err != nil {
				fmt.Println(finalQuery)
				fmt.Println(batchValues)
				return err
			}

			// Reset for the next batch
			batchValues = []interface{}{}
			rowPlaceholderGroups = []string{}
			currentParamCount = 0
		}
	}

	// Handle remaining batch
	if len(batchValues) > 0 {
		finalQuery := insertQuery + join(rowPlaceholderGroups, ", ")
		if _, err := targetDB.Exec(finalQuery, batchValues...); err != nil {
			fmt.Println(finalQuery)
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
