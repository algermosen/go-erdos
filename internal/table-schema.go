package internal

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
)

type tableSchema struct {
	schema, table, tableId, column, columnPosition, dataType string
	maxLength, precision, scale                              int
	isNullable, isIdentity, isComputed                       bool
}

type schemaIndex string

type schema map[schemaIndex]tableSchema

func NewTableSchema(db *sql.DB) schema {
	schema := make(schema)

	// execute the query in table-schema.sql
	query, err := ioutil.ReadFile("path/to/table-schema.sql")
	if err != nil {
		log.Fatalf("Failed to read schema query file: %v", err)
	}

	rows, err := db.Query(string(query))
	if err != nil {
		log.Fatalf("Failed to execute schema query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ts tableSchema
		if err := rows.Scan(&ts.schema, &ts.table, &ts.tableId, &ts.column, &ts.columnPosition, &ts.dataType, &ts.maxLength, &ts.precision, &ts.scale, &ts.isNullable, &ts.isIdentity, &ts.isComputed); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		index := schemaIndex(fmt.Sprintf("%s.%s", ts.schema, ts.table))
		schema[index] = ts
	}

	 

	return schema
}



docker exec -it sqlserver-edge /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "P@ssword1" -Q "BACKUP DATABASE [SGACopy] TO DISK = '/var/opt/mssql/backups/SGACopy.bak' WITH FORMAT, COMPRESSION;