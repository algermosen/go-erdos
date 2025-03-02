package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "erdos",
	Short: "Erdos is a multi-database management tool",
	Long: `Erdos is a CLI tool that allows seamless interaction between different database managers.
It supports exporting, importing, migrations, parsing, and transformations between different databases.

Currently Supported Databases:
- PostgreSQL
- MSSQL
- SQLite
`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Welcome to Erdos! Use --help to see available commands.")
	},
}

func init() {
	// Add global flags here if needed in the future
	rootCmd.PersistentFlags().String("dbtype", "mssql", "Type of the database (mssql, mysql, postgres, sqlite) (default: mssql)")
	rootCmd.PersistentFlags().String("conn", "", "Database connection string")
}
