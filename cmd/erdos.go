package cmd

import (
	"fmt"
	"os"

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

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func init() {
	// Add global flags here if needed in the future
}
