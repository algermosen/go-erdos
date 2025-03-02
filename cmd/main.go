package cmd

import (
	"fmt"
	"os"

	"github.com/algermosen/go-erdos/internal/logger"
)

var appLogger logger.Logger

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func SetLogger(l logger.Logger) {
	appLogger = l
}
