dump:
	go run main.go dump \
	--conn "" \
	--skip-data "ApiLogs,FormData"

query:
	go run main.go query \
	--conn "" \
	--query-file ./output/dump.sql

build:
	@echo "Building for Linux..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/linux/go-erdos
	@echo "Building for Windows..."
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./build/win/go-erdos.exe
	@echo "Building for macOS..."
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./build/osx/go-erdos
	@echo "Build complete. Check build folder"


# DROP DATABASE DUMMY_I;CREATE DATABASE DUMMY_I;USE DUMMY_I;