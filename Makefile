run:
	go run main.go --skip "table1,table2,table3" --bulk 100 --source "sqlDataSource" --target "sqlDataTarget"

build:
	@echo "Building for Linux..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/linux/go-erdos
	@echo "Building for Windows..."
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./build/win/go-erdos.exe
	@echo "Building for macOS..."
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./build/osx/go-erdos
	@echo "Build complete. Check build folder"