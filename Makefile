dev:
	go run main.go \
	--skip "Empleados_bck,Tbl_DDL_Change_Log,MobileApiLogs" \
	--skip-data "ApiLogs,Logs,MobileApiLogs" \
	--bulk 100 \
	--source "Data Source=localhost;Initial Catalog=SGACopy;User ID=sa;Password=P@ssword1;MultipleActiveResultSets=True;Connect Timeout=100;" \
	--target "Data Source=localhost;Initial Catalog=SGACopy2;User ID=sa;Password=P@ssword1;MultipleActiveResultSets=True;Connect Timeout=100;"

build:
	@echo "Building for Linux..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./build/linux/go-erdos
	@echo "Building for Windows..."
	@CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o ./build/win/go-erdos.exe
	@echo "Building for macOS..."
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./build/osx/go-erdos
	@echo "Build complete. Check build folder"