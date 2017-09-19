default:
	go run main.go

test:
	@echo "+ $@"
	@go test -tags nocgo $(shell go list ./... | grep -vE 'vendor|blockchain')