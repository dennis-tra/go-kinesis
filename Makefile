
tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

proto:
	protoc --go_out=. --go_opt=paths=source_relative pb/messages.proto