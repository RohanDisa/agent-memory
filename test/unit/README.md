Unit tests live next to the packages they cover (`internal/.../*_test.go`).
That is the Go default and is what `go test ./...` and the coverage profile
actually measure. This directory exists so the spec's layout is navigable.
