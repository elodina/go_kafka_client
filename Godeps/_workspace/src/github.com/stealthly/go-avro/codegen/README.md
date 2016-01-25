Code Generation Tool for Go-Avro
===============================

`codegen` allows to automatically create Go structs based on defined Avro schema.

**Usage**:

`go run codegen.go --schema foo.avsc --schema bar.avsc --out foo.go`

**Command line flags**:

`--schema` - absolute or relative path to Avro schema file. Multiple of those are allowed but at least one is required.

`--out` - absolute or relative path to output file. All directories will be created if necessary. Existing file will be truncated.
