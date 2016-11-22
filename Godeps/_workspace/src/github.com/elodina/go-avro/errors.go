package avro

import "errors"

// Signals that an end of file or stream has been reached unexpectedly.
var EOF = errors.New("End of file reached")

// Happens when the given value to decode overflows maximum int32 value.
var IntOverflow = errors.New("Overflowed an int value")

// Happens when the given value to decode overflows maximum int64 value.
var LongOverflow = errors.New("Overflowed a long value")

// Happens when given value to decode as bytes has negative length.
var NegativeBytesLength = errors.New("Negative bytes length")

// Happens when given value to decode as bool is neither 0x00 nor 0x01.
var InvalidBool = errors.New("Invalid bool value")

// Happens when given value to decode as a int is invalid
var InvalidInt = errors.New("Invalid int value")

// Happens when given value to decode as a long is invalid
var InvalidLong = errors.New("Invalid long value")

// Happens when given value to decode as string has either negative or undecodable length.
var InvalidStringLength = errors.New("Invalid string length")

// Indicates the given file to decode does not correspond to Avro data file format.
var NotAvroFile = errors.New("Not an Avro data file")

// Happens when file header's sync and block's sync do not match - indicates corrupted data.
var InvalidSync = errors.New("Invalid sync")

// Happens when trying to read next block without finishing the previous one.
var BlockNotFinished = errors.New("Block read is unfinished")

// Happens when avro schema contains invalid value for fixed size.
var InvalidFixedSize = errors.New("Invalid Fixed type size")

// Happens when avro schema contains invalid value for map value type or array item type.
var InvalidValueType = errors.New("Invalid array or map value type")

// Happens when avro schema contains a union within union.
var NestedUnionsNotAllowed = errors.New("Nested unions are not allowed")

// Happens when avro schema is unparsable or is invalid in any other way.
var InvalidSchema = errors.New("Invalid schema")

// Happens when a datum reader has no set schema.
var SchemaNotSet = errors.New("Schema not set")

// FieldDoesNotExist happens when a struct does not have a necessary field.
var FieldDoesNotExist = errors.New("Field does not exist")
