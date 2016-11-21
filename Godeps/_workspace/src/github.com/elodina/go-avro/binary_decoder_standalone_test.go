package avro

import (
	"encoding/hex"
	"testing"
)

func TestBool(t *testing.T) {
	for value, bytes := range goodBooleans {
		if actual, _ := NewBinaryDecoder(bytes).ReadBoolean(); actual != value {
			t.Fatalf("Unexpected boolean: expected %v, actual %v\n", value, actual)
		}
	}
	for expected, invalid := range badBooleans {
		if _, err := NewBinaryDecoder(invalid).ReadBoolean(); err != expected {
			t.Fatalf("Unexpected error for boolean: expected %v, actual %v", expected, err)
		}
	}
}

func TestInt(t *testing.T) {
	for value, bytes := range goodInts {
		if actual, _ := NewBinaryDecoder(bytes).ReadInt(); actual != value {
			t.Fatalf("Unexpected int: expected %v, actual %v\n", value, actual)
		}
	}
}

func TestLong(t *testing.T) {
	for value, bytes := range goodLongs {
		if actual, _ := NewBinaryDecoder(bytes).ReadLong(); actual != value {
			t.Fatalf("Unexpected long: expected %v, actual %v\n", value, actual)
		}
	}
}

func TestFloat(t *testing.T) {
	for value, bytes := range goodFloats {
		if actual, _ := NewBinaryDecoder(bytes).ReadFloat(); actual != value {
			t.Fatalf("Unexpected float: expected %v, actual %v\n", value, actual)
		}
	}
}

func TestDouble(t *testing.T) {
	for value, bytes := range goodDoubles {
		if actual, _ := NewBinaryDecoder(bytes).ReadDouble(); actual != value {
			t.Fatalf("Unexpected double: expected %v, actual %v\n", value, actual)
		}
	}
}

func TestBytes(t *testing.T) {
	for index := 0; index < len(goodBytes); index++ {
		bytes := goodBytes[index]
		actual, err := NewBinaryDecoder(bytes).ReadBytes()
		if err != nil {
			t.Fatal("Unexpected error during decoding bytes: %v", err)
		}
		for i := 0; i < len(actual); i++ {
			if actual[i] != bytes[i+1] {
				t.Fatalf("Unexpected byte at index %d: expected 0x%v, actual 0x%v\n", i, hex.EncodeToString([]byte{bytes[i+1]}), hex.EncodeToString([]byte{actual[i]}))
			}
		}
	}

	for index := 0; index < len(badBytes); index++ {
		pair := badBytes[index]
		expected := pair[0].(error)
		arr := pair[1].([]byte)
		if _, err := NewBinaryDecoder(arr).ReadBytes(); err != expected {
			t.Fatalf("Unexpected error for bytes: expected %v, actual %v", expected, err)
		}
	}
}

func TestString(t *testing.T) {
	for value, bytes := range goodStrings {
		if actual, _ := NewBinaryDecoder(bytes).ReadString(); actual != value {
			t.Fatalf("Unexpected string: expected %v, actual %v\n", value, actual)
		}
	}

	for index := 0; index < len(badStrings); index++ {
		pair := badStrings[index]
		expected := pair[0].(error)
		arr := pair[1].([]byte)
		if _, err := NewBinaryDecoder(arr).ReadString(); err != expected {
			t.Fatalf("Unexpected error for string: expected %v, actual %v", expected, err)
		}
	}
}
