package cfg

import (
	"log"
	"testing"
)

const (
	filename = "samples/test.cfg"
)

func TestLoad(t *testing.T) {
	mymap := make(map[string]string)
	err :=
		Load(filename, mymap)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	log.Printf("%v\n", mymap)
}

func TestLoadNewMap(t *testing.T) {
	mymap, err := LoadNewMap(filename)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	log.Printf("%v\n", mymap)
}
