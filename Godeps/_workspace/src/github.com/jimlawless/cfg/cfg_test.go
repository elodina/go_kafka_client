/*Para testar execute:
Ctrl+9
go test -v
*/

package cfg

import (
	"git.tidexa-es.com/global/go-utils/logutil"
	"testing"
)

const (
	filename = "samples/test.cfg"
)

var (
	log = logutil.GetLogger("cfg")
)

func init() {
	logutil.GetDefaultTestLogBackend()
}

func TestLoad(t *testing.T) {
	mymap := make(map[string]string)
	err :=
		Load(filename, mymap)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("%v\n", mymap)
}

func TestLoadNewMap(t *testing.T) {
	mymap, err := LoadNewMap(filename)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("%v\n", mymap)
}
