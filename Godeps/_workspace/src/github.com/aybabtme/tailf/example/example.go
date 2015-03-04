package main

import (
	"github.com/aybabtme/tailf"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {

	tempFile := makeTempFile()
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	done := make(chan struct{})
	go func() {
		writeSlowlyToFile(tempFile)
		close(done)
	}()

	follow, err := tailf.Follow(tempFile.Name(), true)
	if err != nil {
		log.Fatalf("couldn't follow %q: %v", tempFile.Name(), err)
	}

	go func() {
		<-done
		if err := follow.Close(); err != nil {
			log.Fatalf("couldn't close follower: %v", err)
		}
	}()

	_, err = io.Copy(os.Stdout, follow)
	if err != nil {
		log.Fatalf("couldn't read from follower: %v", err)
	}
}

func makeTempFile() *os.File {
	f, err := ioutil.TempFile(os.TempDir(), "tailf_example")
	if err != nil {
		log.Fatalf("creating temp file: %v", err)
	}
	return f
}

func writeSlowlyToFile(f *os.File) {
	rand.Seed(time.Now().UnixNano())
	log.SetOutput(f)
	timer := time.NewTimer(time.Second * 60)

	min := time.Millisecond * 10
	max := time.Millisecond * 20

	for i := 0; ; i++ {
		sleepFor := randomDuration(min, max)
		select {
		case <-timer.C:
			return
		case <-time.After(sleepFor):
			log.Printf("message %d, slept for %v", i, sleepFor)
		}
	}
}

func randomDuration(min, max time.Duration) time.Duration {
	diff := max - min
	dur := rand.Int63n(int64(diff))
	return time.Duration(dur) + min
}
