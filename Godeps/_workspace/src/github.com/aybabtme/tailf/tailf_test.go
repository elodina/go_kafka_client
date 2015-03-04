package tailf_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aybabtme/tailf"
)

func TestImpl(t *testing.T) {
	var follower io.ReadCloser
	var err error
	withTempFile(t, time.Millisecond*150, func(t *testing.T, filename string, file *os.File) error {
		follower, err = tailf.Follow(filename, false)
		return err
	})
}

func TestCanFollowFile(t *testing.T) { withTempFile(t, time.Millisecond*150, canFollowFile) }

func canFollowFile(t *testing.T, filename string, file *os.File) error {

	toWrite := []string{
		"hello,",
		" world!",
	}

	want := strings.Join(toWrite, "")

	follow, err := tailf.Follow(filename, true)
	if err != nil {
		return fmt.Errorf("failed creating tailf.follower: '%v'", err)
	}

	go func() {
		for _, str := range toWrite {
			t.Logf("writing %d bytes", len(str))
			_, err := file.WriteString(str)
			if err != nil {
				t.Errorf("failed to write to the file: '%v'", err)
			}
		}
	}()

	// this should work, without blocking forever
	data := make([]byte, len(want))
	n, err := io.ReadAtLeast(follow, data, len(want))
	if err != nil {
		return err
	}
	t.Logf("read %d bytes", n)

	errc := make(chan error)
	go func() {
		// Client reading from the follower
		for {
			n, err := follow.Read(make([]byte, 1))
			t.Logf("read %d bytes after closing", n)
			if err != nil {
				errc <- err
			}
		}
	}()

	if err := follow.Close(); err != nil {
		t.Errorf("failed to close tailf.follower: %v", err)
	}

	got := string(data)
	if want != got {
		t.Errorf("wanted '%v', got '%v'", want, got)
	}

	for {
		select {
		case err := <-errc:
			switch err {
			case io.EOF:
				t.Log("EOF recieved, ending")
				return nil
			default:
				t.Errorf("expected EOF after closing the follower, got '%v' instead", err)
			}
		case <-time.After(time.Duration(time.Millisecond * 100)):
			t.Error("expected follower to return after calling Close() faster")
		}
	}

	return nil
}

func TestCanFollowFileOverwritten(t *testing.T) {
	withTempFile(t, time.Millisecond*150, canFollowFileOverwritten)
}

func canFollowFileOverwritten(t *testing.T, filename string, file *os.File) error {

	toWrite := []string{
		"hello,",
		" world!",
	}
	toWriteAgain := []string{
		"bonjour,",
		" le monde!",
	}

	want := strings.Join(append(toWrite, toWriteAgain...), "")

	follow, err := tailf.Follow(filename, true)
	if err != nil {
		t.Errorf("creating tailf.follower: %v", err)
	}

	go func() {
		for _, str := range toWrite {
			t.Logf("writing %d bytes", len(str))
			_, err := file.WriteString(str)
			if err != nil {
				t.Errorf("failed to write to test file: %v", err)
			}
		}

		file.Sync()

		if err := os.Remove(filename); err != nil {
			t.Errorf("couldn't delete file %q: %v", filename, err)
		}

		file.Sync()

		file, err = os.Create(filename)
		if err != nil {
			t.Errorf("failed to write to test file: %v", err)
		}
		defer file.Close()
		for _, str := range toWriteAgain {
			t.Logf("writing %d bytes", len(str))
			_, err := file.WriteString(str)
			if err != nil {
				t.Errorf("failed to write to test file: %v", err)
			}
		}

	}()

	// this should work, without blocking forever
	data := make([]byte, len(want))
	_, err = io.ReadAtLeast(follow, data, len(want))
	if err != nil {
		return err
	}

	if err := follow.Close(); err != nil {
		t.Errorf("failed to close tailf.follower: %v", err)
	}

	got := string(data)
	if want != got {
		t.Errorf("wanted: [%d]byte{'%v'}, got: [%d]byte{'%v'}", len(want), want, len(got), got)
	}

	return nil
}

func TestCanFollowFileFromEnd(t *testing.T) {
	withTempFile(t, time.Millisecond*150, canFollowFileFromEnd)
}

func canFollowFileFromEnd(t *testing.T, filename string, file *os.File) error {

	_, err := file.WriteString("shouldn't read this part")
	if err != nil {
		return err
	}

	toWrite := []string{
		"hello,",
		" world!",
	}

	want := strings.Join(toWrite, "")

	follow, err := tailf.Follow(filename, false)
	if err != nil {
		return fmt.Errorf("failed creating tailf.follower: %v", err)
	}

	go func() {
		for _, str := range toWrite {
			t.Logf("writing %d bytes", len(str))
			_, err := file.WriteString(str)
			if err != nil {
				t.Errorf("failed to write to the file: '%v'", err)
			}
		}
	}()

	// this should work, without blocking forever
	data := make([]byte, len(want))
	_, err = io.ReadAtLeast(follow, data, len(want))
	if err != nil {
		return err
	}

	// this should block forever
	errc := make(chan error, 1)
	go func() {
		n, err := io.ReadAtLeast(follow, make([]byte, 1), 1)
		t.Logf("read %d bytes after closing", n)
		errc <- err
	}()

	if err := follow.Close(); err != nil {
		t.Errorf("failed to close tailf.follower: %v", err)
	}

	got := string(data)
	if want != got {
		t.Errorf("wanted '%v', got '%v'", want, got)
	}

	err = <-errc
	if err != io.EOF {
		t.Errorf("expected EOF after closing the follower, got '%v' instead", err)
	}

	return nil
}

func TestFollowTruncation(t *testing.T) { withTempFile(t, time.Millisecond*150, canFollowTruncation) }

func canFollowTruncation(t *testing.T, filename string, file *os.File) error {
	follow, err := tailf.Follow(filename, false)
	if err != nil {
		return fmt.Errorf("failed creating tailf.follower: %v", err)
	}

	for i := int64(0); i < 10; i++ {
		if i%2 == 0 {
			t.Logf("truncating the file")
			file, err := os.OpenFile(filename, os.O_TRUNC, os.ModeTemporary)
			if err != nil {
				return fmt.Errorf("unable to truncate file: %v", err)
			}
			file.Close()
		}

		wantBuf := strconv.AppendInt(make([]byte, 0), i, 10)
		_, err = file.WriteString(string(wantBuf))
		if err != nil {
			t.Errorf("write failed, %v", err)
		}
		t.Logf("wrote: '%v'", wantBuf)

		gotBuf := make([]byte, 1)
		_, err := follow.Read(gotBuf)
		if err != nil {
			return fmt.Errorf("failed to read: %v", err)
		}

		if !bytes.Equal(gotBuf, wantBuf) {
			t.Logf("want=%x", wantBuf)
			t.Logf(" got=%x", gotBuf)
			t.Errorf("missed write after truncation")
		}
	}

	if err := follow.Close(); err != nil {
		t.Errorf("failed to close tailf.follower: %v", err)
	}

	return nil
}

// Continually read from a file that is having data written to it every 5ms, and randomly truncated every [5,55]ms
func TestFollowRandomTruncation(t *testing.T) {
	withTempFile(t, time.Second, func(t *testing.T, filename string, file *os.File) error {
		follow, err := tailf.Follow(filename, false)
		if err != nil {
			return fmt.Errorf("failed creating tailf.follower: %v", err)
		}

		expected := "Bytes!"

		writer := time.NewTicker(time.Millisecond * 5)
		defer writer.Stop()

		go func() {
			for _ = range writer.C {
				t.Logf("writing: '%v'", expected)
				file.WriteString(expected + "\n")
			}
		}()

		go func() {
			for {
				time.Sleep(time.Duration(time.Millisecond) * time.Duration(rand.Intn(50)+5))

				t.Log("truncating the file")
				trunc, err := os.OpenFile(filename, os.O_TRUNC, os.ModeTemporary)
				if err != nil {
					t.Errorf("unable to truncate file")
				}
				trunc.Close()
			}
		}()

		go func() {
			scanner := bufio.NewScanner(follow)
			for scanner.Scan() {
				t.Log("read:", scanner.Text())
				if actual := strings.Trim(scanner.Text(), "\x00"); actual != expected {
					t.Errorf("bad read! Expected(%v) != Actual(%v)", []byte(expected), []byte(actual))
				}
			}

			if err := scanner.Err(); err != nil && err != io.EOF {
				t.Error("scanner returned an error", err)
			}
		}()

		time.Sleep(time.Duration(time.Millisecond * 100))
		follow.Close()

		return nil
	})
}

// Run for 50ms constantly trying to read from a tailf.follower that has nothing to read
func TestSpinningReader(t *testing.T) {
	withTempFile(t, time.Millisecond*150, func(t *testing.T, filename string, file *os.File) error {
		follow, err := tailf.Follow(filename, false)
		if err != nil {
			return fmt.Errorf("failed creating tailf.follower: %v", err)
		}

		stop := make(chan struct{})
		timeout := time.AfterFunc(time.Duration(time.Millisecond*50), func() { stop <- struct{}{} })

		read := make(chan struct{}, 1000)
		go func() {
			t.Log("reader running")
			buf := make([]byte, 100)
			for {
				_, err := follow.Read(buf)
				if err != nil {
					t.Errorf("read error: %v", err)
				}
				t.Log("read completed")
				read <- struct{}{}
			}
		}()

		count := 0

		// TODO: fix this
		func() {
			for {
				select {
				case <-stop:
					return
				case <-read:
					count += 1
					if count > 5 {
						t.Error("spinning on read")
					}
				}
			}
		}()

		t.Logf("read ran '%v' times", count)
		timeout.Stop()
		return nil
	})
}

// Test to see that our polling
func TestPollingReader(t *testing.T) {
	// Timestamp this stuff
	go func() {
		for {
			t.Logf("timestamp: %v", time.Now())
			<-time.After(time.Millisecond * 250)
		}
	}()

	withTempFile(t, time.Second*2, func(t *testing.T, filename string, file *os.File) error {
		if err := os.Chmod(path.Dir(filename), 0355); err != nil {
			return fmt.Errorf("unable to modify tempdir's permissions to disallow listing its contents")
		}

		follow, err := tailf.Follow(filename, false)
		if err != nil {
			return fmt.Errorf("failed creating tailf.follower: %v", err)
		}

		scanner := bufio.NewScanner(follow)
		scanner.Split(bufio.ScanBytes)
		wanted := make([]byte, 1)

		//
		// Write out 10 bytes
		for i := 1; i <= 10; i++ {
			file.Write([]byte{byte(i)})
			t.Logf("wrote '%v' out", i)
		}

		//
		// Read in 9 bytes
		for i := 1; i <= 9 && scanner.Scan(); i++ {
			wanted[0] = byte(i)
			got := scanner.Bytes()

			t.Logf("read: wanted(%v) ?= got(%v)", wanted, got)
			if !bytes.Equal(wanted, got) {
				t.Errorf("read: wanted(%v) != got(%v)", wanted, got)
			}
		}

		//
		// Rotate the file
		t.Logf("removing: %v:'%v'", file, file.Name())
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("unable to remove the temp file")
		}

		file, err = os.Create(filename)
		if err != nil {
			return fmt.Errorf("unable to recreate the temp file")
		}
		t.Logf("created: %v:'%v'", file, file.Name())

		//
		// Write out 10 more bytes
		for i := 11; i <= 20; i++ {
			file.Write([]byte{byte(i)})
			t.Logf("wrote '%v' out", i)
		}

		//
		// Read in the last 11 bytes
		for i := 10; i <= 20 && scanner.Scan(); i++ {
			wanted[0] = byte(i)
			got := scanner.Bytes()

			t.Logf("read: wanted(%v) ?= got(%v)", wanted, got)
			if !bytes.Equal(wanted, got) {
				t.Errorf("read: wanted(%v) != got(%v)", wanted, got)
			}
		}

		return nil
	})
}

func withTempFile(t *testing.T, timeout time.Duration, action func(t *testing.T, filename string, file *os.File) error) {
	dir, err := ioutil.TempDir(os.TempDir(), "tailf_test_dir")
	if err != nil {
		t.Fatalf("couldn't create temp dir: '%v'", err)
	}

	file, err := ioutil.TempFile(dir, "tailf_test")
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("couldn't create temp file: '%v'", err)
	}
	defer os.RemoveAll(dir)
	defer file.Close()

	errc := make(chan error)
	go func() { errc <- action(t, file.Name(), file) }()

	select {
	case err = <-errc:
		if err != nil {
			t.Errorf("failure: %v", err)
		}
	case <-time.After(timeout):
		t.Error("test took too long :(")
	}
}
