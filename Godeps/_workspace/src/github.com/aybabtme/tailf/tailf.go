/*
Package tailf implements an io.ReaderCloser to a file, which never reaches
io.EOF and instead, blocks until new data is appended to the file it
watches.  Effectively, the same as what `tail -f {{filename}}` does.

This works by putting an inotify watch on the file.

When the io.ReaderCloser is closed, the watch is cancelled and the
following reads will return normally until they reach the offset
that was last reported as the max file size, where the reader will
return EOF.
*/

package tailf

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"gopkg.in/fsnotify.v1"
)

type (
	// ErrFileTruncated signifies the underlying file of a tailf.Follower
	// has been truncated. The follower should be discarded.
	ErrFileTruncated struct{ error }
	// ErrFileRemoved signifies the underlying file of a tailf.Follower
	// has been removed. The follower should be discarded.
	ErrFileRemoved struct{ error }
)

type follower struct {
	filename string

	mu             sync.Mutex
	notifyc        chan struct{}
	errc           chan error
	file           *os.File
	fileReader     *bufio.Reader
	rotationBuffer *bytes.Buffer
	reader         io.Reader
	watch          *fsnotify.Watcher
	size           int64
}

// Follow returns an io.ReadCloser that follows the writes to a file.
func Follow(filename string, fromStart bool) (io.ReadCloser, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	if !fromStart {
		_, err := file.Seek(0, os.SEEK_END)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	reader := bufio.NewReader(file)

	watch, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	absolute_path, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}

	f := &follower{
		filename:       absolute_path,
		notifyc:        make(chan struct{}),
		errc:           make(chan error),
		file:           file,
		fileReader:     reader,
		rotationBuffer: bytes.NewBuffer(nil),
		reader:         reader,
		watch:          watch,
		size:           0,
	}

	if err := watch.Add(filepath.Dir(absolute_path)); err != nil {
		// If we can't watch the directory, we need to poll the file to see if it changes
		go f.pollForChanges()
	}

	go f.followFile()

	return f, nil
}

// Close will remove the watch on the file. Subsequent reads to the file
// will eventually reach EOF.
func (f *follower) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	werr := f.watch.Close()
	cerr := f.file.Close()
	switch {
	case werr != nil && cerr == nil:
		return werr
	case werr == nil && cerr != nil:
		return cerr
	case werr != nil && cerr != nil:
		return fmt.Errorf("couldn't remove watch (%v) and close file (%v)", werr, cerr)
	}
	return nil
}

func (f *follower) Read(b []byte) (int, error) {
	f.mu.Lock()

	// Refill the buffer
	_, err := f.fileReader.Peek(1)
	switch err { // some errors are expected
	case nil:
		// all is good
	case io.EOF:
		// `readable` will be 0 and we will block
		// until inotify reports new data, carry on
	case bufio.ErrBufferFull:
		// the bufio.Reader was already full, carry on
	default:
		perr, ok := err.(*os.PathError)
		if ok && perr.Err == syscall.Errno(syscall.EBADF) {
			// bad file number will likely be replaced by
			// a new file on an inotify event, so carry on
		} else {
			return 0, err
		}
	}
	readable := f.fileReader.Buffered()

	// check for errors before doing anything
	select {
	case err, open := <-f.errc:
		if !open && readable != 0 {
			break
		}
		f.mu.Unlock()
		if !open {
			return 0, io.EOF
		}
		return 0, err
	default:
	}

	if readable == 0 {
		f.mu.Unlock()

		// wait for the file to grow
		_, open := <-f.notifyc
		if !open {
			return 0, io.EOF
		}
		// then let the reader try again
		return 0, nil
	}

	n, err := f.reader.Read(b[:imin(readable, len(b))])
	f.mu.Unlock()

	return n, err
}

func (f *follower) followFile() {
	defer f.watch.Close()
	defer close(f.notifyc)
	defer close(f.errc)
	for {
		select {
		case ev, open := <-f.watch.Events:
			if !open {
				return
			}
			if pathEqual(ev.Name, f.filename) {
				err := f.handleFileEvent(ev)
				if err != nil {
					f.errc <- err
					return
				}
			}
		case err, open := <-f.watch.Errors:
			if !open {
				return
			}
			if err != nil {
				f.errc <- err
				return
			}
		}

		select {
		case f.notifyc <- struct{}{}:
			// try to wake up whoever was waiting on an update
		default:
			// otherwise just wait for the next event
		}
	}
}

func (f *follower) handleFileEvent(ev fsnotify.Event) error {
	switch {
	case isOp(ev, fsnotify.Create):
		// new file created with the same name
		return f.reopenFile()

	case isOp(ev, fsnotify.Write):
		// On write, check to see if the file has been truncated
		// If not, insure the bufio buffer is full
		switch f.checkForTruncate() {
		case nil:
			return f.fillFileBuffer()
		case ErrFileRemoved{}:
			// If file was written to and then removed before we could even Stat the file, just wait for the next creation
			return nil
		default:
			return f.reopenFile()
		}

	case isOp(ev, fsnotify.Remove), isOp(ev, fsnotify.Rename):
		// wait for a new file to be created
		return nil

	case isOp(ev, fsnotify.Chmod):
		// Modified time on the file changed, noop
		return nil

	default:
		return fmt.Errorf("recieved unknown fsnotify event: %#v", ev)
	}
}

func (f *follower) reopenFile() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := os.Stat(f.filename)
	if os.IsNotExist(err) {
		// File disappeared too quickly, wait for next rotation
		return nil
	}
	if err != nil {
		return err
	}

	if err := f.file.Close(); err != nil {
		return err
	}

	f.file, err = os.OpenFile(f.filename, os.O_RDONLY, 0)
	if err != nil {
		return err
	}

	// recover buffered bytes
	unreadByteCount := f.rotationBuffer.Len() + f.fileReader.Buffered()
	buf := bytes.NewBuffer(make([]byte, unreadByteCount))

	n, err := f.reader.Read(buf.Bytes())
	if err != nil {
		return err
	} else if n != unreadByteCount {
		return fmt.Errorf("failed to flush the buffer completely: Actual(%d) | Expected(%d) | buf_len(%d)", n, unreadByteCount, buf.Len())
	}

	f.fileReader.Reset(f.file)
	f.rotationBuffer = buf

	// append buffered bytes before the new file
	f.reader = io.MultiReader(f.rotationBuffer, f.fileReader)

	return err
}

func (f *follower) fillFileBuffer() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.fileReader.Peek(1) // Refill the buffer
	switch err {
	case nil, io.EOF, bufio.ErrBufferFull:
		// all good
		return nil
	default:
		// not nil and not an expected error
		return err
	}
}

// Note: if the file gets truncated, and before the size can be stat'd,
// it has regrown to be >= the same size as previously, the truncate
// will be missed. tl;dr, don't use copy-truncate...
func (f *follower) checkForTruncate() error {
	f.mu.Lock()

	fi, err := os.Stat(f.filename)

	f.mu.Unlock()
	if os.IsNotExist(err) {
		return ErrFileRemoved{fmt.Errorf("file was removed: %v", f.filename)}
	}
	if err != nil {
		return err
	}

	newSize := fi.Size()
	if newSize < f.size {
		err = ErrFileTruncated{fmt.Errorf("file (%s) was truncated", f.filename)}
	}

	f.size = newSize
	return err
}

// This is here for situations where the directory the watched file sits in can't be inotified on
func (f *follower) pollForChanges() {
	previousFile, err := f.file.Stat()
	if err != nil {
		f.errc <- err
	}

	if err := f.watch.Add(f.filename); err != nil {
		f.errc <- err
	}

	for {
		currentFile, err := os.Stat(f.filename)

		switch err {
		case nil:
			switch os.SameFile(currentFile, previousFile) {
			case true:
				// No change, do nothing
				break
			case false:
				previousFile = currentFile
				if err := f.reopenFile(); err != nil {
					f.errc <- err
				}

				if err := f.watch.Add(f.filename); err != nil {
					f.errc <- err
				}

				select {
				case f.notifyc <- struct{}{}:
					// try to wake up whoever was waiting on an update
				default:
					// otherwise just wait for the next event
				}
			}
		default:
			// Filename doens't seem to be there, wait for it to re-appear
		}

		time.Sleep(time.Second)
	}
}

func isOp(ev fsnotify.Event, op fsnotify.Op) bool {
	return ev.Op&op == op
}

func pathEqual(lhs, rhs string) bool {
	var err error
	lhs, err = filepath.Abs(lhs)
	if err != nil {
		return false
	}
	rhs, err = filepath.Abs(rhs)
	if err != nil {
		return false
	}
	return lhs == rhs
}

func imin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
