# tailf

An `io.ReaderCloser` to a file, which never reaches
`io.EOF` and instead blocks for new data to be appended to the file it
watches.  Effectively, the same as what `tail -f {{filename}}` does.

This works by putting an inotify watch on the file and blocking for
events when we reach the file's max size.

When the `io.ReaderCloser` is closed, the watch is cancelled and the
following reads will return normally until they reach the offset
that was last reported as the max file size, where the reader will
return `io.EOF`.


# Example

See `example/example.go`:

![tailf](https://cloud.githubusercontent.com/assets/1189716/4695009/026985fa-57e0-11e4-9aae-221dbc533c29.gif)


```go
func main() {

    tempFile := makeTempFile()
    defer os.Remove(tempFile.Name())
    defer tempFile.Close()

    done := make(chan struct{})
    go func() {
        writeSlowlyToFile(tempFile)
        close(done)
    }()

    follow, err := tailf.Follow(tempFile.Name())
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
```

