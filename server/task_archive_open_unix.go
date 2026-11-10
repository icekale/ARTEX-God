//go:build unix

package server

import (
	"errors"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

// openArchiveRegular returns a readable regular file or a skippable error.
// The descriptor is never read unless fstat shows S_IFREG. O_NOFOLLOW and
// O_NONBLOCK close the window where the directory entry changes to a symlink,
// FIFO, socket, or device between the walk lstat and the read.
func openArchiveRegular(path string) (*os.File, fs.FileInfo, error) {
	var (
		fd  int
		err error
	)
	for {
		fd, err = unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}
	var st unix.Stat_t
	for {
		err = unix.Fstat(fd, &st)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		_ = unix.Close(fd)
		return nil, nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fd)
		return nil, nil, errArchiveNotRegular
	}
	// Regular-file reads do not need O_NONBLOCK. Clear it before os.NewFile so
	// the copy stays on the ordinary blocking path instead of the poller.
	if err := unix.SetNonblock(fd, false); err != nil {
		_ = unix.Close(fd)
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, errors.New("invalid archive file descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, errArchiveNotRegular
	}
	return file, info, nil
}

func archiveOpenSkippable(err error) bool {
	return errors.Is(err, errArchiveNotRegular) ||
		errors.Is(err, unix.ELOOP) ||
		errors.Is(err, unix.ENXIO) ||
		errors.Is(err, unix.ENODEV)
}
