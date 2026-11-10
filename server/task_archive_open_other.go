//go:build !unix

package server

import (
	"errors"
	"io/fs"
	"os"
)

// openArchiveRegular is the non-Unix fallback. The walk already skips non-regular
// files before calling it. A post-open stat still refuses to read anything that
// is not a regular file if the path changed type.
func openArchiveRegular(path string) (*os.File, fs.FileInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
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
	return errors.Is(err, errArchiveNotRegular)
}
