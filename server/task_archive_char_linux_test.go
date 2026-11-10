//go:build linux

package server

import (
	"errors"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

// createArchiveCharNodes makes private char devices inside dir. The numbers match
// the kernel mem and tty drivers (null, zero, random, urandom, console) but the
// nodes live only in the test directory. Callers must not read them: zero and
// urandom never return EOF, and console/random can block.
//
// Returns how many nodes were created and any paths owned by root (sudo mknod)
// that the test user cannot unlink.
func createArchiveCharNodes(t *testing.T, dir string) (created int, rootOwned []string) {
	t.Helper()
	nodes := []struct {
		name         string
		major, minor uint32
	}{
		{"null", 1, 3},
		{"zero", 1, 5},
		{"random", 1, 8},
		{"urandom", 1, 9},
		{"console", 5, 1},
	}
	for _, node := range nodes {
		path := filepath.Join(dir, node.name)
		dev := int(unix.Mkdev(node.major, node.minor))
		err := unix.Mknod(path, unix.S_IFCHR|0o666, dev)
		switch {
		case err == nil:
			created++
			continue
		case errors.Is(err, unix.EPERM):
			cmd := exec.Command("sudo", "-n", "mknod", "-m", "666", path, "c",
				strconv.FormatUint(uint64(node.major), 10),
				strconv.FormatUint(uint64(node.minor), 10))
			out, sudoErr := cmd.CombinedOutput()
			if sudoErr != nil {
				t.Logf("char device %s (%d,%d) unavailable: %v %s", node.name, node.major, node.minor, sudoErr, out)
				return created, rootOwned
			}
			created++
			rootOwned = append(rootOwned, path)
		default:
			t.Fatalf("mknod %s (%d,%d): %v", node.name, node.major, node.minor, err)
		}
	}
	return created, rootOwned
}
