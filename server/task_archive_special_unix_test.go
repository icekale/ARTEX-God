//go:build unix

package server

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	pgdb "github.com/Autumn-27/artex/db"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sys/unix"
)

// archiveSpecialFileTimeout bounds the regression that used to block for hours
// on a FIFO open or a character-device read.
const archiveSpecialFileTimeout = 5 * time.Second

func TestTaskArchivePackageSkipsSpecialFilesWithoutReading(t *testing.T) {
	payload, err := os.MkdirTemp("/tmp", "ta")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(payload) })

	keep := []byte("keep me")
	nested := []byte("nested ordinary file")
	if err := os.WriteFile(filepath.Join(payload, "keep.txt"), keep, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(payload, "empty.dat"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(payload, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(payload, "nested", "note.txt"), nested, 0o600); err != nil {
		t.Fatal(err)
	}
	// Field logs skipped a symlink named bin immediately before walking dev/.
	if err := os.Symlink("keep.txt", filepath.Join(payload, "bin")); err != nil {
		t.Fatal(err)
	}

	fed := filepath.Join(payload, "fifo-fed")
	idle := filepath.Join(payload, "fifo-idle")
	if err := unix.Mkfifo(fed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(idle, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("fifo-idle", filepath.Join(payload, "link-fifo")); err != nil {
		t.Fatal(err)
	}
	const fifoSentinel = "FIFO-PAYLOAD-SENTINEL"
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		w, openErr := os.OpenFile(fed, os.O_WRONLY, 0)
		if openErr != nil {
			return
		}
		_, _ = io.WriteString(w, fifoSentinel)
		_ = w.Close()
	}()
	t.Cleanup(func() {
		// Unblock the writer if the archiver correctly never opened the FIFO.
		r, openErr := os.OpenFile(fed, os.O_RDONLY|unix.O_NONBLOCK, 0)
		if openErr == nil {
			_ = r.Close()
		}
		select {
		case <-writerDone:
		case <-time.After(2 * time.Second):
		}
	})

	sockPath := filepath.Join(payload, "zsock")
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("unix socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	devDir := filepath.Join(payload, "dev")
	if err := os.MkdirAll(devDir, 0o700); err != nil {
		t.Fatal(err)
	}
	charNodes, rootOwned := createArchiveCharNodes(t, devDir)
	if len(rootOwned) > 0 {
		t.Cleanup(func() {
			args := append([]string{"-n", "rm", "-f"}, rootOwned...)
			_ = exec.Command("sudo", args...).Run()
		})
	}
	if charNodes == 0 {
		t.Log("isolated char devices were not created (CAP_MKNOD or passwordless sudo unavailable); FIFO, socket, and symlink coverage still runs")
	}

	// Keep the archive outside payload so the walk does not archive itself.
	archivePath := filepath.Join(os.TempDir(), filepath.Base(payload)+".tar.zst")
	t.Cleanup(func() {
		_ = os.Remove(archivePath)
		_ = os.Remove(archivePath + ".partial")
	})

	snapshot := &pgdb.TaskArchiveSnapshot{FormatVersion: pgdb.TaskArchiveFormatVersion, TaskID: 42}
	type result struct {
		original int64
		checksum string
		err      error
	}
	done := make(chan result, 1)
	started := time.Now()
	go func() {
		original, _, checksum, err := writeTaskArchivePackage(archivePath, payload, snapshot)
		done <- result{original: original, checksum: checksum, err: err}
	}()

	var got result
	select {
	case got = <-done:
	case <-time.After(archiveSpecialFileTimeout):
		t.Fatalf("writeTaskArchivePackage still blocked after %s while walking special files (fifo/device read hang)", archiveSpecialFileTimeout)
	}
	if got.err != nil {
		t.Fatalf("archive special files: %v", got.err)
	}
	if time.Since(started) > archiveSpecialFileTimeout {
		t.Fatalf("archive took %s, want < %s", time.Since(started), archiveSpecialFileTimeout)
	}
	if _, err := os.Stat(archivePath + ".partial"); !os.IsNotExist(err) {
		t.Fatalf("partial archive left behind: %v", err)
	}

	raw := decompressArchiveLimited(t, archivePath, 1<<20)
	if bytes.Contains(raw, []byte(fifoSentinel)) {
		t.Fatal("FIFO payload was copied into the archive")
	}
	entries := tarEntryNames(t, raw)
	for _, name := range []string{
		"bin", "fifo-fed", "fifo-idle", "link-fifo", "zsock",
		"dev/null", "dev/zero", "dev/random", "dev/urandom", "dev/console",
	} {
		if _, ok := entries[name]; ok {
			t.Fatalf("special node archived: %s type=%d", name, entries[name])
		}
	}
	for name, flag := range entries {
		if flag != tar.TypeReg && flag != tar.TypeRegA && flag != tar.TypeDir {
			t.Fatalf("entry %s has typeflag %d; format only stores files and directories", name, flag)
		}
	}
	for _, name := range []string{"keep.txt", "empty.dat", "nested/note.txt", "dev/"} {
		if _, ok := entries[name]; !ok {
			// directory headers may omit the trailing slash depending on FileInfoHeader
			if name == "dev/" {
				if _, ok := entries["dev"]; !ok {
					t.Fatalf("missing directory entry dev, entries=%v", keys(entries))
				}
				continue
			}
			t.Fatalf("missing regular entry %s, entries=%v", name, keys(entries))
		}
	}

	var wantSize int64
	walkErr := filepath.WalkDir(payload, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			wantSize += info.Size()
		}
		return nil
	})
	if walkErr != nil {
		t.Fatal(walkErr)
	}
	if got.original != wantSize {
		t.Fatalf("archived payload size=%d, regular files total %d", got.original, wantSize)
	}

	extracted, err := os.MkdirTemp("/tmp", "tae")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(extracted) })
	if err := extractTaskArchivePackage(archivePath, got.checksum, extracted); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(filepath.Join(extracted, "keep.txt")); err != nil || !bytes.Equal(body, keep) {
		t.Fatalf("keep.txt=%q err=%v", body, err)
	}
	if body, err := os.ReadFile(filepath.Join(extracted, "nested", "note.txt")); err != nil || !bytes.Equal(body, nested) {
		t.Fatalf("nested=%q err=%v", body, err)
	}
	if body, err := os.ReadFile(filepath.Join(extracted, "empty.dat")); err != nil || len(body) != 0 {
		t.Fatalf("empty.dat=%q err=%v", body, err)
	}
	if _, err := os.Lstat(filepath.Join(extracted, "dev")); err != nil {
		t.Fatalf("dev directory not restored: %v", err)
	}
	matches, _ := filepath.Glob(filepath.Join(extracted, "dev", "*"))
	if len(matches) != 0 {
		t.Fatalf("dev special nodes restored: %v", matches)
	}
}

func decompressArchiveLimited(t *testing.T, path string, limit int64) []byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder, err := zstd.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(decoder, limit+1))
	if err != nil {
		t.Fatal(err)
	}
	if n > limit {
		t.Fatalf("decompressed archive exceeded %d bytes; special-file payload may have been copied", limit)
	}
	return buf.Bytes()
}

func tarEntryNames(t *testing.T, raw []byte) map[string]byte {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(raw))
	names := map[string]byte{}
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		names[header.Name] = header.Typeflag
	}
}

func TestOpenArchiveRegularRejectsSpecialFiles(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "tao")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	body := bytes.Repeat([]byte("abc"), 100_000)
	regular := filepath.Join(dir, "keep.txt")
	if err := os.WriteFile(regular, body, 0o600); err != nil {
		t.Fatal(err)
	}
	file, info, err := openArchiveRegular(regular)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(body)) {
		t.Fatalf("size=%d want %d", info.Size(), len(body))
	}
	assertLinuxRegularFDBlocking(t, regular)
	got, err := io.ReadAll(file)
	closeErr := file.Close()
	if err != nil || closeErr != nil || !bytes.Equal(got, body) {
		t.Fatalf("regular read len=%d err=%v close=%v", len(got), err, closeErr)
	}

	fifo := filepath.Join(dir, "queue.fifo")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	err = callWithin(t, time.Second, func() error {
		opened, _, openErr := openArchiveRegular(fifo)
		if opened != nil {
			_ = opened.Close()
			return errors.New("FIFO descriptor was returned")
		}
		if !errors.Is(openErr, errArchiveNotRegular) {
			return openErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FIFO: %v", err)
	}

	link := filepath.Join(dir, "link-fifo")
	if err := os.Symlink(fifo, link); err != nil {
		t.Fatal(err)
	}
	err = callWithin(t, time.Second, func() error {
		opened, _, openErr := openArchiveRegular(link)
		if opened != nil {
			_ = opened.Close()
			return errors.New("symlink descriptor was returned")
		}
		if !errors.Is(openErr, unix.ELOOP) {
			return openErr
		}
		return nil
	})
	if err != nil {
		t.Fatalf("symlink to FIFO: %v", err)
	}

	dev := filepath.Join(dir, "dev")
	if err := os.MkdirAll(dev, 0o700); err != nil {
		t.Fatal(err)
	}
	created, rootOwned := createArchiveCharNodes(t, dev)
	if len(rootOwned) > 0 {
		t.Cleanup(func() {
			args := append([]string{"-n", "rm", "-f"}, rootOwned...)
			_ = exec.Command("sudo", args...).Run()
		})
	}
	if created == 0 {
		t.Log("char device nodes unavailable; FIFO and symlink checks already ran")
		return
	}
	for _, name := range []string{"null", "zero", "random", "urandom", "console"} {
		path := filepath.Join(dev, name)
		st, statErr := os.Lstat(path)
		if statErr != nil || st.Mode()&os.ModeCharDevice == 0 {
			continue
		}
		var before, after int64
		var haveRChar bool
		if name == "zero" || name == "urandom" || name == "random" || name == "console" {
			before, haveRChar = linuxRChar()
		}
		err := callWithin(t, time.Second, func() error {
			opened, _, openErr := openArchiveRegular(path)
			if opened != nil {
				_ = opened.Close()
				return errors.New("char device descriptor was returned")
			}
			if !errors.Is(openErr, errArchiveNotRegular) {
				return openErr
			}
			return nil
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if haveRChar {
			var ok bool
			after, ok = linuxRChar()
			haveRChar = ok
		}
		if haveRChar {
			t.Logf("%s rchar delta=%d", name, after-before)
			// Sampling /proc/self/io accounts for about 100 bytes. A device read
			// uses a poll buffer (typically 32KiB) and would blow past this.
			if after-before > 1024 {
				t.Fatalf("%s: rchar increased by %d while rejecting the device; payload was read", name, after-before)
			}
		}
	}
}

func callWithin(t *testing.T, limit time.Duration, fn func() error) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- fn()
	}()
	select {
	case err := <-errCh:
		return err
	case <-time.After(limit):
		t.Fatalf("still blocked after %s", limit)
		return errors.New("timeout")
	}
}

func linuxRChar() (int64, bool) {
	if runtime.GOOS != "linux" {
		return 0, false
	}
	raw, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "rchar:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, false
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

func assertLinuxRegularFDBlocking(t *testing.T, path string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		link, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err != nil || link != path {
			continue
		}
		info, err := os.ReadFile("/proc/self/fdinfo/" + entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(info), "\n") {
			if !strings.HasPrefix(line, "flags:") {
				continue
			}
			fields := strings.Fields(line)
			flags, err := strconv.ParseUint(fields[1], 8, 64)
			if err != nil {
				t.Fatal(err)
			}
			const oNonblock = 0x800
			if flags&oNonblock != 0 {
				t.Fatalf("regular file fd still O_NONBLOCK: %#o", flags)
			}
			return
		}
	}
	t.Fatal("regular file fd not found")
}

func keys(m map[string]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
