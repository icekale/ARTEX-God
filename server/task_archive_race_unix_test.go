//go:build unix

package server

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pgdb "github.com/Autumn-27/artex/db"
	"golang.org/x/sys/unix"
)

// TestOpenArchiveRegularRaceDoesNotBlock flips one directory entry between a
// regular file, a FIFO, and a symlink to a FIFO while the archive opener runs.
// Opening the FIFO without O_NONBLOCK, or following the symlink, blocks. The
// opener must return within the deadline, and a successful result must be the
// regular payload.
func TestOpenArchiveRegularRaceDoesNotBlock(t *testing.T) {
	dir := t.TempDir()
	slot := filepath.Join(dir, "slot")
	payload := []byte("regular-payload-race")
	if err := os.WriteFile(slot, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	side := filepath.Join(dir, "side.fifo")
	if err := unix.Mkfifo(side, 0o600); err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()

	stop := make(chan struct{})
	var once sync.Once
	stopReplacer := func() { once.Do(func() { close(stop) }) }
	var replacer sync.WaitGroup
	replacer.Add(1)
	go func() {
		defer replacer.Done()
		flipArchiveSlot(scratch, slot, payload, stop)
	}()
	t.Cleanup(func() {
		stopReplacer()
		replacer.Wait()
	})

	deadline := time.Now().Add(2 * time.Second)
	var opens, skips int
	for time.Now().Before(deadline) {
		kind, err := openArchiveRegularWithin(t, time.Second, slot, payload)
		if err != nil {
			stopReplacer()
			t.Fatal(err)
		}
		switch kind {
		case "regular":
			opens++
		case "skip":
			skips++
		default:
			stopReplacer()
			t.Fatalf("unexpected open result %q", kind)
		}
	}
	stopReplacer()
	replacer.Wait()
	if opens == 0 || skips == 0 {
		t.Fatalf("race did not observe both outcomes: regular=%d skipped=%d", opens, skips)
	}
	t.Logf("regular opens=%d skipped=%d", opens, skips)
}

// TestWriteTaskArchivePackageRaceDoesNotBlock runs the compressor walk while
// one payload entry flips between a regular file and a FIFO. The pre-fix
// os.Open of that FIFO blocks, so the .partial archive never finishes.
func TestWriteTaskArchivePackageRaceDoesNotBlock(t *testing.T) {
	payloadDir := t.TempDir()
	keep := []byte("keep-race")
	if err := os.WriteFile(filepath.Join(payloadDir, "keep.txt"), keep, 0o600); err != nil {
		t.Fatal(err)
	}
	slotPayload := []byte("slot-regular")
	slot := filepath.Join(payloadDir, "slot")
	if err := os.WriteFile(slot, slotPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(payloadDir, "side.fifo"), 0o600); err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()

	stop := make(chan struct{})
	var once sync.Once
	stopReplacer := func() { once.Do(func() { close(stop) }) }
	var replacer sync.WaitGroup
	replacer.Add(1)
	go func() {
		defer replacer.Done()
		flipArchiveSlot(scratch, slot, slotPayload, stop)
	}()
	t.Cleanup(func() {
		stopReplacer()
		replacer.Wait()
	})

	archivePath := filepath.Join(t.TempDir(), "race.tar.zst")
	extracted := t.TempDir()
	snapshot := &pgdb.TaskArchiveSnapshot{FormatVersion: pgdb.TaskArchiveFormatVersion, TaskID: 7}
	started := time.Now()
	err := callWithin(t, 5*time.Second, func() error {
		_, _, checksum, err := writeTaskArchivePackage(archivePath, payloadDir, snapshot)
		if err != nil {
			return err
		}
		if err := extractTaskArchivePackage(archivePath, checksum, extracted); err != nil {
			return err
		}
		body, err := os.ReadFile(filepath.Join(extracted, "keep.txt"))
		if err != nil {
			return err
		}
		if !bytes.Equal(body, keep) {
			return errors.New("keep.txt mismatch")
		}
		slotBody, err := os.ReadFile(filepath.Join(extracted, "slot"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil && !bytes.Equal(slotBody, slotPayload) {
			return errors.New("slot payload was not the regular file")
		}
		if _, err := os.Lstat(filepath.Join(extracted, "side.fifo")); !errors.Is(err, os.ErrNotExist) {
			return errors.New("side FIFO was archived")
		}
		return nil
	})
	stopReplacer()
	replacer.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatalf("archive race took %s", time.Since(started))
	}
}

func flipArchiveSlot(scratch, slot string, payload []byte, stop <-chan struct{}) {
	mode := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		// The temporary name stays outside the walked payload so the archiver
		// cannot observe a half-created FIFO and fail the package with ENOENT.
		next := filepath.Join(scratch, "next")
		_ = os.Remove(next)
		var err error
		switch mode % 3 {
		case 0:
			err = os.WriteFile(next, payload, 0o600)
		case 1:
			err = unix.Mkfifo(next, 0o600)
		default:
			err = os.Symlink("side.fifo", next)
		}
		mode++
		if err != nil {
			continue
		}
		if err := os.Rename(next, slot); err != nil {
			_ = os.Remove(next)
			continue
		}
		time.Sleep(200 * time.Microsecond)
	}
}

func openArchiveRegularWithin(t *testing.T, limit time.Duration, path string, payload []byte) (string, error) {
	t.Helper()
	type result struct {
		kind string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		file, info, err := openArchiveRegular(path)
		if err != nil {
			if archiveOpenSkippable(err) || errors.Is(err, os.ErrNotExist) {
				done <- result{kind: "skip"}
				return
			}
			done <- result{err: err}
			return
		}
		defer file.Close()
		if info == nil || !info.Mode().IsRegular() {
			done <- result{err: errors.New("open returned a non-regular file")}
			return
		}
		got, readErr := io.ReadAll(file)
		if readErr != nil {
			done <- result{err: readErr}
			return
		}
		if !bytes.Equal(got, payload) {
			done <- result{err: errors.New("regular payload mismatch")}
			return
		}
		done <- result{kind: "regular"}
	}()
	select {
	case got := <-done:
		return got.kind, got.err
	case <-time.After(limit):
		t.Fatalf("openArchiveRegular still blocked after %s", limit)
		return "", errors.New("timeout")
	}
}
