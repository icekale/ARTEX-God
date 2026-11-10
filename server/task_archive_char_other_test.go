//go:build unix && !linux

package server

import "testing"

func createArchiveCharNodes(t *testing.T, _ string) (created int, rootOwned []string) {
	t.Helper()
	t.Log("isolated char devices are created only on Linux; FIFO and socket coverage still runs")
	return 0, nil
}
