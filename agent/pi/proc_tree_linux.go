//go:build linux

package pi

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// forceKillDescendants sends SIGKILL to every process currently descended
// from rootPID, deepest children first. Process-group signalling alone is not
// sufficient for Pi: its bash tool starts commands with setsid(2), placing
// them in a separate session and process group.
//
// /proc is sampled before the root process is killed. This ordering matters:
// after rootPID exits, detached children are re-parented to the user service
// manager and there is no safe way to distinguish them from unrelated work.
func forceKillDescendants(rootPID int) error {
	pids, err := descendantPIDs(rootPID)
	if err != nil {
		return err
	}

	var errs []error
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil &&
			!errors.Is(err, os.ErrProcessDone) &&
			!errors.Is(err, syscall.ESRCH) {
			errs = append(errs, fmt.Errorf("kill descendant %d: %w", pid, err))
		}
	}
	return errors.Join(errs...)
}

// descendantPIDs returns descendants in post-order, so callers signal leaves
// before their parents. It builds a point-in-time PPID graph from /proc rather
// than relying on process groups, which detached tool subprocesses do not
// share with Pi.
func descendantPIDs(rootPID int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("read /proc: %w", err)
	}

	children := make(map[int][]int)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		ppid, ok := procParentPID(pid)
		if !ok {
			continue
		}
		children[ppid] = append(children[ppid], pid)
	}

	seen := map[int]bool{rootPID: true}
	var result []int
	var visit func(int)
	visit = func(pid int) {
		for _, child := range children[pid] {
			if seen[child] {
				continue
			}
			seen[child] = true
			visit(child)
			result = append(result, child)
		}
	}
	visit(rootPID)
	return result, nil
}

func procParentPID(pid int) (int, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		// Processes routinely disappear while /proc is being scanned.
		return 0, false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "PPid:") {
			continue
		}
		ppid, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")))
		return ppid, err == nil
	}
	return 0, false
}
