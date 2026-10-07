//go:build linux

package pi

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestForceKillCmd_KillsDetachedDescendants guards the Mega Kernel failure:
// Pi's bash tool starts a new session with setsid, so killing only Pi's
// process group leaves bash, git and git-lfs running under systemd --user.
func TestForceKillCmd_KillsDetachedDescendants(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c",
		`setsid /bin/sh -c 'sleep 60 & echo "$$ $!"; wait' & wait`)
	prepareCmdForKill(cmd)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		_ = forceKillCmd(cmd)
		_ = cmd.Wait()
		t.Fatalf("read detached PIDs: %v", err)
	}
	fields := strings.Fields(line)
	if len(fields) != 2 {
		_ = forceKillCmd(cmd)
		_ = cmd.Wait()
		t.Fatalf("detached PID line = %q, want two PIDs", line)
	}

	detachedPIDs := make([]int, 0, len(fields))
	for _, field := range fields {
		pid, err := strconv.Atoi(field)
		if err != nil {
			t.Fatalf("parse PID %q: %v", field, err)
		}
		detachedPIDs = append(detachedPIDs, pid)
	}
	t.Cleanup(func() {
		for _, pid := range detachedPIDs {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	if err := forceKillCmd(cmd); err != nil {
		t.Fatalf("forceKillCmd: %v", err)
	}
	_ = cmd.Wait()

	deadline := time.Now().Add(3 * time.Second)
	for _, pid := range detachedPIDs {
		for processAlive(pid) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if processAlive(pid) {
			t.Errorf("detached descendant %d survived forceKillCmd", pid)
		}
	}
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return false
	}
	if err != nil {
		return true
	}

	// A killed process can remain briefly as a zombie until its new parent
	// reaps it. It cannot execute or hold files, so treat that as terminated.
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "State:") {
			return !strings.Contains(line, "Z (zombie)")
		}
	}
	return true
}
