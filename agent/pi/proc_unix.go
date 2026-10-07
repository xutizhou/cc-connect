//go:build unix

package pi

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// prepareCmdForKill puts the spawned child into its own process group so that
// the entire descendant tree can be terminated with a single signal aimed at
// the negative PID. Without this, cc-connect can only signal the direct child
// (the `pi` CLI), leaving any grandchildren (MCP server processes, tool
// subprocesses) as orphans after the parent is killed.
//
// Mirrors the pattern used by agent/claudecode/proc_unix.go and
// agent/codex/proc_unix.go.
func prepareCmdForKill(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// forceKillCmd SIGKILLs the entire process group rooted at cmd.
// Returns nil if the group is already gone.
func forceKillCmd(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	// Pi's bash tool deliberately starts commands in a new session. Those
	// commands are descendants of the Pi process, but no longer members of
	// Pi's process group, so a group-only kill leaves them running after an
	// idle timeout or /stop. Discover and signal descendants while the Pi
	// parent is still alive; once the parent exits detached descendants are
	// re-parented and can no longer be associated with this session safely.
	descendantErr := forceKillDescendants(cmd.Process.Pid)

	var groupErr error
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil &&
		!errors.Is(err, os.ErrProcessDone) &&
		!errors.Is(err, syscall.ESRCH) {
		groupErr = err
	}
	return errors.Join(descendantErr, groupErr)
}
