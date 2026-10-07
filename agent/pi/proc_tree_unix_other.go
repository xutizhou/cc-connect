//go:build unix && !linux

package pi

// Non-Linux Unix systems do not expose a portable process-descendant API.
// Keep the existing process-group cleanup there; Linux additionally walks
// /proc so it can terminate Pi bash tools that detach with setsid(2).
func forceKillDescendants(_ int) error {
	return nil
}
