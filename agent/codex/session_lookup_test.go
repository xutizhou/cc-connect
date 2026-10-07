package codex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestLookupSessionAcrossDirectoriesIsExplicitAndReadOnly(t *testing.T) {
	const id = "01a0b257-f56b-7b10-95ce-0a4b86808e33"
	for _, tc := range []struct {
		name, query, backend, source, metaID string
		want                                 bool
	}{
		{"shared-full-id", id, "app_server", `"vscode"`, id, true},
		{"prefix", id[:12], "app_server", `"vscode"`, id, false},
		{"empty", "", "app_server", `"vscode"`, id, false},
		{"path", "../../sessions", "app_server", `"vscode"`, id, false},
		{"exec-backend", id, "exec", `"vscode"`, id, false},
		{"subagent", id, "app_server", `{"subagent":{"thread_spawn":{}}}`, id, false},
		{"wrong-metadata", id, "app_server", `"vscode"`, "different-id", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codexHome := t.TempDir()
			dir := filepath.Join(codexHome, "sessions", "2026", "09", "18")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "rollout-2026-09-18T02-28-24-"+id+".jsonl")
			body := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":%q,\"cwd\":\"/vm/other\",\"source\":%s}}\n", tc.metaID, tc.source)
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			a := &Agent{workDir: "/bridge/default", codexHome: codexHome, backend: tc.backend}
			got, err := a.LookupSession(context.Background(), tc.query)
			if err != nil || (got != nil) != tc.want {
				t.Fatalf("LookupSession = %#v, %v; want found=%v", got, err, tc.want)
			}
			if got != nil && got.ID != id {
				t.Fatalf("wrong session: %#v", got)
			}
			if a.workDir != "/bridge/default" {
				t.Fatal("lookup changed the shared agent directory")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != body {
				t.Fatal("lookup rewrote the VM rollout")
			}
			listed, err := a.ListSessions(context.Background())
			if err != nil || len(listed) != 0 {
				t.Fatalf("default picker no longer respects workspace: %#v, %v", listed, err)
			}
		})
	}
}
