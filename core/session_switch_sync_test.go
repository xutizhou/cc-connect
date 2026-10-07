package core

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type explicitSwitchAgent struct {
	restartRecoveryAgent
	target AgentSessionInfo
}

func (a *explicitSwitchAgent) LookupSession(_ context.Context, id string) (*AgentSessionInfo, error) {
	if id != a.target.ID {
		return nil, nil
	}
	copy := a.target
	return &copy, nil
}

// The VM thread is deliberately absent from ListSessions. The user selects
// its full ID, sees live output without sending a prompt, then checks history.
func TestCUJ_B19_SwitchExternalVMThreadImmediatelyMirrors(t *testing.T) {
	const key = "feishu:moe:user"
	const otherKey = "feishu:maas:user"
	const id = "01a0b257-f56b-7b10-95ce-0a4b86808e33"
	p := &restartRecoveryPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	vm := &restartRecoverySession{id: id, events: make(chan Event, 8)}
	agent := &explicitSwitchAgent{
		restartRecoveryAgent: restartRecoveryAgent{session: vm},
		target:               AgentSessionInfo{ID: id, Summary: "running MoE"},
	}
	e := NewEngine("switch-live", agent, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.SetDisplayConfig(DisplayCfg{Mode: "full", ThinkingMessages: true})
	defer e.Stop()
	e.markPlatformReady(p)
	old := e.sessions.GetOrCreateActive(key)
	old.SetAgentSessionID("old-moe", agent.Name())
	old.AddHistory("assistant", "old conversation preserved")

	// A second chat already has a live subscription. Switching MoE must not
	// rebuild that reader or consume/replay its history.
	other := e.sessions.GetOrCreateActive(otherKey)
	other.SetAgentSessionID("maas-thread", agent.Name())
	otherVM := &restartRecoverySession{id: "maas-thread", events: make(chan Event, 8)}
	otherState := &interactiveState{agentSession: otherVM, platform: p, replyCtx: "maas", eventsNeedResync: false}
	e.interactiveStates[otherKey] = otherState
	e.startUnsolicitedReader(otherState, other, e.sessions, otherKey, "")
	otherState.mu.Lock()
	otherReader := otherState.unsolicitedDone
	otherState.mu.Unlock()

	send := func(n int, command string) {
		e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "feishu", UserID: "user", MessageID: fmt.Sprint(n), Content: command, ReplyCtx: "moe"})
	}
	send(1, "/switch "+id)
	vm.events <- Event{Type: EventThinking, Content: "VM is benchmarking EP8 now"}
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(strings.Join(p.getSent(), "\n"), "benchmarking EP8") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := strings.Join(p.getSent(), "\n"); !strings.Contains(got, "running MoE") || !strings.Contains(got, "benchmarking EP8") {
		t.Fatalf("user did not see switch confirmation and in-flight VM output: %s", got)
	}
	send(2, "/current")
	vm.events <- Event{Type: EventResult, Content: "EP8 finished successfully", Done: true}
	current := e.sessions.GetOrCreateActive(key)
	deadline = time.Now().Add(3 * time.Second)
	for current.HistoryLen() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	send(3, "/history")
	if got := strings.Join(p.getSent(), "\n"); !strings.Contains(got, "EP8 finished successfully") {
		t.Fatalf("new conversation history missing: %s", got)
	}
	if current.GetAgentSessionID() != id || old.HistoryLen() != 1 || other.GetAgentSessionID() != "maas-thread" {
		t.Fatal("switch lost history or changed another chat's binding")
	}
	otherState.mu.Lock()
	unchanged := otherState.unsolicitedDone == otherReader
	otherState.mu.Unlock()
	if !unchanged {
		t.Fatal("switch restarted an unrelated live reader")
	}
	agent.mu.Lock()
	calls := append([]string(nil), agent.calls...)
	agent.mu.Unlock()
	if len(calls) != 1 || calls[0] != id {
		t.Fatalf("unexpected resumed sessions: %v", calls)
	}
}

func TestExplicitSwitchRespectsExternalSessionFilter(t *testing.T) {
	agent := &explicitSwitchAgent{target: AgentSessionInfo{ID: "full-external-id", Summary: "external"}}
	e := NewEngine("filter-switch", agent, nil, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	defer e.Stop()
	e.SetFilterExternalSessions(true)
	old := e.sessions.GetOrCreateActive("test:user")
	old.SetAgentSessionID("tracked", agent.Name())
	got, err := e.resolveSwitchSession(agent, e.sessions, "full-external-id")
	if err != nil || got != nil {
		t.Fatalf("explicit lookup bypassed filter: %#v, %v", got, err)
	}
}

func TestSwitchCardPreservesKnownHistory(t *testing.T) {
	agent := &stubListAgent{sessions: []AgentSessionInfo{{ID: "old-thread", Summary: "old"}}}
	e := NewEngine("card-switch", agent, nil, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	defer e.Stop()
	key := "test:user"
	old := e.sessions.GetOrCreateActive(key)
	old.SetAgentSessionID("old-thread", agent.Name())
	old.AddHistory("assistant", "keep this history")
	e.sessions.NewSession(key, "new")
	e.executeCardAction("/switch", "old-thread", key)
	if active := e.sessions.GetOrCreateActive(key); active.GetAgentSessionID() != "old-thread" || active.HistoryLen() != 1 {
		t.Fatal("switch card discarded the original conversation")
	}
}
