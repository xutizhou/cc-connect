package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

func TestAppServerSession_ApplyThreadRuntimeState(t *testing.T) {
	s := &appServerSession{}
	effort := "xhigh"

	s.applyThreadRuntimeState("/tmp/project", "gpt-5.4", &effort)

	if got := s.GetWorkDir(); got != "/tmp/project" {
		t.Fatalf("GetWorkDir() = %q, want /tmp/project", got)
	}
	if got := s.GetModel(); got != "gpt-5.4" {
		t.Fatalf("GetModel() = %q, want gpt-5.4", got)
	}
	if got := s.GetReasoningEffort(); got != "xhigh" {
		t.Fatalf("GetReasoningEffort() = %q, want xhigh", got)
	}
}

func TestAppServerSession_ApplyResumedThreadRuntimeStatePreservesExplicitOverrides(t *testing.T) {
	s := &appServerSession{
		model:  "gpt-6-astra",
		effort: "xhigh",
	}
	oldEffort := "medium"

	s.applyResumedThreadRuntimeState("/tmp/maas", "gpt-5.6-sol", &oldEffort)

	if got := s.GetWorkDir(); got != "/tmp/maas" {
		t.Fatalf("GetWorkDir() = %q, want /tmp/maas", got)
	}
	if got := s.GetModel(); got != "gpt-6-astra" {
		t.Fatalf("GetModel() = %q, want explicit override gpt-6-astra", got)
	}
	if got := s.GetReasoningEffort(); got != "xhigh" {
		t.Fatalf("GetReasoningEffort() = %q, want explicit override xhigh", got)
	}
}

func TestAppServerSession_ApplyResumedThreadRuntimeStateAdoptsThreadDefaults(t *testing.T) {
	s := &appServerSession{}
	effort := "high"

	s.applyResumedThreadRuntimeState("/tmp/project", "gpt-5.6-sol", &effort)

	if got := s.GetModel(); got != "gpt-5.6-sol" {
		t.Fatalf("GetModel() = %q, want resumed thread model gpt-5.6-sol", got)
	}
	if got := s.GetReasoningEffort(); got != "high" {
		t.Fatalf("GetReasoningEffort() = %q, want resumed thread effort high", got)
	}
}

func TestAppServerSession_ReadLoopAcceptsResponseLargerThanTenMiB(t *testing.T) {
	const responseID int64 = 42
	largeValue := strings.Repeat("x", 11*1024*1024)
	wire, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      responseID,
		"result":  map[string]any{"history": largeValue},
	})
	if err != nil {
		t.Fatalf("marshal large response: %v", err)
	}
	wire = append(wire, '\n')

	responseCh := make(chan rpcResponseEnvelope, 1)
	s := &appServerSession{
		ctx:              context.Background(),
		pending:          map[int64]chan rpcResponseEnvelope{responseID: responseCh},
		pendingApprovals: make(map[string]chan core.PermissionResult),
		events:           make(chan core.Event, 1),
	}
	s.alive.Store(true)
	s.wg.Add(1)
	go s.readLoop(bytes.NewReader(wire))

	select {
	case resp := <-responseCh:
		if resp.Error != nil {
			t.Fatalf("large response failed: %v", resp.Error)
		}
		var result struct {
			History string `json:"history"`
		}
		if err := json.Unmarshal(resp.Result, &result); err != nil {
			t.Fatalf("decode large result: %v", err)
		}
		if len(result.History) != len(largeValue) {
			t.Fatalf("history length = %d, want %d", len(result.History), len(largeValue))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response larger than 10 MiB")
	}
	s.wg.Wait()
}

func TestAppServerSession_HandleRateLimitsUpdatedCachesUsage(t *testing.T) {
	s := &appServerSession{}
	raw, err := json.Marshal(appServerRateLimitsResponse{
		RateLimits: appServerRateLimitSnapshot{
			LimitID:   "codex",
			PlanType:  "pro",
			Primary:   &appServerRateLimitWindow{UsedPercent: 25, WindowDurationMins: 15, ResetsAt: 1730947200},
			Secondary: &appServerRateLimitWindow{UsedPercent: 42, WindowDurationMins: 60, ResetsAt: 1730950800},
			Credits:   &appServerCreditsSnapshot{HasCredits: true, Unlimited: false},
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	s.handleNotification("account/rateLimits/updated", raw)

	report, err := s.GetUsage(context.Background())
	if err != nil {
		t.Fatalf("GetUsage() returned error: %v", err)
	}
	if report.Provider != "codex" {
		t.Fatalf("provider = %q, want codex", report.Provider)
	}
	if report.Plan != "pro" {
		t.Fatalf("plan = %q, want pro", report.Plan)
	}
	if len(report.Buckets) != 1 {
		t.Fatalf("buckets = %d, want 1", len(report.Buckets))
	}
	if got := report.Buckets[0].Name; got != "codex" {
		t.Fatalf("bucket name = %q, want codex", got)
	}
	if got := report.Buckets[0].Windows[0].WindowSeconds; got != 15*60 {
		t.Fatalf("primary window seconds = %d, want %d", got, 15*60)
	}
	if got := report.Buckets[0].Windows[1].UsedPercent; got != 42 {
		t.Fatalf("secondary used percent = %d, want 42", got)
	}
	if report.Credits == nil || !report.Credits.HasCredits {
		t.Fatalf("credits = %#v, want has credits", report.Credits)
	}
}

func TestAppServerSession_HandleThreadTokenUsageUpdatedCachesContextUsage(t *testing.T) {
	s := &appServerSession{}
	s.threadID.Store("thread-1")
	raw, err := json.Marshal(appServerThreadTokenUsageNotification{
		ThreadID: "thread-1",
		TurnID:   "turn-1",
		TokenUsage: struct {
			Total              codexTokenUsage `json:"total"`
			Last               codexTokenUsage `json:"last"`
			ModelContextWindow int             `json:"modelContextWindow"`
		}{
			Total: codexTokenUsage{
				TotalTokens:           52011395,
				InputTokens:           51847383,
				CachedInputTokens:     48187904,
				OutputTokens:          164012,
				ReasoningOutputTokens: 78910,
			},
			Last: codexTokenUsage{
				TotalTokens:           41061,
				InputTokens:           40849,
				CachedInputTokens:     36864,
				OutputTokens:          212,
				ReasoningOutputTokens: 32,
			},
			ModelContextWindow: 258400,
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}

	s.handleNotification("thread/tokenUsage/updated", raw)

	usage := s.GetContextUsage()
	if usage == nil {
		t.Fatal("GetContextUsage() = nil, want cached context usage")
	}
	if usage.UsedTokens != 41061 {
		t.Fatalf("used tokens = %d, want 41061", usage.UsedTokens)
	}
	if usage.BaselineTokens != codexContextBaselineTokens {
		t.Fatalf("baseline tokens = %d, want %d", usage.BaselineTokens, codexContextBaselineTokens)
	}
	if usage.TotalTokens != 41061 {
		t.Fatalf("total tokens = %d, want 41061", usage.TotalTokens)
	}
	if usage.ContextWindow != 258400 {
		t.Fatalf("context window = %d, want 258400", usage.ContextWindow)
	}
	if usage.CachedInputTokens != 36864 {
		t.Fatalf("cached input tokens = %d, want 36864", usage.CachedInputTokens)
	}
	if usage.InputTokens != 40849 {
		t.Fatalf("input tokens = %d, want 40849", usage.InputTokens)
	}
}

func TestAppServerSession_FailedTurnEmitsError(t *testing.T) {
	s := &appServerSession{
		events:      make(chan core.Event, 2),
		currentTurn: "turn-1",
		pendingMsgs: []string{"partial reasoning"},
	}
	s.threadID.Store("thread-1")

	raw, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"turn": map[string]any{
			"id":     "turn-1",
			"status": "failed",
			"error":  map[string]any{"message": "service unavailable"},
		},
	})
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	s.handleNotification("turn/completed", raw)

	event := <-s.events
	if event.Type != core.EventError || event.Error == nil || event.Error.Error() != "service unavailable" {
		t.Fatalf("event = %#v, want EventError with service unavailable", event)
	}

	s.stateMu.Lock()
	currentTurn := s.currentTurn
	pendingCount := len(s.pendingMsgs)
	s.stateMu.Unlock()
	if currentTurn != "" {
		t.Fatalf("current turn = %q after failure, want empty", currentTurn)
	}
	if pendingCount != 0 {
		t.Fatalf("pending message count = %d after failure, want 0", pendingCount)
	}

	idleRaw, err := json.Marshal(map[string]any{
		"threadId": "thread-1",
		"status":   map[string]any{"type": "idle"},
	})
	if err != nil {
		t.Fatalf("marshal idle notification: %v", err)
	}
	s.handleNotification("thread/status/changed", idleRaw)
	select {
	case duplicate := <-s.events:
		t.Fatalf("idle notification emitted duplicate event %#v", duplicate)
	default:
	}
}

func TestAppServerSession_TerminalEventWaitsForQueueSpace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &appServerSession{
		ctx:    ctx,
		events: make(chan core.Event, 1),
	}
	s.events <- core.Event{Type: core.EventThinking, Content: "queued progress"}

	emitted := make(chan struct{})
	go func() {
		s.emit(core.Event{Type: core.EventResult, Content: "done", Done: true})
		close(emitted)
	}()

	select {
	case <-emitted:
		t.Fatal("terminal event was dropped instead of waiting for queue space")
	case <-time.After(20 * time.Millisecond):
	}

	<-s.events
	select {
	case <-emitted:
	case <-time.After(time.Second):
		t.Fatal("terminal event did not resume after queue space became available")
	}
	event := <-s.events
	if event.Type != core.EventResult || event.Content != "done" {
		t.Fatalf("terminal event = %#v, want EventResult done", event)
	}
}

func appServerNotificationProbe(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal notification: %v", err)
	}
	return raw
}

func TestAppServerSession_MirrorsExternalCommentaryBeforeTurnCompletes(t *testing.T) {
	s := &appServerSession{events: make(chan core.Event, 8)}
	s.threadID.Store("thread-maas")

	s.handleNotification("turn/started", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turn":     map[string]any{"id": "turn-vm", "status": "inProgress"},
	}))
	s.handleNotification("item/completed", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turnId":   "turn-vm",
		"item": map[string]any{
			"type": "userMessage",
			"content": []any{map[string]any{
				"type": "text", "text": "run the matrix",
			}},
		},
	}))
	user := <-s.events
	if user.Type != core.EventUserMessage || user.Content != "run the matrix" {
		t.Fatalf("mirrored VM prompt = %#v", user)
	}

	s.handleNotification("item/started", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turnId":   "turn-vm",
		"item": map[string]any{
			"id": "commentary-1", "type": "agentMessage", "phase": "commentary",
		},
	}))
	s.handleNotification("item/agentMessage/delta", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turnId":   "turn-vm",
		"itemId":   "commentary-1",
		"delta":    "8K/C64 ",
	}))
	delta := <-s.events
	if delta.Type != core.EventThinking || delta.Content != "8K/C64 " || delta.Done {
		t.Fatalf("mirrored VM commentary delta = %#v", delta)
	}

	s.handleNotification("item/completed", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turnId":   "turn-vm",
		"item": map[string]any{
			"id": "commentary-1", "type": "agentMessage", "phase": "commentary", "text": "8K/C64 finished",
		},
	}))
	progress := <-s.events
	if progress.Type != core.EventThinking || progress.Content != "8K/C64 finished" || !progress.Done {
		t.Fatalf("mirrored VM commentary = %#v", progress)
	}

	s.handleNotification("item/completed", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turnId":   "turn-vm",
		"item": map[string]any{
			"type": "agentMessage", "phase": "final_answer", "text": "matrix done",
		},
	}))
	s.handleNotification("turn/completed", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-maas",
		"turn":     map[string]any{"id": "turn-vm", "status": "completed"},
	}))
	result := <-s.events
	if result.Type != core.EventResult || result.Content != "matrix done" || !result.Done {
		t.Fatalf("mirrored VM result = %#v", result)
	}
	if got, _ := result.Metadata["agent_turn_id"].(string); got != "turn-vm" {
		t.Fatalf("mirrored result cursor = %q, want turn-vm", got)
	}
}

func TestAppServerSession_IgnoresNotificationsForAnotherThread(t *testing.T) {
	s := &appServerSession{events: make(chan core.Event, 4)}
	s.threadID.Store("thread-maas")
	s.handleNotification("turn/started", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-cake",
		"turn":     map[string]any{"id": "turn-cake", "status": "inProgress"},
	}))
	s.handleNotification("item/completed", appServerNotificationProbe(t, map[string]any{
		"threadId": "thread-cake",
		"turnId":   "turn-cake",
		"item": map[string]any{
			"type": "agentMessage", "phase": "commentary", "text": "wrong session",
		},
	}))
	select {
	case event := <-s.events:
		t.Fatalf("cross-thread notification leaked: %#v", event)
	default:
	}
	s.stateMu.Lock()
	turn := s.currentTurn
	s.stateMu.Unlock()
	if turn != "" {
		t.Fatalf("cross-thread turn changed local state to %q", turn)
	}
}

func TestSeedRecoveredActiveTurnPublishesLatestCheckpoint(t *testing.T) {
	s := &appServerSession{events: make(chan core.Event, 4)}
	s.seedRecoveredActiveTurn(appServerTurnSnapshot{
		ID:     "turn-active",
		Status: "inProgress",
		Items: []map[string]any{
			{"type": "agentMessage", "phase": "commentary", "text": "old checkpoint"},
			{"type": "agentMessage", "phase": "commentary", "text": "latest checkpoint"},
		},
	})
	event := <-s.events
	if event.Type != core.EventThinking || event.Content != "latest checkpoint" {
		t.Fatalf("recovered checkpoint = %#v", event)
	}
	s.stateMu.Lock()
	turn, local := s.currentTurn, s.currentTurnLocal
	s.stateMu.Unlock()
	if turn != "turn-active" || local {
		t.Fatalf("recovered turn state = (%q, local=%v)", turn, local)
	}
}

func TestAppServerSession_TransientIdleDoesNotCompleteWithoutFinalAnswer(t *testing.T) {
	s := &appServerSession{
		events:           make(chan core.Event, 8),
		currentTurn:      "turn-goal",
		currentTurnLocal: true,
	}
	s.threadID.Store("thread-goal")

	s.handleItemCompleted(map[string]any{
		"type":  "agentMessage",
		"text":  "still investigating",
		"phase": "commentary",
	})

	idleRaw, err := json.Marshal(map[string]any{
		"threadId": "thread-goal",
		"status":   map[string]any{"type": "idle"},
	})
	if err != nil {
		t.Fatalf("marshal idle notification: %v", err)
	}
	s.handleNotification("thread/status/changed", idleRaw)

	select {
	case event := <-s.events:
		t.Fatalf("transient idle emitted terminal event %#v", event)
	default:
	}
	s.stateMu.Lock()
	currentTurn := s.currentTurn
	pendingCount := len(s.pendingMsgs)
	s.stateMu.Unlock()
	if currentTurn != "turn-goal" {
		t.Fatalf("current turn = %q after transient idle, want turn-goal", currentTurn)
	}
	if pendingCount != 1 {
		t.Fatalf("pending message count = %d after transient idle, want 1", pendingCount)
	}
}

func TestAppServerSession_FinalAnswerCompletesAfterTransientIdle(t *testing.T) {
	s := &appServerSession{
		events:           make(chan core.Event, 8),
		currentTurn:      "turn-goal",
		currentTurnLocal: true,
	}
	s.threadID.Store("thread-goal")

	s.handleItemCompleted(map[string]any{
		"type":  "agentMessage",
		"text":  "still investigating",
		"phase": "commentary",
	})
	s.handleItemStarted(map[string]any{
		"type":    "commandExecution",
		"command": "go test ./...",
	})

	first := <-s.events
	second := <-s.events
	if first.Type != core.EventThinking || second.Type != core.EventToolUse {
		t.Fatalf("continued events = %#v, %#v; want thinking then tool use", first, second)
	}

	s.handleItemCompleted(map[string]any{
		"type":  "agentMessage",
		"text":  "all checks passed",
		"phase": "final_answer",
	})
	completedRaw, err := json.Marshal(map[string]any{
		"threadId": "thread-goal",
		"turn": map[string]any{
			"id":     "turn-goal",
			"status": "completed",
		},
	})
	if err != nil {
		t.Fatalf("marshal completed notification: %v", err)
	}
	s.handleNotification("turn/completed", completedRaw)

	resultEvent := <-s.events
	if resultEvent.Type != core.EventResult || !resultEvent.Done || resultEvent.Content != "all checks passed" {
		t.Fatalf("result event = %#v, want terminal result", resultEvent)
	}
	select {
	case extra := <-s.events:
		t.Fatalf("unexpected event before terminal result delivery: %#v", extra)
	default:
	}
}

func TestAppServerSession_CompletedTurnWithoutFinalAnswerEmitsError(t *testing.T) {
	s := &appServerSession{
		events:      make(chan core.Event, 4),
		currentTurn: "turn-empty",
	}
	s.threadID.Store("thread-empty")

	completedRaw, err := json.Marshal(map[string]any{
		"threadId": "thread-empty",
		"turn": map[string]any{
			"id":     "turn-empty",
			"status": "completed",
		},
	})
	if err != nil {
		t.Fatalf("marshal completed notification: %v", err)
	}
	s.handleNotification("turn/completed", completedRaw)

	event := <-s.events
	if event.Type != core.EventError || event.Error == nil || !strings.Contains(event.Error.Error(), "without a final answer") {
		t.Fatalf("event = %#v, want missing-final EventError", event)
	}
}

func TestAppServerSession_CompletedTurnAcceptsLegacyUnphasedMessage(t *testing.T) {
	s := &appServerSession{
		events:      make(chan core.Event, 4),
		currentTurn: "turn-legacy",
	}
	s.threadID.Store("thread-legacy")
	s.handleItemCompleted(map[string]any{
		"type": "agentMessage",
		"text": "legacy provider answer",
	})

	completedRaw, err := json.Marshal(map[string]any{
		"threadId": "thread-legacy",
		"turn": map[string]any{
			"id":     "turn-legacy",
			"status": "completed",
		},
	})
	if err != nil {
		t.Fatalf("marshal completed notification: %v", err)
	}
	s.handleNotification("turn/completed", completedRaw)

	resultEvent := <-s.events
	if resultEvent.Type != core.EventResult || !resultEvent.Done || resultEvent.Content != "legacy provider answer" {
		t.Fatalf("result event = %#v, want terminal result", resultEvent)
	}
	select {
	case extra := <-s.events:
		t.Fatalf("unexpected event before terminal result delivery: %#v", extra)
	default:
	}
}

func TestAppServerSession_RequestTimeoutIncludesBlockedStdinWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := newBlockingWriteCloser()
	defer func() { _ = stdin.Close() }()

	s := &appServerSession{
		ctx:     ctx,
		cancel:  cancel,
		events:  make(chan core.Event),
		stdin:   stdin,
		pending: make(map[int64]chan rpcResponseEnvelope),
	}

	done := make(chan error, 1)
	go func() {
		var out map[string]any
		done <- s.requestWithTimeout("turn/start", map[string]any{
			"input": strings.Repeat("x", 1024),
		}, &out, 25*time.Millisecond)
	}()

	select {
	case <-stdin.started:
	case <-time.After(time.Second):
		t.Fatal("request did not attempt to write to stdin")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("requestWithTimeout returned nil, want write timeout")
		}
		if !strings.Contains(err.Error(), "turn/start") || !strings.Contains(err.Error(), "write timed out") {
			t.Fatalf("error = %q, want turn/start write timeout", err.Error())
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("requestWithTimeout did not return while stdin write was blocked")
	}

	if !stdin.Closed() {
		t.Fatal("blocked stdin was not closed after timeout")
	}
}

func TestAppServerSession_SteerTargetsActiveTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &lockedWriteCloser{}
	s := &appServerSession{
		ctx:         ctx,
		cancel:      cancel,
		events:      make(chan core.Event, 2),
		stdin:       stdin,
		pending:     make(map[int64]chan rpcResponseEnvelope),
		currentTurn: "turn-active",
	}
	s.threadID.Store("thread-active")
	s.alive.Store(true)

	done := make(chan error, 1)
	go func() {
		done <- s.Steer("focus on the failing test", "msg-2", nil, nil)
	}()

	line := waitForWrittenJSONLine(t, stdin)
	var req struct {
		ID     int64  `json:"id"`
		Method string `json:"method"`
		Params struct {
			ThreadID            string           `json:"threadId"`
			ExpectedTurnID      string           `json:"expectedTurnId"`
			ClientUserMessageID string           `json:"clientUserMessageId"`
			Input               []map[string]any `json:"input"`
		} `json:"params"`
	}
	if err := json.Unmarshal([]byte(line), &req); err != nil {
		t.Fatalf("decode steering request %q: %v", line, err)
	}
	if req.Method != "turn/steer" || req.Params.ThreadID != "thread-active" || req.Params.ExpectedTurnID != "turn-active" {
		t.Fatalf("steering request = %#v", req)
	}
	if req.Params.ClientUserMessageID != "msg-2" {
		t.Fatalf("clientUserMessageId = %q, want msg-2", req.Params.ClientUserMessageID)
	}
	if len(req.Params.Input) != 1 || req.Params.Input[0]["type"] != "text" || req.Params.Input[0]["text"] != "focus on the failing test" {
		t.Fatalf("steering input = %#v", req.Params.Input)
	}

	result, err := json.Marshal(map[string]any{"turnId": "turn-active"})
	if err != nil {
		t.Fatal(err)
	}
	s.handleResponse(rpcResponseEnvelope{ID: req.ID, Result: result})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Steer() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Steer() did not return after app-server acknowledgement")
	}

	s.stateMu.Lock()
	activeTurn := s.currentTurn
	s.stateMu.Unlock()
	if activeTurn != "turn-active" {
		t.Fatalf("Steer changed active turn to %q", activeTurn)
	}
}

func TestAppServerSession_SteerRejectsIdleSession(t *testing.T) {
	s := &appServerSession{}
	s.threadID.Store("thread-idle")
	s.alive.Store(true)
	if err := s.Steer("too late", "msg-idle", nil, nil); err == nil || !strings.Contains(err.Error(), "no active turn") {
		t.Fatalf("Steer() error = %v, want no active turn", err)
	}
}

func TestMapAppServerRateLimits_PrefersMultiBucketView(t *testing.T) {
	report := mapAppServerRateLimits(appServerRateLimitsResponse{
		RateLimits: appServerRateLimitSnapshot{
			LimitID:  "legacy",
			PlanType: "team",
			Primary:  &appServerRateLimitWindow{UsedPercent: 99, WindowDurationMins: 15},
		},
		RateLimitsByLimitID: map[string]appServerRateLimitSnapshot{
			"codex": {
				LimitID:   "codex",
				LimitName: "Codex",
				PlanType:  "team",
				Primary:   &appServerRateLimitWindow{UsedPercent: 10, WindowDurationMins: 15},
			},
			"codex_other": {
				LimitID:  "codex_other",
				PlanType: "team",
				Primary:  &appServerRateLimitWindow{UsedPercent: 20, WindowDurationMins: 60},
			},
		},
	})

	if report.Plan != "team" {
		t.Fatalf("plan = %q, want team", report.Plan)
	}
	if len(report.Buckets) != 2 {
		t.Fatalf("buckets = %d, want 2", len(report.Buckets))
	}
	if report.Buckets[0].Name != "Codex" {
		t.Fatalf("first bucket = %q, want Codex", report.Buckets[0].Name)
	}
	if report.Buckets[1].Name != "codex_other" {
		t.Fatalf("second bucket = %q, want codex_other", report.Buckets[1].Name)
	}
}

func TestAppServerSession_HandleRequestUserInputEmitsAskQuestion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &lockedWriteCloser{}
	s := &appServerSession{
		events:           make(chan core.Event, 4),
		ctx:              ctx,
		pendingApprovals: make(map[string]chan core.PermissionResult),
		stdin:            stdin,
	}

	s.handleServerRequest(serverRequestProbe(t, `"rui-1"`, "item/tool/requestUserInput", map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"itemId":   "call-1",
		"questions": []any{
			map[string]any{
				"id":       "database",
				"header":   "Database",
				"question": "Which database should we use?",
				"isOther":  true,
				"isSecret": false,
				"options": []any{
					map[string]any{"label": "Postgres", "description": "Use the existing relational database"},
					map[string]any{"label": "SQLite", "description": "Keep it embedded"},
				},
			},
		},
	}))

	var event core.Event
	select {
	case event = <-s.events:
	case <-time.After(time.Second):
		t.Fatal("expected AskUserQuestion event")
	}
	if event.Type != core.EventPermissionRequest {
		t.Fatalf("event type = %s, want %s", event.Type, core.EventPermissionRequest)
	}
	if event.ToolName != "AskUserQuestion" {
		t.Fatalf("tool name = %q, want AskUserQuestion", event.ToolName)
	}
	if event.RequestID != `"rui-1"` {
		t.Fatalf("request id = %q, want raw JSON id", event.RequestID)
	}
	if len(event.Questions) != 1 {
		t.Fatalf("questions = %d, want 1", len(event.Questions))
	}
	q := event.Questions[0]
	if q.Question != "Which database should we use?" || q.Header != "Database" {
		t.Fatalf("question = %#v", q)
	}
	if len(q.Options) != 2 || q.Options[0].Label != "Postgres" || q.Options[1].Description != "Keep it embedded" {
		t.Fatalf("options = %#v", q.Options)
	}
	if stdin.String() != "" {
		t.Fatalf("request_user_input should not write before the answer, got %q", stdin.String())
	}
}

func TestAppServerSession_HandleRequestUserInputWritesCodexResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &lockedWriteCloser{}
	s := &appServerSession{
		events:           make(chan core.Event, 4),
		ctx:              ctx,
		pendingApprovals: make(map[string]chan core.PermissionResult),
		stdin:            stdin,
	}

	s.handleServerRequest(serverRequestProbe(t, `"rui-2"`, "item/tool/requestUserInput", map[string]any{
		"threadId": "thread-1",
		"turnId":   "turn-1",
		"itemId":   "call-2",
		"questions": []any{
			map[string]any{
				"id":       "database",
				"header":   "Database",
				"question": "Which database should we use?",
				"options": []any{
					map[string]any{"label": "Postgres", "description": "Use the existing relational database"},
					map[string]any{"label": "SQLite", "description": "Keep it embedded"},
				},
			},
		},
	}))

	var event core.Event
	select {
	case event = <-s.events:
	case <-time.After(time.Second):
		t.Fatal("expected AskUserQuestion event")
	}
	if err := s.RespondPermission(event.RequestID, core.PermissionResult{
		Behavior: "allow",
		UpdatedInput: map[string]any{
			"answers": map[string]any{
				"Which database should we use?": "Postgres",
			},
		},
	}); err != nil {
		t.Fatalf("RespondPermission() error = %v", err)
	}

	line := waitForWrittenJSONLine(t, stdin)
	var envelope struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Result  struct {
			Answers map[string]struct {
				Answers []string `json:"answers"`
			} `json:"answers"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		t.Fatalf("decode response %q: %v", line, err)
	}
	if envelope.JSONRPC != "2.0" || envelope.ID != "rui-2" {
		t.Fatalf("envelope = %#v", envelope)
	}
	got := envelope.Result.Answers["database"].Answers
	if len(got) != 1 || got[0] != "Postgres" {
		t.Fatalf("answers[database] = %#v, want [Postgres]", got)
	}
}

var _ interface {
	GetUsage(context.Context) (*core.UsageReport, error)
} = (*appServerSession)(nil)

var _ interface {
	GetContextUsage() *core.ContextUsage
} = (*appServerSession)(nil)

var _ core.AgentSessionSteerer = (*appServerSession)(nil)
var _ core.AgentSessionCanceller = (*appServerSession)(nil)

type lockedWriteCloser struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriteCloser) Close() error { return nil }

func (w *lockedWriteCloser) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

var _ io.WriteCloser = (*lockedWriteCloser)(nil)

type blockingWriteCloser struct {
	started   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	isClosed bool
}

func newBlockingWriteCloser() *blockingWriteCloser {
	return &blockingWriteCloser{
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (w *blockingWriteCloser) Write([]byte) (int, error) {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockingWriteCloser) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.isClosed = true
		w.mu.Unlock()
		close(w.closed)
	})
	return nil
}

func (w *blockingWriteCloser) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isClosed
}

var _ io.WriteCloser = (*blockingWriteCloser)(nil)

func serverRequestProbe(t *testing.T, idJSON, method string, params any) map[string]json.RawMessage {
	t.Helper()
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	methodJSON, err := json.Marshal(method)
	if err != nil {
		t.Fatalf("marshal method: %v", err)
	}
	return map[string]json.RawMessage{
		"id":     json.RawMessage(idJSON),
		"method": methodJSON,
		"params": paramsJSON,
	}
}

func waitForWrittenJSONLine(t *testing.T, w *lockedWriteCloser) string {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for JSON response, buffer=%q", w.String())
		case <-ticker.C:
			for _, line := range strings.Split(w.String(), "\n") {
				line = strings.TrimSpace(line)
				if line != "" {
					return line
				}
			}
		}
	}
}
