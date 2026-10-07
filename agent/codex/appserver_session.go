package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type rpcResponseEnvelope struct {
	ID     any             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcNotificationEnvelope struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type initResponse struct {
	ProtocolVersion string `json:"protocolVersion"`
}

type threadStartResponse struct {
	Cwd             string  `json:"cwd"`
	Model           string  `json:"model"`
	ReasoningEffort *string `json:"reasoningEffort"`
	Thread          struct {
		ID string `json:"id"`
	} `json:"thread"`
}

type threadResumeResponse struct {
	Cwd             string  `json:"cwd"`
	Model           string  `json:"model"`
	ReasoningEffort *string `json:"reasoningEffort"`
	Thread          struct {
		ID string `json:"id"`
	} `json:"thread"`
}

type turnStartResponse struct {
	Turn struct {
		ID string `json:"id"`
	} `json:"turn"`
}

type turnSteerResponse struct {
	TurnID string `json:"turnId"`
}

type turnNotification struct {
	ThreadID string `json:"threadId"`
	Turn     struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"turn"`
}

type threadTurnsListResponse struct {
	Data       []appServerTurnSnapshot `json:"data"`
	NextCursor string                  `json:"nextCursor"`
}

type appServerTurnSnapshot struct {
	ID     string           `json:"id"`
	Status string           `json:"status"`
	Items  []map[string]any `json:"items"`
}

type itemNotification struct {
	ThreadID string         `json:"threadId"`
	TurnID   string         `json:"turnId"`
	Item     map[string]any `json:"item"`
}

type agentMessageDeltaNotification struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
	ItemID   string `json:"itemId"`
	Delta    string `json:"delta"`
}

type reasoningSummaryTextDeltaNotification struct {
	ThreadID     string `json:"threadId"`
	TurnID       string `json:"turnId"`
	ItemID       string `json:"itemId"`
	SummaryIndex int64  `json:"summaryIndex"`
	Delta        string `json:"delta"`
}

type errorNotification struct {
	ThreadID string `json:"threadId"`
	TurnID   string `json:"turnId"`
	Error    struct {
		Message string `json:"message"`
	} `json:"error"`
	// Message is kept for compatibility with older app-server builds that
	// emitted the error text at the top level.
	Message   string `json:"message"`
	WillRetry bool   `json:"willRetry"`
}

type appServerRateLimitsResponse struct {
	RateLimits          appServerRateLimitSnapshot            `json:"rateLimits"`
	RateLimitsByLimitID map[string]appServerRateLimitSnapshot `json:"rateLimitsByLimitId"`
}

type appServerRateLimitSnapshot struct {
	LimitID   string                    `json:"limitId"`
	LimitName string                    `json:"limitName"`
	PlanType  string                    `json:"planType"`
	Primary   *appServerRateLimitWindow `json:"primary"`
	Secondary *appServerRateLimitWindow `json:"secondary"`
	Credits   *appServerCreditsSnapshot `json:"credits"`
}

type appServerRateLimitWindow struct {
	UsedPercent        int   `json:"usedPercent"`
	WindowDurationMins int   `json:"windowDurationMins"`
	ResetsAt           int64 `json:"resetsAt"`
}

type appServerCreditsSnapshot struct {
	Balance    *string `json:"balance"`
	HasCredits bool    `json:"hasCredits"`
	Unlimited  bool    `json:"unlimited"`
}

type appServerRequestUserInputParams struct {
	ThreadID  string                              `json:"threadId"`
	TurnID    string                              `json:"turnId"`
	ItemID    string                              `json:"itemId"`
	Questions []appServerRequestUserInputQuestion `json:"questions"`
}

type appServerRequestUserInputQuestion struct {
	ID       string                            `json:"id"`
	Header   string                            `json:"header"`
	Question string                            `json:"question"`
	IsOther  bool                              `json:"isOther"`
	IsSecret bool                              `json:"isSecret"`
	Options  []appServerRequestUserInputOption `json:"options"`
}

type appServerRequestUserInputOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

type appServerRequestUserInputResponse struct {
	Answers map[string]appServerRequestUserInputAnswer `json:"answers"`
}

type appServerRequestUserInputAnswer struct {
	Answers []string `json:"answers"`
}

type appServerSession struct {
	url            string
	workDir        string
	model          string
	effort         string
	mode           string
	baseURL        string
	modelProvider  string
	extraEnv       []string
	codexHome      string
	promptPreamble string

	events chan core.Event

	ctx    context.Context
	cancel context.CancelFunc

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	procMu  sync.Mutex
	writeMu sync.Mutex

	nextID atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]chan rpcResponseEnvelope

	approvalsMu      sync.Mutex
	pendingApprovals map[string]chan core.PermissionResult

	threadID atomic.Value
	alive    atomic.Bool

	closeOnce sync.Once
	wg        sync.WaitGroup

	stateMu                 sync.Mutex
	pendingMsgs             []string
	currentTurn             string
	currentTurnLocal        bool
	localStartPending       bool
	pendingHasFinal         bool
	pendingHasUnknownPhase  bool
	agentMessagePhases      map[string]string
	reasoningSummaryIndexes map[string]int64
	preambleSent            bool

	runtimeMu sync.RWMutex
	usage     *core.UsageReport
	context   *core.ContextUsage
}

var _ core.AgentSessionRecovery = (*appServerSession)(nil)

const (
	appServerRequestTimeout      = 120 * time.Second
	appServerSteerTimeout        = 5 * time.Second
	appServerUsageRefreshTimeout = 1500 * time.Millisecond
)

func newAppServerSession(ctx context.Context, url, workDir, model, effort, mode, resumeID, baseURL, modelProvider string, extraEnv []string, codexHome string, systemPrompt string, appendPrompt string) (*appServerSession, error) {
	sessionCtx, cancel := context.WithCancel(ctx)
	s := &appServerSession{
		url:                     url,
		workDir:                 workDir,
		model:                   model,
		effort:                  effort,
		mode:                    mode,
		baseURL:                 baseURL,
		modelProvider:           modelProvider,
		extraEnv:                append([]string(nil), extraEnv...),
		codexHome:               strings.TrimSpace(codexHome),
		promptPreamble:          buildCodexPromptPreamble(systemPrompt, appendPrompt),
		events:                  make(chan core.Event, 128),
		ctx:                     sessionCtx,
		cancel:                  cancel,
		pending:                 make(map[int64]chan rpcResponseEnvelope),
		pendingApprovals:        make(map[string]chan core.PermissionResult),
		agentMessagePhases:      make(map[string]string),
		reasoningSummaryIndexes: make(map[string]int64),
		preambleSent:            resumeID != "" && resumeID != core.ContinueSession,
	}
	s.alive.Store(true)

	if err := s.connect(); err != nil {
		cancel()
		return nil, err
	}

	if err := s.initialize(); err != nil {
		_ = s.Close()
		return nil, err
	}

	if err := s.ensureThread(resumeID); err != nil {
		_ = s.Close()
		return nil, err
	}
	if err := s.refreshUsage(context.Background()); err != nil {
		slog.Debug("codex app-server: initial rate limit fetch failed", "error", err)
	}

	return s, nil
}

func (s *appServerSession) connect() error {
	args := []string{"app-server"}
	if strings.TrimSpace(s.url) != "" {
		args = append(args, "--listen", strings.TrimSpace(s.url))
	}
	if model := strings.TrimSpace(s.model); model != "" {
		args = append(args, "-c", fmt.Sprintf("model=%q", model))
	}
	if effort := strings.TrimSpace(s.effort); effort != "" {
		args = append(args, "-c", fmt.Sprintf("model_reasoning_effort=%q", effort))
	}
	if provider := strings.TrimSpace(s.modelProvider); provider != "" {
		args = append(args, "-c", fmt.Sprintf("model_provider=%q", provider))
	}
	if baseURL := strings.TrimSpace(s.baseURL); baseURL != "" {
		args = append(args, "-c", fmt.Sprintf("openai_base_url=%q", baseURL))
	}
	cmd := exec.CommandContext(s.ctx, "codex", args...)
	cmd.Dir = s.workDir
	env := append([]string(nil), s.extraEnv...)
	if s.codexHome != "" {
		env = append(env, "CODEX_HOME="+s.codexHome)
	}
	if len(env) > 0 {
		cmd.Env = core.MergeEnv(os.Environ(), env)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("codex app-server stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("codex app-server stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("codex app-server stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("codex app-server start: %w", err)
	}

	s.procMu.Lock()
	s.cmd = cmd
	s.stdin = stdin
	s.procMu.Unlock()

	slog.Info("codex app-server session started", "transport", "stdio", "pid", cmd.Process.Pid, "work_dir", s.workDir)

	s.wg.Add(3)
	go s.readLoop(stdout)
	go s.stderrLoop(stderr)
	go s.waitLoop()
	return nil
}

func (s *appServerSession) initialize() error {
	params := map[string]any{
		"clientInfo": map[string]any{
			"name":    "cc-connect-codex-agent",
			"title":   "CC Connect Codex Agent",
			"version": "0.1.0",
		},
		"capabilities": map[string]any{
			"experimentalApi": true,
			"optOutNotificationMethods": []string{
				"command/exec/outputDelta",
				"item/plan/delta",
				"item/fileChange/outputDelta",
				"item/reasoning/textDelta",
			},
		},
	}

	var resp initResponse
	if err := s.request("initialize", params, &resp); err != nil {
		return fmt.Errorf("codex app-server initialize: %w", err)
	}
	if err := s.notify("initialized", nil); err != nil {
		return fmt.Errorf("codex app-server initialized notify: %w", err)
	}
	return nil
}

func (s *appServerSession) ensureThread(resumeID string) error {
	if resumeID != "" && resumeID != core.ContinueSession {
		params := s.threadRequestParams()
		params["threadId"] = resumeID
		params["persistExtendedHistory"] = true

		var resp threadResumeResponse
		if err := s.request("thread/resume", params, &resp); err != nil {
			return err
		}
		if resp.Thread.ID == "" {
			return fmt.Errorf("codex app-server resume returned empty thread id")
		}
		s.applyResumedThreadRuntimeState(resp.Cwd, resp.Model, resp.ReasoningEffort)
		s.threadID.Store(resp.Thread.ID)
		slog.Info("codex app-server thread resumed", "thread_id", resp.Thread.ID)
		return nil
	}

	var resp threadStartResponse
	if err := s.request("thread/start", s.threadRequestParams(), &resp); err != nil {
		return err
	}
	if resp.Thread.ID == "" {
		return fmt.Errorf("codex app-server start returned empty thread id")
	}
	s.applyThreadRuntimeState(resp.Cwd, resp.Model, resp.ReasoningEffort)
	s.threadID.Store(resp.Thread.ID)
	slog.Info("codex app-server thread started", "thread_id", resp.Thread.ID)
	return nil
}

func (s *appServerSession) threadRequestParams() map[string]any {
	params := map[string]any{
		"experimentalRawEvents":  false,
		"persistExtendedHistory": false,
	}
	if model := s.GetModel(); model != "" {
		params["model"] = model
	}
	if approval, sandbox := appServerModeSettings(s.mode); approval != "" {
		params["approvalPolicy"] = approval
		if sandbox != "" {
			params["sandbox"] = sandbox
		}
	}
	return params
}

func appServerModeSettings(mode string) (approval string, sandbox string) {
	switch normalizeMode(mode) {
	case "auto-edit", "full-auto":
		return "never", "workspace-write"
	case "yolo":
		return "never", "danger-full-access"
	default:
		return "on-request", "read-only"
	}
}

func (s *appServerSession) applyThreadRuntimeState(workDir, model string, effort *string) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if dir := strings.TrimSpace(workDir); dir != "" {
		s.workDir = dir
	}
	if m := strings.TrimSpace(model); m != "" {
		s.model = m
	}
	s.effort = normalizeRuntimeReasoningEffort(stringValue(effort))
}

// applyResumedThreadRuntimeState adopts runtime values reported by an existing
// thread only when the new session did not explicitly request an override.
// In particular, thread/resume may report the model used by the thread's last
// turn even when the caller supplied a new model. Overwriting the requested
// model here would make the following turn/start silently fall back to the old
// model and make /model appear successful without taking effect.
func (s *appServerSession) applyResumedThreadRuntimeState(workDir, model string, effort *string) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	if dir := strings.TrimSpace(workDir); dir != "" {
		s.workDir = dir
	}
	if strings.TrimSpace(s.model) == "" {
		if m := strings.TrimSpace(model); m != "" {
			s.model = m
		}
	}
	if strings.TrimSpace(s.effort) == "" {
		s.effort = normalizeRuntimeReasoningEffort(stringValue(effort))
	}
}

func (s *appServerSession) refreshUsage(ctx context.Context) error {
	timeout := appServerUsageRefreshTimeout
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			if until := time.Until(deadline); until > 0 && until < timeout {
				timeout = until
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if timeout <= 0 {
		return context.DeadlineExceeded
	}

	var resp appServerRateLimitsResponse
	if err := s.requestWithTimeout("account/rateLimits/read", map[string]any{}, &resp, timeout); err != nil {
		return err
	}
	s.storeUsage(mapAppServerRateLimits(resp))
	return nil
}

func (s *appServerSession) cachedUsage() *core.UsageReport {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return cloneUsageReport(s.usage)
}

func (s *appServerSession) cachedContextUsage() *core.ContextUsage {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return cloneContextUsage(s.context)
}

func (s *appServerSession) storeUsage(report *core.UsageReport) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.usage = cloneUsageReport(report)
}

func (s *appServerSession) storeContextUsage(usage *core.ContextUsage) {
	s.runtimeMu.Lock()
	defer s.runtimeMu.Unlock()
	s.context = cloneContextUsage(usage)
}

func (s *appServerSession) Send(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if !s.alive.Load() {
		return fmt.Errorf("session is closed")
	}

	if len(files) > 0 {
		filePaths := core.SaveFilesToDisk(s.workDir, messageID, files)
		prompt = core.AppendFileRefs(prompt, filePaths)
	}

	prompt, imagePaths, err := s.stageImages(prompt, images)
	if err != nil {
		return err
	}

	s.stateMu.Lock()
	if !s.preambleSent {
		prompt = prependCodexPromptPreamble(prompt, s.promptPreamble)
		s.preambleSent = true
	}
	s.stateMu.Unlock()

	threadID := s.CurrentSessionID()
	if threadID == "" {
		return fmt.Errorf("codex app-server thread id is empty")
	}

	input := make([]map[string]any, 0, 1+len(imagePaths))
	input = append(input, map[string]any{
		"type":          "text",
		"text":          prompt,
		"text_elements": []any{},
	})
	for _, path := range imagePaths {
		input = append(input, map[string]any{
			"type": "localImage",
			"path": path,
		})
	}

	params := map[string]any{
		"threadId": threadID,
		"input":    input,
	}
	if model := s.GetModel(); model != "" {
		params["model"] = model
	}
	if effort := s.GetReasoningEffort(); effort != "" {
		params["effort"] = effort
	}
	if approval, _ := appServerModeSettings(s.mode); approval != "" {
		params["approvalPolicy"] = approval
	}

	// Mark the next turn/started notification as platform-originated before
	// issuing the request. Shared App Server notifications can arrive before
	// the turn/start response reaches this goroutine.
	s.stateMu.Lock()
	s.localStartPending = true
	s.stateMu.Unlock()

	var resp turnStartResponse
	if err := s.request("turn/start", params, &resp); err != nil {
		s.stateMu.Lock()
		s.localStartPending = false
		s.stateMu.Unlock()
		return fmt.Errorf("codex app-server turn/start: %w", err)
	}
	if resp.Turn.ID == "" {
		s.stateMu.Lock()
		s.localStartPending = false
		s.stateMu.Unlock()
		return fmt.Errorf("codex app-server turn/start returned empty turn id")
	}

	s.stateMu.Lock()
	s.localStartPending = false
	s.currentTurn = resp.Turn.ID
	s.currentTurnLocal = true
	s.pendingMsgs = s.pendingMsgs[:0]
	s.pendingHasFinal = false
	s.pendingHasUnknownPhase = false
	clear(s.agentMessagePhases)
	clear(s.reasoningSummaryIndexes)
	s.stateMu.Unlock()

	return nil
}

// Steer appends user input to the Codex turn currently in flight. Unlike
// Send, turn/steer does not create a new turn or a second turn/started event,
// so the engine's existing foreground event loop remains valid.
func (s *appServerSession) Steer(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if !s.alive.Load() {
		return fmt.Errorf("session is closed")
	}

	if len(files) > 0 {
		filePaths := core.SaveFilesToDisk(s.workDir, messageID, files)
		prompt = core.AppendFileRefs(prompt, filePaths)
	}

	prompt, imagePaths, err := s.stageImages(prompt, images)
	if err != nil {
		return err
	}

	threadID := s.CurrentSessionID()
	s.stateMu.Lock()
	turnID := s.currentTurn
	s.stateMu.Unlock()
	if threadID == "" {
		return fmt.Errorf("codex app-server thread id is empty")
	}
	if turnID == "" {
		return fmt.Errorf("codex app-server has no active turn to steer")
	}

	input := make([]map[string]any, 0, 1+len(imagePaths))
	input = append(input, map[string]any{
		"type":          "text",
		"text":          prompt,
		"text_elements": []any{},
	})
	for _, path := range imagePaths {
		input = append(input, map[string]any{
			"type": "localImage",
			"path": path,
		})
	}

	params := map[string]any{
		"threadId":       threadID,
		"expectedTurnId": turnID,
		"input":          input,
	}
	if messageID != "" {
		params["clientUserMessageId"] = messageID
	}

	var resp turnSteerResponse
	if err := s.requestWithTimeout("turn/steer", params, &resp, appServerSteerTimeout); err != nil {
		return fmt.Errorf("codex app-server turn/steer: %w", err)
	}
	if resp.TurnID == "" {
		return fmt.Errorf("codex app-server turn/steer returned empty turn id")
	}
	if resp.TurnID != turnID {
		return fmt.Errorf("codex app-server turn/steer returned turn %q, expected %q", resp.TurnID, turnID)
	}
	return nil
}

// CancelTurn interrupts only the active Codex turn and keeps the app-server
// thread alive so the next Feishu message can continue the same conversation.
func (s *appServerSession) CancelTurn() error {
	if !s.alive.Load() {
		return fmt.Errorf("session is closed")
	}
	threadID := s.CurrentSessionID()
	s.stateMu.Lock()
	turnID := s.currentTurn
	s.stateMu.Unlock()
	if threadID == "" || turnID == "" {
		return fmt.Errorf("codex app-server has no active turn to interrupt")
	}
	if err := s.request("turn/interrupt", map[string]any{
		"threadId": threadID,
		"turnId":   turnID,
	}, nil); err != nil {
		return fmt.Errorf("codex app-server turn/interrupt: %w", err)
	}
	return nil
}

func (s *appServerSession) stageImages(prompt string, images []core.ImageAttachment) (string, []string, error) {
	if len(images) == 0 {
		return prompt, nil, nil
	}

	imgDir := filepath.Join(s.workDir, ".cc-connect", "images")
	if err := os.MkdirAll(imgDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("codex app-server: create image dir: %w", err)
	}

	imagePaths := make([]string, 0, len(images))
	for i, img := range images {
		ext := codexImageExt(img.MimeType)
		fname := fmt.Sprintf("img_%d_%d%s", time.Now().UnixMilli(), i, ext)
		fpath := filepath.Join(imgDir, fname)
		if err := os.WriteFile(fpath, img.Data, 0o644); err != nil {
			return "", nil, fmt.Errorf("codex app-server: save image: %w", err)
		}
		imagePaths = append(imagePaths, fpath)
	}

	if strings.TrimSpace(prompt) == "" {
		prompt = "Please analyze the attached image(s)."
	}

	return prompt, imagePaths, nil
}

func (s *appServerSession) RespondPermission(requestID string, result core.PermissionResult) error {
	s.approvalsMu.Lock()
	ch := s.pendingApprovals[requestID]
	s.approvalsMu.Unlock()
	if ch == nil {
		return fmt.Errorf("codex app-server: no pending approval for request %s", requestID)
	}
	select {
	case ch <- result:
	default:
	}
	return nil
}

func (s *appServerSession) handleServerRequest(probe map[string]json.RawMessage) {
	rawID := probe["id"]
	var method string
	if err := json.Unmarshal(probe["method"], &method); err != nil {
		return
	}
	params := probe["params"]

	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
		s.handleApprovalRequest(rawID, method, params)
	case "item/permissions/requestApproval":
		s.handlePermissionsApproval(rawID, params)
	case "item/tool/requestUserInput":
		s.handleRequestUserInput(rawID, params)
	case "item/tool/call":
		s.handleDynamicToolCall(rawID, params)
	default:
		_ = s.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"error": map[string]any{"code": -32601, "message": "method not found"},
		})
	}
}

func (s *appServerSession) handleApprovalRequest(rawID json.RawMessage, method string, paramsRaw json.RawMessage) {
	requestID := string(rawID)
	var params map[string]any
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return
	}

	toolName, toolInput := method, appServerJSON(params)
	switch method {
	case "item/commandExecution/requestApproval":
		toolName = "Bash"
		if cmd, _ := params["command"].(string); cmd != "" {
			toolInput = cmd
			if cwd, _ := params["cwd"].(string); cwd != "" {
				toolInput += "\n(in " + cwd + ")"
			}
		}
	case "item/fileChange/requestApproval":
		toolName = "Patch"
		if reason, _ := params["reason"].(string); reason != "" {
			toolInput = reason
		}
	}

	ch := make(chan core.PermissionResult, 1)
	s.approvalsMu.Lock()
	s.pendingApprovals[requestID] = ch
	s.approvalsMu.Unlock()

	s.flushPendingAsThinking()
	s.emit(core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     toolName,
		ToolInput:    toolInput,
		ToolInputRaw: params,
	})

	go func() {
		timer := time.NewTimer(5 * time.Minute)
		defer timer.Stop()
		var result core.PermissionResult
		select {
		case result = <-ch:
		case <-s.ctx.Done():
			result = core.PermissionResult{Behavior: "deny"}
		case <-timer.C:
			result = core.PermissionResult{Behavior: "deny"}
		}
		s.approvalsMu.Lock()
		delete(s.pendingApprovals, requestID)
		s.approvalsMu.Unlock()

		decision := "decline"
		if strings.EqualFold(result.Behavior, "allow") {
			decision = "accept"
		}
		_ = s.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"result": map[string]any{"decision": decision},
		})
	}()
}

func (s *appServerSession) handlePermissionsApproval(rawID json.RawMessage, paramsRaw json.RawMessage) {
	requestID := string(rawID)
	var params map[string]any
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return
	}

	ch := make(chan core.PermissionResult, 1)
	s.approvalsMu.Lock()
	s.pendingApprovals[requestID] = ch
	s.approvalsMu.Unlock()

	s.flushPendingAsThinking()
	s.emit(core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     "Permissions",
		ToolInput:    appServerJSON(params),
		ToolInputRaw: params,
	})

	go func() {
		timer := time.NewTimer(5 * time.Minute)
		defer timer.Stop()
		var result core.PermissionResult
		select {
		case result = <-ch:
		case <-s.ctx.Done():
			result = core.PermissionResult{Behavior: "deny"}
		case <-timer.C:
			result = core.PermissionResult{Behavior: "deny"}
		}
		s.approvalsMu.Lock()
		delete(s.pendingApprovals, requestID)
		s.approvalsMu.Unlock()

		if strings.EqualFold(result.Behavior, "allow") {
			perms := params["permissions"]
			if perms == nil {
				perms = map[string]any{}
			}
			_ = s.writeJSON(map[string]any{
				"jsonrpc": "2.0", "id": rawID,
				"result": map[string]any{"permissions": perms, "scope": "turn"},
			})
		} else {
			_ = s.writeJSON(map[string]any{
				"jsonrpc": "2.0", "id": rawID,
				"result": map[string]any{"permissions": map[string]any{}},
			})
		}
	}()
}

func (s *appServerSession) handleRequestUserInput(rawID json.RawMessage, paramsRaw json.RawMessage) {
	requestID := string(rawID)
	var params appServerRequestUserInputParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		_ = s.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"error": map[string]any{"code": -32602, "message": "invalid params"},
		})
		return
	}

	questions := appServerRequestUserInputQuestions(params.Questions)
	if len(questions) == 0 {
		_ = s.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"result": appServerRequestUserInputResponse{Answers: map[string]appServerRequestUserInputAnswer{}},
		})
		return
	}

	rawInput := appServerRequestUserInputRawInput(params)
	ch := make(chan core.PermissionResult, 1)
	s.approvalsMu.Lock()
	s.pendingApprovals[requestID] = ch
	s.approvalsMu.Unlock()

	s.flushPendingAsThinking()
	s.emit(core.Event{
		Type:         core.EventPermissionRequest,
		RequestID:    requestID,
		ToolName:     "AskUserQuestion",
		ToolInput:    appServerJSON(rawInput),
		ToolInputRaw: rawInput,
		Questions:    questions,
	})

	go func() {
		timer := time.NewTimer(5 * time.Minute)
		defer timer.Stop()
		var result core.PermissionResult
		select {
		case result = <-ch:
		case <-s.ctx.Done():
			result = core.PermissionResult{Behavior: "deny"}
		case <-timer.C:
			result = core.PermissionResult{Behavior: "deny"}
		}
		s.approvalsMu.Lock()
		delete(s.pendingApprovals, requestID)
		s.approvalsMu.Unlock()

		response := appServerRequestUserInputResponseFromResult(params.Questions, result)
		_ = s.writeJSON(map[string]any{
			"jsonrpc": "2.0", "id": rawID,
			"result": response,
		})
	}()
}

func (s *appServerSession) handleDynamicToolCall(rawID json.RawMessage, paramsRaw json.RawMessage) {
	_ = s.writeJSON(map[string]any{
		"jsonrpc": "2.0", "id": rawID,
		"result": map[string]any{
			"success":      false,
			"contentItems": []map[string]any{{"type": "inputText", "text": "tool not available on this client"}},
		},
	})
}

func appServerRequestUserInputQuestions(input []appServerRequestUserInputQuestion) []core.UserQuestion {
	questions := make([]core.UserQuestion, 0, len(input))
	for _, in := range input {
		questionText := strings.TrimSpace(in.Question)
		if questionText == "" {
			continue
		}
		q := core.UserQuestion{
			Question: questionText,
			Header:   strings.TrimSpace(in.Header),
		}
		for _, opt := range in.Options {
			q.Options = append(q.Options, core.UserQuestionOption{
				Label:       strings.TrimSpace(opt.Label),
				Description: strings.TrimSpace(opt.Description),
			})
		}
		questions = append(questions, q)
	}
	return questions
}

func appServerRequestUserInputRawInput(params appServerRequestUserInputParams) map[string]any {
	questions := make([]any, 0, len(params.Questions))
	for _, in := range params.Questions {
		q := map[string]any{
			"id":       in.ID,
			"header":   in.Header,
			"question": in.Question,
			"isOther":  in.IsOther,
			"isSecret": in.IsSecret,
			"options":  appServerRequestUserInputRawOptions(in.Options),
		}
		questions = append(questions, q)
	}
	return map[string]any{
		"threadId":  params.ThreadID,
		"turnId":    params.TurnID,
		"itemId":    params.ItemID,
		"questions": questions,
	}
}

func appServerRequestUserInputRawOptions(options []appServerRequestUserInputOption) []any {
	out := make([]any, 0, len(options))
	for _, opt := range options {
		out = append(out, map[string]any{
			"label":       opt.Label,
			"description": opt.Description,
		})
	}
	return out
}

func appServerRequestUserInputResponseFromResult(questions []appServerRequestUserInputQuestion, result core.PermissionResult) appServerRequestUserInputResponse {
	response := appServerRequestUserInputResponse{Answers: map[string]appServerRequestUserInputAnswer{}}
	if !strings.EqualFold(result.Behavior, "allow") {
		return response
	}

	answersRaw, _ := result.UpdatedInput["answers"].(map[string]any)
	if len(answersRaw) == 0 {
		return response
	}

	for _, q := range questions {
		id := strings.TrimSpace(q.ID)
		text := strings.TrimSpace(q.Question)
		if id == "" || text == "" {
			continue
		}
		values := appServerRequestUserInputAnswerValues(answersRaw[text])
		if len(values) == 0 {
			continue
		}
		response.Answers[id] = appServerRequestUserInputAnswer{Answers: values}
	}
	return response
}

func appServerRequestUserInputAnswerValues(raw any) []string {
	switch v := raw.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []string{v}
	case []string:
		values := make([]string, 0, len(v))
		for _, s := range v {
			if strings.TrimSpace(s) != "" {
				values = append(values, s)
			}
		}
		return values
	case []any:
		values := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				values = append(values, s)
			}
		}
		return values
	case map[string]any:
		return appServerRequestUserInputAnswerValues(v["answers"])
	case appServerRequestUserInputAnswer:
		return appServerRequestUserInputAnswerValues(v.Answers)
	default:
		return nil
	}
}

func (s *appServerSession) rejectPendingApprovals(err error) {
	s.approvalsMu.Lock()
	defer s.approvalsMu.Unlock()
	for id, ch := range s.pendingApprovals {
		delete(s.pendingApprovals, id)
		select {
		case ch <- core.PermissionResult{Behavior: "deny"}:
		default:
		}
	}
}

func (s *appServerSession) Events() <-chan core.Event {
	return s.events
}

// RecoverMissedTurns reconciles completed App Server turns after this client
// reconnects. thread/resume restores the live subscription, while
// thread/turns/list covers the interval in which cc-connect was down.
func (s *appServerSession) RecoverMissedTurns(ctx context.Context, afterTurnID string, localHistory []core.HistoryEntry) ([]core.RecoveredAgentTurn, string, error) {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, strings.TrimSpace(afterTurnID), ctx.Err()
		default:
		}
	}

	const (
		pageSize = 20
		maxPages = 10
	)
	threadID := s.CurrentSessionID()
	if threadID == "" {
		return nil, strings.TrimSpace(afterTurnID), fmt.Errorf("codex app-server recovery: empty thread id")
	}

	var snapshots []appServerTurnSnapshot // newest first
	cursor := ""
	for page := 0; page < maxPages; page++ {
		params := map[string]any{
			"threadId":      threadID,
			"limit":         pageSize,
			"sortDirection": "desc",
			"itemsView":     "summary",
		}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var resp threadTurnsListResponse
		if err := s.request("thread/turns/list", params, &resp); err != nil {
			return nil, strings.TrimSpace(afterTurnID), fmt.Errorf("codex app-server recovery list turns: %w", err)
		}
		snapshots = append(snapshots, resp.Data...)
		if recoveryAnchorIndex(snapshots, afterTurnID, localHistory) >= 0 || strings.TrimSpace(resp.NextCursor) == "" {
			break
		}
		cursor = resp.NextCursor
	}

	// Seed an active VM turn so notifications that arrive after resume have a
	// valid turn boundary even though turn/started happened before reconnect.
	for _, turn := range snapshots {
		if strings.EqualFold(strings.TrimSpace(turn.Status), "inProgress") {
			s.seedRecoveredActiveTurn(turn)
			break
		}
	}

	anchor := recoveryAnchorIndex(snapshots, afterTurnID, localHistory)
	latestCompleted := newestCompletedTurnID(snapshots)
	if anchor < 0 {
		// Establish a conservative baseline instead of replaying arbitrary old
		// turns when no trustworthy cursor can be found.
		return nil, latestCompleted, nil
	}

	newer := snapshots[:anchor]
	recovered := make([]core.RecoveredAgentTurn, 0, len(newer))
	for i := len(newer) - 1; i >= 0; i-- {
		turn := newer[i]
		if !strings.EqualFold(strings.TrimSpace(turn.Status), "completed") {
			continue
		}
		assistant := recoveredTurnAssistantText(turn)
		if strings.TrimSpace(assistant) == "" {
			continue
		}
		recovered = append(recovered, core.RecoveredAgentTurn{
			ID:               strings.TrimSpace(turn.ID),
			UserMessage:      recoveredTurnUserText(turn),
			AssistantMessage: assistant,
		})
	}
	if len(recovered) > 0 {
		latestCompleted = recovered[len(recovered)-1].ID
	} else if strings.TrimSpace(afterTurnID) != "" {
		latestCompleted = strings.TrimSpace(afterTurnID)
	}
	return recovered, latestCompleted, nil
}

func recoveryAnchorIndex(turns []appServerTurnSnapshot, afterTurnID string, localHistory []core.HistoryEntry) int {
	if afterTurnID = strings.TrimSpace(afterTurnID); afterTurnID != "" {
		for i := range turns {
			if strings.TrimSpace(turns[i].ID) == afterTurnID {
				return i
			}
		}
	}
	for h := len(localHistory) - 1; h >= 0; h-- {
		if localHistory[h].Role != "assistant" || strings.TrimSpace(localHistory[h].Content) == "" {
			continue
		}
		want := strings.TrimSpace(localHistory[h].Content)
		for i := range turns {
			if strings.TrimSpace(recoveredTurnAssistantText(turns[i])) == want {
				return i
			}
		}
	}
	return -1
}

func newestCompletedTurnID(turns []appServerTurnSnapshot) string {
	for _, turn := range turns {
		if strings.EqualFold(strings.TrimSpace(turn.Status), "completed") {
			return strings.TrimSpace(turn.ID)
		}
	}
	return ""
}

func recoveredTurnUserText(turn appServerTurnSnapshot) string {
	for _, item := range turn.Items {
		if itemType, _ := item["type"].(string); strings.EqualFold(itemType, "userMessage") {
			if text := appServerUserMessageText(item); strings.TrimSpace(text) != "" {
				return text
			}
		}
	}
	return ""
}

func recoveredTurnAssistantText(turn appServerTurnSnapshot) string {
	var all []string
	var finals []string
	for _, item := range turn.Items {
		itemType, _ := item["type"].(string)
		if !strings.EqualFold(itemType, "agentMessage") {
			continue
		}
		text, _ := item["text"].(string)
		if strings.TrimSpace(text) == "" {
			continue
		}
		all = append(all, text)
		phase, _ := item["phase"].(string)
		if strings.EqualFold(strings.TrimSpace(phase), "final_answer") {
			finals = append(finals, text)
		}
	}
	if len(finals) > 0 {
		return strings.Join(finals, "")
	}
	return strings.Join(all, "")
}

func (s *appServerSession) seedRecoveredActiveTurn(turn appServerTurnSnapshot) {
	turnID := strings.TrimSpace(turn.ID)
	if turnID == "" {
		return
	}
	s.stateMu.Lock()
	if s.currentTurn != "" && s.currentTurn != turnID {
		s.stateMu.Unlock()
		return
	}
	if s.currentTurn == "" {
		s.currentTurn = turnID
		s.currentTurnLocal = false
		s.pendingMsgs = s.pendingMsgs[:0]
		s.pendingHasFinal = false
		s.pendingHasUnknownPhase = false
	}
	latestCommentary := ""
	for _, item := range turn.Items {
		itemType, _ := item["type"].(string)
		if !strings.EqualFold(itemType, "agentMessage") {
			continue
		}
		itemID, _ := item["id"].(string)
		phase, _ := item["phase"].(string)
		itemID = strings.TrimSpace(itemID)
		phase = strings.ToLower(strings.TrimSpace(phase))
		if itemID != "" && phase != "" {
			if s.agentMessagePhases == nil {
				s.agentMessagePhases = make(map[string]string)
			}
			s.agentMessagePhases[itemID] = phase
		}
		text, _ := item["text"].(string)
		if strings.TrimSpace(text) == "" {
			continue
		}
		switch phase {
		case "commentary":
			// Existing commentary is progress, not part of the authoritative
			// final answer. Publish only the newest checkpoint on reconnect.
			latestCommentary = text
		case "final_answer":
			s.pendingMsgs = append(s.pendingMsgs, text)
			s.pendingHasFinal = true
		case "":
			s.pendingMsgs = append(s.pendingMsgs, text)
			s.pendingHasUnknownPhase = true
		}
	}
	s.stateMu.Unlock()
	if strings.TrimSpace(latestCommentary) != "" {
		s.emit(core.Event{Type: core.EventThinking, Content: latestCommentary})
	}
}

func (s *appServerSession) CurrentSessionID() string {
	v, _ := s.threadID.Load().(string)
	return v
}

func (s *appServerSession) GetWorkDir() string {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.workDir
}

func (s *appServerSession) GetModel() string {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return strings.TrimSpace(s.model)
}

func (s *appServerSession) GetReasoningEffort() string {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return strings.TrimSpace(s.effort)
}

func (s *appServerSession) GetUsage(ctx context.Context) (*core.UsageReport, error) {
	if err := s.refreshUsage(ctx); err != nil {
		if cached := s.cachedUsage(); cached != nil {
			return cached, nil
		}
		return nil, err
	}
	if cached := s.cachedUsage(); cached != nil {
		return cached, nil
	}
	return nil, fmt.Errorf("codex app-server usage unavailable")
}

func (s *appServerSession) GetContextUsage() *core.ContextUsage {
	return s.cachedContextUsage()
}

func (s *appServerSession) Alive() bool {
	return s.alive.Load()
}

func (s *appServerSession) Close() error {
	s.alive.Store(false)
	s.cancel()

	s.procMu.Lock()
	if s.stdin != nil {
		_ = s.stdin.Close()
		s.stdin = nil
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	s.procMu.Unlock()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	s.closeOnce.Do(func() {
		close(s.events)
	})
	return nil
}

func (s *appServerSession) readLoop(r io.Reader) {
	defer s.wg.Done()
	// App Server responses are newline-delimited JSON. A resumed thread may
	// include its complete history in one response, so bufio.Scanner's bounded
	// token buffer is not appropriate here: legitimate long-lived threads can
	// easily produce a line larger than 10 MiB. json.Decoder streams one JSON
	// value at a time without imposing an arbitrary per-message ceiling.
	decoder := json.NewDecoder(bufio.NewReaderSize(r, 64*1024))

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		var data json.RawMessage
		if err := decoder.Decode(&data); err != nil {
			if errors.Is(err, io.EOF) {
				s.alive.Store(false)
				if s.ctx.Err() == nil {
					s.emitError(fmt.Errorf("codex app-server connection closed: %w", err))
				}
				s.rejectPending(io.EOF)
				s.rejectPendingApprovals(io.EOF)
				return
			}
			if s.ctx.Err() == nil {
				slog.Warn("codex app-server read failed", "error", err)
				s.emitError(fmt.Errorf("codex app-server connection closed: %w", err))
			}
			s.alive.Store(false)
			s.rejectPending(err)
			s.rejectPendingApprovals(err)
			return
		}

		var probe map[string]json.RawMessage
		if err := json.Unmarshal(data, &probe); err != nil {
			slog.Debug("codex app-server: invalid JSON", "error", err)
			continue
		}

		_, hasID := probe["id"]
		_, hasMethod := probe["method"]

		switch {
		case hasID && !hasMethod:
			// Response to one of our requests.
			var resp rpcResponseEnvelope
			if err := json.Unmarshal(data, &resp); err != nil {
				slog.Debug("codex app-server: bad response envelope", "error", err)
				continue
			}
			s.handleResponse(resp)

		case hasID && hasMethod:
			// Server-initiated request that requires a response (e.g. approval).
			s.handleServerRequest(probe)

		default:
			// Notification (no id).
			var notif rpcNotificationEnvelope
			if err := json.Unmarshal(data, &notif); err != nil {
				slog.Debug("codex app-server: bad notification envelope", "error", err)
				continue
			}
			s.handleNotification(notif.Method, notif.Params)
		}
	}
}

func (s *appServerSession) stderrLoop(r io.Reader) {
	defer s.wg.Done()
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		slog.Debug("codex app-server stderr", "line", line)
	}
	if err := scanner.Err(); err != nil && s.ctx.Err() == nil {
		slog.Debug("codex app-server stderr read failed", "error", err)
	}
}

func (s *appServerSession) waitLoop() {
	defer s.wg.Done()

	s.procMu.Lock()
	cmd := s.cmd
	s.procMu.Unlock()
	if cmd == nil {
		return
	}

	err := cmd.Wait()
	if s.ctx.Err() == nil && err != nil {
		slog.Warn("codex app-server exited unexpectedly", "error", err)
		s.emitError(fmt.Errorf("codex app-server exited: %w", err))
	}
	s.alive.Store(false)
	if err == nil {
		err = io.EOF
	}
	s.rejectPending(err)
}

func (s *appServerSession) handleResponse(resp rpcResponseEnvelope) {
	id, ok := rpcIDToInt64(resp.ID)
	if !ok {
		return
	}

	s.pendingMu.Lock()
	ch := s.pending[id]
	delete(s.pending, id)
	s.pendingMu.Unlock()

	if ch == nil {
		return
	}

	select {
	case ch <- resp:
	default:
	}
}

func (s *appServerSession) handleNotification(method string, paramsRaw json.RawMessage) {
	switch method {
	case "turn/started":
		var notif turnNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsThreadNotification(method, notif.ThreadID) {
			s.stateMu.Lock()
			if s.currentTurn != strings.TrimSpace(notif.Turn.ID) {
				s.currentTurn = strings.TrimSpace(notif.Turn.ID)
				s.currentTurnLocal = s.localStartPending
				s.pendingMsgs = s.pendingMsgs[:0]
				s.pendingHasFinal = false
				s.pendingHasUnknownPhase = false
				clear(s.agentMessagePhases)
				clear(s.reasoningSummaryIndexes)
			}
			s.stateMu.Unlock()
			s.storeContextUsage(nil)
		}

	case "item/started":
		var notif itemNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.TurnID) {
			s.ensureCurrentTurn(notif.TurnID)
			s.handleItemStarted(notif.Item)
		}

	case "item/completed":
		var notif itemNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.TurnID) {
			s.ensureCurrentTurn(notif.TurnID)
			s.handleItemCompleted(notif.Item)
		}

	case "item/agentMessage/delta":
		var notif agentMessageDeltaNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.TurnID) {
			s.ensureCurrentTurn(notif.TurnID)
			s.appendAgentMessageDelta(notif.ItemID, notif.Delta)
		}

	case "item/reasoning/summaryTextDelta":
		var notif reasoningSummaryTextDeltaNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.TurnID) {
			s.ensureCurrentTurn(notif.TurnID)
			s.appendReasoningSummaryDelta(notif.ItemID, notif.SummaryIndex, notif.Delta)
		}

	case "turn/completed":
		var notif turnNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.Turn.ID) {
			status := strings.ToLower(strings.TrimSpace(notif.Turn.Status))
			if status == "failed" || notif.Turn.Error != nil {
				errMsg := ""
				if notif.Turn.Error != nil {
					errMsg = strings.TrimSpace(notif.Turn.Error.Message)
				}
				if errMsg == "" {
					errMsg = "turn failed (no details)"
				}
				s.failTurn(fmt.Errorf("%s", errMsg))
			} else if status == "interrupted" {
				s.failTurn(fmt.Errorf("turn interrupted"))
			} else {
				// A successful turn is terminal only after Codex has emitted a
				// non-empty final_answer. In particular, goal auto-continuation
				// can make the thread transiently idle while the same turn keeps
				// producing commentary and tool calls.
				if !s.completeTurnWithFinal(notif.Turn.ID, true) {
					s.failTurn(fmt.Errorf("turn completed without a final answer"))
				}
			}
		}

	case "thread/status/changed":
		var notif struct {
			ThreadID string `json:"threadId"`
			Status   struct {
				Type string `json:"type"`
			} `json:"status"`
		}
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && notif.Status.Type == "idle" && s.acceptsThreadNotification(method, notif.ThreadID) {
			// thread/status is a coarse scheduling signal, not a reliable turn
			// boundary: goal auto-continuation may briefly report idle and then
			// resume the same turn ID. Only use it as a completion fallback when
			// a real final_answer is already buffered.
			s.completeTurnWithFinal("", false)
		}

	case "account/rateLimits/updated":
		var notif appServerRateLimitsResponse
		if err := json.Unmarshal(paramsRaw, &notif); err == nil {
			s.storeUsage(mapAppServerRateLimits(notif))
		}

	case "thread/tokenUsage/updated":
		var notif appServerThreadTokenUsageNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.TurnID) {
			s.storeContextUsage(mapAppServerTokenUsage(notif))
		}

	case "error":
		var notif errorNotification
		if err := json.Unmarshal(paramsRaw, &notif); err == nil && s.acceptsTurnNotification(method, notif.ThreadID, notif.TurnID) {
			message := strings.TrimSpace(notif.Error.Message)
			if message == "" {
				message = strings.TrimSpace(notif.Message)
			}
			// Retriable errors are progress notifications; App Server keeps the
			// turn alive and will later send either success or terminal failure.
			if message != "" && !notif.WillRetry {
				s.failTurn(fmt.Errorf("%s", message))
			}
		}
	}
}

func (s *appServerSession) acceptsThreadNotification(method, threadID string) bool {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return true
	}
	currentThreadID := s.CurrentSessionID()
	if currentThreadID != "" && threadID == currentThreadID {
		return true
	}
	slog.Debug("codex app-server: ignoring notification for another thread",
		"method", method, "thread_id", threadID, "current_thread_id", currentThreadID)
	return false
}

func (s *appServerSession) acceptsTurnNotification(method, threadID, turnID string) bool {
	if !s.acceptsThreadNotification(method, threadID) {
		return false
	}
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return true
	}
	s.stateMu.Lock()
	currentTurn := s.currentTurn
	s.stateMu.Unlock()
	if currentTurn == "" || turnID == currentTurn {
		return true
	}
	slog.Debug("codex app-server: ignoring notification for another turn",
		"method", method, "turn_id", turnID, "current_turn_id", currentTurn)
	return false
}

func (s *appServerSession) ensureCurrentTurn(turnID string) {
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return
	}
	s.stateMu.Lock()
	if s.currentTurn == "" {
		s.currentTurn = turnID
		s.currentTurnLocal = s.localStartPending
		s.pendingMsgs = s.pendingMsgs[:0]
		s.pendingHasFinal = false
		s.pendingHasUnknownPhase = false
		clear(s.agentMessagePhases)
		clear(s.reasoningSummaryIndexes)
	}
	s.stateMu.Unlock()
}

func (s *appServerSession) handleItemStarted(item map[string]any) {
	itemType, _ := item["type"].(string)
	if itemType == "" {
		return
	}

	switch itemType {
	case "agentMessage":
		s.rememberAgentMessagePhase(item)
		return
	case "reasoning", "userMessage", "plan", "hookPrompt", "contextCompaction":
		return
	}

	s.stateMu.Lock()
	localTurn := s.currentTurnLocal
	s.stateMu.Unlock()
	if localTurn {
		s.flushPendingAsThinking()
	} else {
		s.discardPendingMessages()
	}

	switch itemType {
	case "commandExecution":
		command, _ := item["command"].(string)
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "Bash", ToolInput: command})

	case "mcpToolCall":
		server, _ := item["server"].(string)
		tool, _ := item["tool"].(string)
		name := strings.Trim(strings.Join([]string{server, tool}, ":"), ":")
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "MCP", ToolInput: name + "\n" + appServerJSON(item["arguments"])})

	case "webSearch":
		query, _ := item["query"].(string)
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "WebSearch", ToolInput: query})

	case "dynamicToolCall":
		tool, _ := item["tool"].(string)
		s.emit(core.Event{Type: core.EventToolUse, ToolName: tool, ToolInput: appServerJSON(item["arguments"])})

	case "fileChange":
		s.emit(core.Event{Type: core.EventToolUse, ToolName: "Patch", ToolInput: appServerJSON(item["changes"])})
	}
}

func (s *appServerSession) handleItemCompleted(item map[string]any) {
	itemType, _ := item["type"].(string)
	if itemType == "" {
		return
	}

	switch itemType {
	case "reasoning":
		text := appServerReasoningText(item)
		if text != "" {
			itemID, _ := item["id"].(string)
			s.emit(core.Event{
				Type:    core.EventThinking,
				Content: text,
				Done:    true,
				Metadata: map[string]any{
					"item_id": itemID,
					"source":  "reasoning_summary",
				},
			})
		}

	case "agentMessage":
		text, _ := item["text"].(string)
		if strings.TrimSpace(text) != "" {
			itemID, _ := item["id"].(string)
			s.rememberAgentMessagePhase(item)
			phase, _ := item["phase"].(string)
			phase = strings.ToLower(strings.TrimSpace(phase))
			s.stateMu.Lock()
			external := !s.currentTurnLocal
			if !(external && phase == "commentary") {
				s.pendingMsgs = append(s.pendingMsgs, text)
			}
			switch phase {
			case "final_answer":
				s.pendingHasFinal = true
			case "":
				// MessagePhase is optional in the app-server protocol. Preserve
				// compatibility for providers that omit it, but only at the
				// authoritative turn/completed boundary (never at transient idle).
				s.pendingHasUnknownPhase = true
			}
			s.stateMu.Unlock()
			if external && phase == "commentary" {
				s.emit(core.Event{
					Type:    core.EventThinking,
					Content: text,
					Done:    true,
					Metadata: map[string]any{
						"item_id": itemID,
						"source":  "commentary",
					},
				})
			}
		}

	case "userMessage":
		text := appServerUserMessageText(item)
		if strings.TrimSpace(text) != "" {
			s.stateMu.Lock()
			external := !s.currentTurnLocal
			s.stateMu.Unlock()
			if external {
				s.emit(core.Event{Type: core.EventUserMessage, Content: text})
			}
		}

	case "commandExecution":
		command, _ := item["command"].(string)
		status, _ := item["status"].(string)
		output, _ := item["aggregatedOutput"].(string)
		exitCode, hasExitCode := toInt(item["exitCode"])
		var exitCodePtr *int
		if hasExitCode {
			exitCodePtr = &exitCode
		}
		success := appServerToolSuccess(status, exitCodePtr)
		s.emit(core.Event{
			Type:         core.EventToolResult,
			ToolName:     "Bash",
			ToolInput:    command,
			ToolResult:   truncate(strings.TrimSpace(output), 500),
			ToolStatus:   strings.TrimSpace(status),
			ToolExitCode: exitCodePtr,
			ToolSuccess:  &success,
		})

	case "mcpToolCall":
		tool, _ := item["tool"].(string)
		status, _ := item["status"].(string)
		result := appServerJSON(item["result"])
		if errText := appServerJSON(item["error"]); strings.TrimSpace(errText) != "" && result == "" {
			result = errText
		}
		success := appServerToolSuccess(status, nil)
		s.emit(core.Event{
			Type:        core.EventToolResult,
			ToolName:    tool,
			ToolResult:  truncate(strings.TrimSpace(result), 500),
			ToolStatus:  strings.TrimSpace(status),
			ToolSuccess: &success,
		})

	case "webSearch":
		query, _ := item["query"].(string)
		s.emit(core.Event{
			Type:       core.EventToolResult,
			ToolName:   "WebSearch",
			ToolResult: truncate(strings.TrimSpace(query), 500),
		})

	case "dynamicToolCall":
		tool, _ := item["tool"].(string)
		status, _ := item["status"].(string)
		result := appServerDynamicToolText(item["contentItems"])
		success := appServerToolSuccess(status, nil)
		s.emit(core.Event{
			Type:        core.EventToolResult,
			ToolName:    tool,
			ToolResult:  truncate(strings.TrimSpace(result), 500),
			ToolStatus:  strings.TrimSpace(status),
			ToolSuccess: &success,
		})
	}
}

func (s *appServerSession) appendAgentMessageDelta(itemID, delta string) {
	if delta == "" {
		return
	}
	itemID = strings.TrimSpace(itemID)
	s.stateMu.Lock()
	external := !s.currentTurnLocal
	phase := s.agentMessagePhases[itemID]
	s.stateMu.Unlock()
	if !external {
		return
	}
	eventType := core.EventText
	if phase == "commentary" {
		eventType = core.EventThinking
	}
	s.emit(core.Event{
		Type:    eventType,
		Content: delta,
		Metadata: map[string]any{
			"item_id": itemID,
			"source":  phase,
		},
	})
}

func (s *appServerSession) appendReasoningSummaryDelta(itemID string, summaryIndex int64, delta string) {
	if delta == "" {
		return
	}
	itemID = strings.TrimSpace(itemID)
	s.stateMu.Lock()
	external := !s.currentTurnLocal
	if !external {
		s.stateMu.Unlock()
		return
	}
	if s.reasoningSummaryIndexes == nil {
		s.reasoningSummaryIndexes = make(map[string]int64)
	}
	previousIndex, seen := s.reasoningSummaryIndexes[itemID]
	s.reasoningSummaryIndexes[itemID] = summaryIndex
	s.stateMu.Unlock()
	if seen && previousIndex != summaryIndex {
		delta = "\n\n" + delta
	}
	s.emit(core.Event{
		Type:    core.EventThinking,
		Content: delta,
		Metadata: map[string]any{
			"item_id":       itemID,
			"source":        "reasoning_summary",
			"summary_index": summaryIndex,
		},
	})
}

func (s *appServerSession) rememberAgentMessagePhase(item map[string]any) {
	itemID, _ := item["id"].(string)
	phase, _ := item["phase"].(string)
	itemID = strings.TrimSpace(itemID)
	phase = strings.ToLower(strings.TrimSpace(phase))
	if itemID == "" || phase == "" {
		return
	}
	s.stateMu.Lock()
	if s.agentMessagePhases == nil {
		s.agentMessagePhases = make(map[string]string)
	}
	s.agentMessagePhases[itemID] = phase
	s.stateMu.Unlock()
}

func appServerReasoningText(item map[string]any) string {
	var parts []string
	if summary, ok := item["summary"].([]any); ok {
		for _, entry := range summary {
			if text, ok := entry.(string); ok && strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
		}
	}
	if len(parts) == 0 {
		if content, ok := item["content"].([]any); ok {
			for _, entry := range content {
				if text, ok := entry.(string); ok && strings.TrimSpace(text) != "" {
					parts = append(parts, text)
				}
			}
		}
	}
	return strings.Join(parts, "\n")
}

func appServerUserMessageText(item map[string]any) string {
	content, ok := item["content"].([]any)
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(content))
	for _, entry := range content {
		part, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		partType, _ := part["type"].(string)
		if !strings.EqualFold(partType, "text") && !strings.EqualFold(partType, "inputText") {
			continue
		}
		if text, _ := part["text"].(string); strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func appServerDynamicToolText(raw any) string {
	items, ok := raw.([]any)
	if !ok {
		return appServerJSON(raw)
	}
	var parts []string
	for _, entry := range items {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if text, _ := m["text"].(string); strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	if len(parts) == 0 {
		return appServerJSON(raw)
	}
	return strings.Join(parts, "\n")
}

func appServerToolSuccess(status string, exitCode *int) bool {
	s := strings.ToLower(strings.TrimSpace(status))
	if exitCode != nil {
		return *exitCode == 0
	}
	return s == "completed" || s == "success" || s == "succeeded" || s == "ok"
}

func mapAppServerRateLimits(payload appServerRateLimitsResponse) *core.UsageReport {
	report := &core.UsageReport{Provider: "codex"}

	var snapshots []appServerRateLimitSnapshot
	if len(payload.RateLimitsByLimitID) > 0 {
		keys := make([]string, 0, len(payload.RateLimitsByLimitID))
		for key := range payload.RateLimitsByLimitID {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			snapshots = append(snapshots, payload.RateLimitsByLimitID[key])
		}
	} else if payload.RateLimits.LimitID != "" || payload.RateLimits.Primary != nil || payload.RateLimits.Secondary != nil || payload.RateLimits.Credits != nil {
		snapshots = append(snapshots, payload.RateLimits)
	}

	for _, snapshot := range snapshots {
		if report.Plan == "" && strings.TrimSpace(snapshot.PlanType) != "" {
			report.Plan = strings.TrimSpace(snapshot.PlanType)
		}
		if report.Credits == nil && snapshot.Credits != nil {
			report.Credits = &core.UsageCredits{
				HasCredits: snapshot.Credits.HasCredits,
				Unlimited:  snapshot.Credits.Unlimited,
			}
			if snapshot.Credits.Balance != nil {
				report.Credits.Balance = strings.TrimSpace(*snapshot.Credits.Balance)
			}
		}

		windows := appServerUsageWindows(snapshot)
		if len(windows) == 0 {
			continue
		}
		limitReached := false
		for _, window := range windows {
			if window.UsedPercent >= 100 {
				limitReached = true
				break
			}
		}

		report.Buckets = append(report.Buckets, core.UsageBucket{
			Name:         appServerBucketName(snapshot),
			Allowed:      !limitReached,
			LimitReached: limitReached,
			Windows:      windows,
		})
	}

	return report
}

func appServerBucketName(snapshot appServerRateLimitSnapshot) string {
	if name := strings.TrimSpace(snapshot.LimitName); name != "" {
		return name
	}
	if id := strings.TrimSpace(snapshot.LimitID); id != "" {
		return id
	}
	return "Rate limit"
}

func appServerUsageWindows(snapshot appServerRateLimitSnapshot) []core.UsageWindow {
	var windows []core.UsageWindow
	if snapshot.Primary != nil {
		windows = append(windows, appServerUsageWindow("Primary", snapshot.Primary))
	}
	if snapshot.Secondary != nil {
		windows = append(windows, appServerUsageWindow("Secondary", snapshot.Secondary))
	}
	return windows
}

func appServerUsageWindow(name string, window *appServerRateLimitWindow) core.UsageWindow {
	resetAfter := 0
	if window != nil && window.ResetsAt > 0 {
		resetAfter = int(time.Until(time.Unix(window.ResetsAt, 0)).Seconds())
		if resetAfter < 0 {
			resetAfter = 0
		}
	}
	return core.UsageWindow{
		Name:              name,
		UsedPercent:       window.UsedPercent,
		WindowSeconds:     window.WindowDurationMins * 60,
		ResetAfterSeconds: resetAfter,
		ResetAtUnix:       window.ResetsAt,
	}
}

func cloneUsageReport(report *core.UsageReport) *core.UsageReport {
	if report == nil {
		return nil
	}
	cloned := *report
	if len(report.Buckets) > 0 {
		cloned.Buckets = make([]core.UsageBucket, len(report.Buckets))
		for i, bucket := range report.Buckets {
			cloned.Buckets[i] = bucket
			if len(bucket.Windows) > 0 {
				cloned.Buckets[i].Windows = append([]core.UsageWindow(nil), bucket.Windows...)
			}
		}
	}
	if report.Credits != nil {
		credits := *report.Credits
		cloned.Credits = &credits
	}
	return &cloned
}

func normalizeRuntimeReasoningEffort(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ""
	case "med":
		return "medium"
	case "x-high", "very-high":
		return "xhigh"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func appServerJSON(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	s := strings.TrimSpace(string(b))
	if s == "null" || s == "{}" || s == "[]" || s == `""` {
		return ""
	}
	return s
}

func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return int(i), true
		}
	}
	return 0, false
}

func rpcIDToInt64(v any) (int64, bool) {
	switch id := v.(type) {
	case float64:
		return int64(id), true
	case int64:
		return id, true
	case int:
		return int64(id), true
	case json.Number:
		i, err := id.Int64()
		return i, err == nil
	}
	return 0, false
}

func (s *appServerSession) completeTurnWithFinal(turnID string, allowUnknownPhase bool) bool {
	s.stateMu.Lock()
	if s.currentTurn == "" || (!s.pendingHasFinal && !(allowUnknownPhase && s.pendingHasUnknownPhase)) {
		s.stateMu.Unlock()
		return false
	}
	if turnID = strings.TrimSpace(turnID); turnID != "" && turnID != s.currentTurn {
		s.stateMu.Unlock()
		return false
	}
	// app-server reports completed agent messages as whole items rather than
	// token deltas. Deliver the buffered final answer on EventResult itself so
	// card-style progress can atomically replace its empty body and terminal
	// status in one update instead of opening a second preview card first.
	completedTurnID := s.currentTurn
	finalText := strings.Join(s.pendingMsgs, "")
	s.currentTurn = ""
	s.currentTurnLocal = false
	s.pendingMsgs = s.pendingMsgs[:0]
	s.pendingHasFinal = false
	s.pendingHasUnknownPhase = false
	clear(s.agentMessagePhases)
	clear(s.reasoningSummaryIndexes)
	s.stateMu.Unlock()
	s.emit(core.Event{
		Type:      core.EventResult,
		Content:   finalText,
		SessionID: s.CurrentSessionID(),
		Done:      true,
		Metadata:  map[string]any{"agent_turn_id": completedTurnID},
	})
	return true
}

func (s *appServerSession) failTurn(err error) {
	s.stateMu.Lock()
	if s.currentTurn == "" {
		s.stateMu.Unlock()
		return
	}
	s.currentTurn = ""
	s.currentTurnLocal = false
	s.pendingMsgs = s.pendingMsgs[:0]
	s.pendingHasFinal = false
	s.pendingHasUnknownPhase = false
	clear(s.agentMessagePhases)
	clear(s.reasoningSummaryIndexes)
	s.stateMu.Unlock()
	s.emitError(err)
}

func (s *appServerSession) flushPendingAsThinking() {
	s.stateMu.Lock()
	msgs := append([]string(nil), s.pendingMsgs...)
	s.pendingMsgs = s.pendingMsgs[:0]
	// Any message followed by another tool is necessarily interim, even if a
	// provider mislabeled its phase. Do not let it authorize terminal completion.
	s.pendingHasFinal = false
	s.pendingHasUnknownPhase = false
	s.stateMu.Unlock()

	for _, text := range msgs {
		if strings.TrimSpace(text) != "" {
			s.emit(core.Event{Type: core.EventThinking, Content: text})
		}
	}
}

func (s *appServerSession) discardPendingMessages() {
	s.stateMu.Lock()
	s.pendingMsgs = s.pendingMsgs[:0]
	s.pendingHasFinal = false
	s.pendingHasUnknownPhase = false
	clear(s.agentMessagePhases)
	clear(s.reasoningSummaryIndexes)
	s.stateMu.Unlock()
}

func (s *appServerSession) emit(event core.Event) {
	// Terminal and interactive-control events must never be silently lost when
	// a burst of commentary/tool notifications fills the best-effort queue.
	// Blocking here applies backpressure only to the App Server reader; it does
	// not block the model process, and cancellation still tears it down cleanly.
	switch event.Type {
	case core.EventResult, core.EventError, core.EventPermissionRequest:
		if s.ctx == nil {
			s.events <- event
			return
		}
		select {
		case s.events <- event:
		case <-s.ctx.Done():
		}
		return
	}
	select {
	case s.events <- event:
	default:
		slog.Warn("codex appserver: event channel full, dropping event", "type", event.Type)
	}
}

func (s *appServerSession) emitError(err error) {
	if err == nil {
		return
	}
	s.emit(core.Event{Type: core.EventError, Error: err})
}

func (s *appServerSession) rejectPending(err error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for id, ch := range s.pending {
		delete(s.pending, id)
		select {
		case ch <- rpcResponseEnvelope{ID: id, Error: &rpcError{Message: err.Error()}}:
		default:
		}
	}
}

func (s *appServerSession) request(method string, params any, out any) error {
	return s.requestWithTimeout(method, params, out, appServerRequestTimeout)
}

func (s *appServerSession) requestWithTimeout(method string, params any, out any, timeout time.Duration) error {
	id := s.nextID.Add(1)
	ch := make(chan rpcResponseEnvelope, 1)

	s.pendingMu.Lock()
	if s.pending == nil {
		s.pending = make(map[int64]chan rpcResponseEnvelope)
	}
	s.pending[id] = ch
	s.pendingMu.Unlock()

	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}

	deadline := time.Now().Add(timeout)
	if err := s.writeJSONWithTimeout(method, payload, timeout); err != nil {
		s.pendingMu.Lock()
		delete(s.pending, id)
		s.pendingMu.Unlock()
		return err
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		s.pendingMu.Lock()
		delete(s.pending, id)
		s.pendingMu.Unlock()
		return fmt.Errorf("%s timed out", method)
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()
	ctxDone := s.contextDone()
	select {
	case resp := <-ch:
		if resp.Error != nil {
			return fmt.Errorf("%s", strings.TrimSpace(resp.Error.Message))
		}
		if out != nil {
			if err := json.Unmarshal(resp.Result, out); err != nil {
				return fmt.Errorf("decode %s response: %w", method, err)
			}
		}
		return nil
	case <-ctxDone:
		return s.contextErr()
	case <-timer.C:
		s.pendingMu.Lock()
		delete(s.pending, id)
		s.pendingMu.Unlock()
		return fmt.Errorf("%s timed out", method)
	}
}

func (s *appServerSession) writeJSONWithTimeout(method string, v any, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- s.writeJSON(v)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ctxDone := s.contextDone()
	select {
	case err := <-done:
		return err
	case <-ctxDone:
		return s.contextErr()
	case <-timer.C:
		err := fmt.Errorf("%s write timed out", method)
		slog.Warn("codex app-server write timed out, closing session", "method", method, "timeout", timeout)
		s.abortTransport()
		return err
	}
}

func (s *appServerSession) contextDone() <-chan struct{} {
	if s.ctx == nil {
		return nil
	}
	return s.ctx.Done()
}

func (s *appServerSession) contextErr() error {
	if s.ctx == nil {
		return context.Canceled
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return context.Canceled
}

func (s *appServerSession) abortTransport() {
	s.alive.Store(false)
	if s.cancel != nil {
		s.cancel()
	}

	s.procMu.Lock()
	if s.stdin != nil {
		_ = s.stdin.Close()
		s.stdin = nil
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
	s.procMu.Unlock()
}

func (s *appServerSession) notify(method string, params any) error {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
	}
	if params != nil {
		payload["params"] = params
	}
	return s.writeJSON(payload)
}

func (s *appServerSession) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("codex app-server encode: %w", err)
	}

	s.procMu.Lock()
	stdin := s.stdin
	s.procMu.Unlock()
	if stdin == nil {
		return fmt.Errorf("codex app-server connection is closed")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := stdin.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("codex app-server write: %w", err)
	}
	return nil
}
