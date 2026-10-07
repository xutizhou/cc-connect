package core

import (
	"context"
	"log/slog"
	"sync"
)

// asyncStreamPreview keeps platform network I/O off the agent event reader.
// Producers only append to an in-memory string and signal a size-one wake
// channel. The worker coalesces everything that arrived while a slow platform
// call was in flight, so platform latency cannot backpressure model events.
type asyncStreamPreview struct {
	ctx      context.Context
	preview  *streamPreview
	fallback func(string)

	mu        sync.Mutex
	fullText  string
	consumed  int
	finalText string
	// finalTransform lets callers render the terminal state differently from
	// live frames (for example a green, completed Feishu progress card rather
	// than another blue running card).
	finalTransform func(string) string
	finishing      bool
	aborted        bool

	wake chan struct{}
	done chan struct{}
}

func (ap *asyncStreamPreview) setFinalTransform(transform func(string) string) {
	ap.mu.Lock()
	ap.finalTransform = transform
	ap.mu.Unlock()
}

func newAsyncStreamPreview(ctx context.Context, preview *streamPreview, fallback func(string)) *asyncStreamPreview {
	ap := &asyncStreamPreview{
		ctx:      ctx,
		preview:  preview,
		fallback: fallback,
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go ap.run()
	return ap
}

func (ap *asyncStreamPreview) appendText(text string) {
	if text == "" {
		return
	}
	ap.mu.Lock()
	if ap.finishing || ap.aborted {
		ap.mu.Unlock()
		return
	}
	ap.fullText += text
	ap.mu.Unlock()
	ap.signal()
}

// finish requests one authoritative final update and returns immediately.
// The platform worker delivers it after any in-flight preview request.
func (ap *asyncStreamPreview) finish(finalText string) {
	ap.mu.Lock()
	if ap.finishing || ap.aborted {
		ap.mu.Unlock()
		return
	}
	ap.finishing = true
	ap.finalText = finalText
	ap.mu.Unlock()
	ap.signal()
}

// abort stops an unfinished preview. Cleanup is also asynchronous because a
// platform delete can be just as slow as a send or update.
func (ap *asyncStreamPreview) abort() {
	ap.mu.Lock()
	if ap.finishing || ap.aborted {
		ap.mu.Unlock()
		return
	}
	ap.aborted = true
	ap.mu.Unlock()
	ap.signal()
}

func (ap *asyncStreamPreview) signal() {
	select {
	case ap.wake <- struct{}{}:
	default:
	}
}

func (ap *asyncStreamPreview) run() {
	defer close(ap.done)
	for {
		select {
		case <-ap.ctx.Done():
			return
		case <-ap.wake:
		}

		for {
			ap.mu.Lock()
			aborted := ap.aborted
			finishing := ap.finishing
			finalText := ap.finalText
			finalTransform := ap.finalTransform
			text := ap.fullText
			start := ap.consumed
			ap.consumed = len(text)
			ap.mu.Unlock()

			if aborted {
				ap.preview.discard()
				return
			}
			if start < len(text) {
				if ap.preview.canPreview() {
					ap.preview.appendText(text[start:])
				} else if !finishing && ap.fallback != nil {
					// Platforms without editable previews still receive live
					// progress, but the send remains off the event reader.
					ap.fallback(text[start:])
				}
			}
			if finishing {
				if finalTransform != nil {
					ap.preview.setTransform(finalTransform)
				}
				// An App Server reconnect may recover only an authoritative
				// EventResult, with no preceding deltas. Open the editable card
				// here so that terminal-only recovery does not fall back to a
				// plain text message.
				if finalText != "" && text == "" && ap.preview.canPreview() {
					ap.preview.appendText(finalText)
				}
				if finalText != "" && !ap.preview.finish(finalText, "") && ap.fallback != nil {
					ap.fallback(finalText)
				}
				slog.Debug("async stream preview finished", "content_len", len(finalText))
				return
			}

			ap.mu.Lock()
			more := ap.consumed < len(ap.fullText) || ap.finishing || ap.aborted
			ap.mu.Unlock()
			if !more {
				break
			}
		}
	}
}
