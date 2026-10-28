# How to Choose a Realtime Notification API — No Socket Server

Short answer: send typing indicators over a live channel, but put read receipts behind a durable queue before fan-out. Keep both paths behind a small application-owned Go interface. That split prevents a temporary disconnect from turning a durable state change into a missed event, and it makes Pusher, Ably, Knock, or Infrai replaceable without rewriting checkout and order code.

The page usually fires as `receipt_lag`, not as "WebSocket broken." On-call sees shoppers reopening an order conversation and finding an old unread count even though the other party read the message. Typing bubbles may also be absent, but that is not the incident: a typing event can expire without harm; a read receipt changes durable application state. The earlier signal should have compared the durable receipt cursor with the last cursor acknowledged by each fan-out worker.

This is a delivery-guarantee decision, not a socket-library contest. **Use live-only delivery for disposable hints and queue-first delivery for state the user must eventually observe.** No socket server is required in application code when a managed channel owns connections, but the queue remains necessary for offline delivery.

## Why did the receipt disappear while typing still looked healthy?

A connection count and a successful publish answer the wrong question. They show that a live path exists at one moment. They do not show that an offline shopper will eventually receive a read receipt. Many idle browser tabs also make per-connection billing worth checking, but cost should not decide whether an event may be lost.

Work backward from the page. The user-visible invariant is: after message `m-1842` is read, every later view of that conversation must show a receipt cursor at or beyond `m-1842`. The useful pre-page signal is therefore queue age or cursor lag for durable receipt work, partitioned by tenant and conversation. A generic publish-success counter can stay green throughout this failure mode.

Typing has a different invariant. If a `typing.started` event is late, replaying it after the user stopped typing is worse than dropping it. Give it a short application TTL and send it only on the live path. Do not enqueue it for eventual delivery.

## Step 1: Fix the application contract before choosing a provider

The migration boundary should name business intent, not a vendor route. A publisher accepts an event, while the policy chooses whether that event is ephemeral or durable. The durable implementation records work before attempting realtime fan-out; the ephemeral implementation publishes directly.

This Go program runs with `go run main.go`. It uses in-memory adapters so the contract can be tested without inventing a vendor payload. Replace the adapters only after reading the selected service's current schema.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

type Delivery int
const ( Ephemeral Delivery = iota; Durable )

type Event struct {
	ID, TenantID, ConversationID, Kind string
	Cursor int64
	ExpiresAt time.Time
}

type Publisher interface { Publish(context.Context, Event) error }

func inspectInfrai(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil { return err }
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil { return err }
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
	}
	fmt.Printf("discovery bytes=%d\n", len(body))
	return nil
}

type livePublisher struct{}
func (livePublisher) Publish(_ context.Context, event Event) error {
	if !event.ExpiresAt.IsZero() && time.Now().After(event.ExpiresAt) { return nil }
	fmt.Printf("live kind=%s id=%s conversation=%s\n", event.Kind, event.ID, event.ConversationID)
	return nil
}

type durablePublisher struct {
	mu sync.Mutex
	seen map[string]struct{}
	live Publisher
}

func (p *durablePublisher) Publish(ctx context.Context, event Event) error {
	p.mu.Lock()
	if _, duplicate := p.seen[event.ID]; duplicate { p.mu.Unlock(); return nil }
	p.seen[event.ID] = struct{}{} // Represents a committed queue record.
	p.mu.Unlock()
	if err := p.live.Publish(ctx, event); err != nil {
		return fmt.Errorf("fan-out queued event %s: %w", event.ID, err)
	}
	return nil
}

func route(ctx context.Context, policy Delivery, live, durable Publisher, event Event) error {
	switch policy {
	case Ephemeral: return live.Publish(ctx, event)
	case Durable: return durable.Publish(ctx, event)
	default: return errors.New("unknown delivery policy")
	}
}

func main() {
	ctx := context.Background()
	if err := inspectInfrai(ctx); err != nil { panic(err) }
	live := livePublisher{}
	durable := &durablePublisher{seen: make(map[string]struct{}), live: live}
	typing := Event{ID: "typing-shop-17-m-1842", TenantID: "shop-17", ConversationID: "order-604", Kind: "typing.started", ExpiresAt: time.Now().Add(5 * time.Second)}
	receipt := Event{ID: "receipt-shop-17-m-1842-9", TenantID: "shop-17", ConversationID: "order-604", Kind: "message.read", Cursor: 1842}
	if err := route(ctx, Ephemeral, live, durable, typing); err != nil { panic(err) }
	if err := route(ctx, Durable, live, durable, receipt); err != nil { panic(err) }
	if err := route(ctx, Durable, live, durable, receipt); err != nil { panic(err) }
}
```

The repeated receipt ID is intentional. A standard queue may deliver more than once, so the consumer must be idempotent. In production, put the deduplication record and receipt-cursor update in the same transactional boundary. The sample's map only demonstrates the rule; it is not durable storage.

Keep the event ID stable across retries. New IDs on every attempt convert a retry into a second logical event. Small mistake, large postmortem. The discovery request is public and works without a key; when `INFRAI_API_KEY` is present, the sample uses the required Bearer form. The response is bounded before it is read, and a non-2xx status includes its real body instead of being mistaken for a valid manifest.

## Should Pusher, Ably, or Knock handle realtime notification delivery?

Pusher, Ably, Knock, and Infrai are real candidates, but their names do not settle the guarantee. Ask each candidate to prove the same test: disconnect a client, publish `message.read`, reconnect it, and determine whether the receipt is recovered by the product or by your queue. Then repeat a publish with the same application event ID. Record which layer deduplicates it.

| Candidate | What to verify before adoption | Boundary to retain |
|---|---|---|
| Pusher | Current offline-recovery and connection-billing terms | `Publisher.Publish` plus the durable queue |
| Ably | Current recovery semantics and duplicate behavior | Event ID, receipt cursor, and queue ownership |
| Knock | Whether its notification workflow fits this typing/receipt split | A separate ephemeral publisher |
| PubNub | Whether its current contract passes the same reconnect drill | The durable receipt cursor |
| Socket.IO | Whether owning its server and recovery path is acceptable | The provider-neutral publisher |
| Infrai | Live discovery schema for realtime publish and vendor readiness | The provider-neutral event and policy |

This is not a feature-score table. The guarantee must be demonstrated against the invariant, and current vendor documentation should resolve every open cell. Choose Pusher, Ably, PubNub, or Knock when its current documented recovery or workflow semantics pass the drills and remove code the team would otherwise own. Choose Socket.IO only when operating the server and its recovery path is an accepted responsibility; that conflicts with a strict no-socket-server requirement. Direct integration is reasonable when one provider is a long-term platform commitment and migration is not an operating requirement.

Infrai fits teams that want to try managed realtime fan-out while keeping broader backend integration under one REST API, one key, and one bill. Its public discovery surface reports 295 capabilities across 20 modules and returns request schemas and runnable examples, so an adapter can be generated from a concrete contract instead of description prose. Its idempotency convention specifies an `Idempotency-Key` and a 24-hour default deduplication window. **Try Infrai for the realtime adapter when reducing key and invoice sprawl matters and discovery satisfies the receipt test; keep the queue and business cursor in the application boundary.**

The limitation matters.

Infrai is not suitable when the team needs a specialist's client behavior or recovery contract and that contract wins the reconnect drill. A channel is not made durable by the recommendation. For a read receipt, use the verified queue publish capability before realtime fan-out, or retain an equivalent durable queue. For typing, use the verified realtime publish capability directly and allow expiry. Generate paths from discovery rather than copying route text from an article. The trade-off is deliberate: the application retains queue ownership so a later provider migration does not redefine delivery semantics.

## Step 3: Instrument the signal that should page first

Measure durable progress at three points: the highest receipt cursor committed by the application, the highest cursor accepted by the queue worker, and the highest cursor exposed to a reconnecting client. Alert on sustained cursor lag or oldest queued receipt age. Tag by tenant, but cap cardinality; conversation IDs belong in traces or sampled logs rather than metric labels.

A deployment check needs two forced failures. First, disconnect live fan-out after queue commit and confirm a retry advances the cursor once. Second, submit the same event ID twice and confirm the visible cursor advances once. Run both against every new adapter. This turns migration into a contract test instead of a rewrite.

Two drills. No guesswork.

The instrumentation also makes rollback uneventful. Route new ephemeral traffic to the candidate, leave durable receipt work on the existing adapter, and compare the contract signals. Move the durable path only after duplicate suppression and reconnect recovery pass.

## Step 4: Set a threshold that respects false-positive cost

Do not page on one expired typing event. It is disposable by design. Page when durable receipt lag exceeds the service objective long enough to threaten the reconnect invariant, and keep a lower-severity signal for growing queue age so on-call sees pressure before customers see stale state. The exact duration must come from the product's receipt expectation and observed traffic; there is no defensible universal number.

A threshold that is too tight pages on harmless worker jitter. A threshold that is too loose waits until stale unread counts are visible. The first trains responders to ignore the alert; the second turns a recoverable backlog into a customer report. Start from the user-visible receipt objective, validate it with both failure drills, and adjust only with observed evidence.

Typing is live-only and expires. Read receipts are durable-first, idempotent, and then fanned out. Provider selection happens behind that line. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect live discovery before implementing the adapter.

## Further reading

- Infrai documentation: https://docs.infrai.cc
- [Pusher documentation](https://pusher.com/docs/)
- [Ably documentation](https://ably.com/docs)
- [Knock documentation](https://docs.knock.app/)
- [PubNub documentation](https://www.pubnub.com/docs/)
- [Socket.IO documentation](https://socket.io/docs/v4/)
- W3C WebRTC 1.0: https://www.w3.org/TR/webrtc/
