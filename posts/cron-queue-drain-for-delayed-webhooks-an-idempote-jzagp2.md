# Cron Queue Drain for Delayed Webhooks: An Idempotent Worker Pattern

Short answer: use cron to start a bounded queue drain for pending webhooks, then let idempotent workers handle delivery; do not make the cron process perform the outbound calls itself.

That choice gives up some immediate latency in exchange for a recoverable handoff. For a B2B SaaS system draining a rate-limited worker pool, that is usually the right trade when the alternative is a nightly batch that can be interrupted halfway through delivery. The schedule is a prompt to inspect durable state. It is not evidence that a webhook was sent.

I have been paged for missed jobs and duplicate deliveries. The useful incident question was never “did cron run?” It was “what durable state proves that this delivery was claimed, published, attempted, and completed?” That distinction drives the design below.

## The incident lesson: a clock cannot own delivery

A common implementation selects pending rows at night and sends each webhook in a loop. It looks small in a code review. Under load, it becomes a queue with the least useful queue properties: no clear lease, no durable handoff, and no clean answer when the process exits after the receiver accepted a request but before the local status update.

Keep the loop short.

That is the boundary.

The safer sequence is:

1. Cron invokes a protected public HTTP handler.
2. The handler claims a bounded set of pending deliveries.
3. It records the enqueue intent with the delivery identity.
4. A queue receives references to those deliveries.
5. Workers reload the authoritative record, call the receiver, and record completion.

The transactional outbox pattern is useful between steps two and four. It gives the application a durable record of the intent to publish, so a restart can reconcile an outbox row rather than guessing from an in-memory loop. Consider the failure sequence explicitly: the handler claims delivery `acct-42/webhook-17`, writes an outbox row, publishes a queue message, and loses its connection before seeing the publish response. A timeout is not proof that publication failed. The reconciler must be able to find that outbox row, retry the same identity, and let the worker ask storage whether the delivery is still eligible. If the first queue message is already in flight, the duplicate must be harmless. If the receiver accepted the first request while the worker was waiting for its response, the next attempt must carry the same idempotency key. That is the evidence trail an operator can use after a deploy, process restart, or partial network failure; an in-memory `sent` flag has none of it. The pattern does not make an external HTTP side effect exactly once. The receiver still needs an idempotency key, and the worker must use the same key on every retry.

The recovery scan should select state, not a narrow time window. A query such as “rows created since the previous tick” can lose work after a paused schedule. A pending-state scan can find old work on the next run. Give it a stable order, a claim limit, and a lease or equivalent state transition. Watch the age of the oldest pending delivery; a green cron invocation with an aging queue is still an incident.

## How should cron enqueue pending webhooks for a delayed worker pattern?

The handler's contract can be deliberately boring: claim no more than the pool can absorb, publish references rather than oversized payloads, and return after the handoff. The authoritative body and delivery state stay in storage. A message should carry a stable delivery ID and enough routing information for the worker to reload the record.

This is the part I would put in a runbook because it is where duplicate delivery usually starts. If publication succeeds but the handler loses the response, a retry may publish the same delivery again. That is acceptable only when the worker and receiver converge on the same delivery ID. Marking a row complete before the receiver accepts the request creates missed work; marking it complete after acceptance but without an idempotency contract creates ambiguous duplicates.

Here is the state transition in Go. The queue is intentionally an interface: the scheduling rule should survive a change in queue implementation. `CompleteOnce` represents a durable uniqueness constraint in production, not a process-local map.

```go
package main

import (
	"context"
	"errors"
	"fmt"
)

type Delivery struct {
	ID      string
	Payload []byte
}

type Queue interface {
	Publish(ctx context.Context, delivery Delivery) error
}

type Store interface {
	ClaimPending(ctx context.Context, limit int) ([]Delivery, error)
	CompleteOnce(ctx context.Context, deliveryID string) (bool, error)
}

func enqueueBatch(ctx context.Context, store Store, queue Queue, limit int) error {
	deliveries, err := store.ClaimPending(ctx, limit)
	if err != nil {
		return fmt.Errorf("claim pending deliveries: %w", err)
	}

	for _, delivery := range deliveries {
		if err := queue.Publish(ctx, delivery); err != nil {
			return fmt.Errorf("publish delivery %s: %w", delivery.ID, err)
		}
	}
	return nil
}

func handleDelivery(ctx context.Context, store Store, delivery Delivery, send func(context.Context, Delivery) error) error {
	// The receiver should treat delivery.ID as its idempotency key.
	if err := send(ctx, delivery); err != nil {
		return err
	}

	completed, err := store.CompleteOnce(ctx, delivery.ID)
	if err != nil {
		return fmt.Errorf("record completion: %w", err)
	}
	if !completed {
		return errors.New("delivery was already completed")
	}
	return nil
}
```

The example separates enqueueing from delivery, but it does not pretend to solve the hardest persistence detail. A real claim must be atomic with the state change or outbox write, and a failed publication needs a visible retry state. If the queue rejects a message because it is rate-limited, retain the same identity and apply backoff; do not create a fresh delivery ID for the retry.

## Where latency, cost, and correctness pull apart

Batching reduces scheduler invocations and amortizes storage work, but it increases the waiting time for the last item in a batch. A smaller batch lowers the blast radius and queue burst, but it can leave a rate-limited pool underused. There is no universal batch size. Start with the receiver's rate limit, worker concurrency, request timeout, and the maximum acceptable age for a pending webhook; then measure queue age rather than optimizing the cron duration in isolation.

The queue should contain a reference when payloads can grow. Cloud Tasks, for example, documents a 256 KB task limit and a maximum message retention period of 30 days. Those constraints are reasons to keep the canonical webhook body in application storage and define a retention policy deliberately. They are not reasons to silently drop old deliveries.

One more operational boundary matters: scheduled work is not necessarily near-real-time work. Delayed messages cannot be scheduled more than seven days ahead, and cron timing can have seconds of jitter. A webhook that must follow its source transaction closely should be published by a continuous outbox worker, with the scheduled scan retained as repair. A date months away belongs in durable storage until it enters the allowed delay horizon.

The catch is that a nightly reconciler is unsuitable when the product promise is sub-minute delivery or when the workload needs dependency graphs, joins, compensation, or multiple independent consumer groups. Use a workflow engine for a durable multi-step process with waits and compensation. Use a data-pipeline scheduler when backfills and dependency graphs are the primary unit of work. Keep this pattern for bounded recovery and dispatch.

## What should operators verify after a cron queue drain?

Alert on the oldest pending delivery, claimed-but-not-complete count, queue depth, worker failure rate, and repeated delivery IDs. Record a correlation trail for each ID: source row, claim or lease, enqueue intent, queue attempt, receiver response, and completion transition. A successful HTTP response from the cron handler proves only that the handler answered.

During an incident, inspect one delivery from beginning to end before changing batch size. If the outbox intent exists but no queue attempt exists, the publisher is the gap. If attempts exist but completion does not, inspect receiver timeouts and idempotency handling. If completion exists while the receiver reports no request, the state transition is wrong. This sequence makes the postmortem actionable because each boundary has evidence.

The method is intentionally conservative. It favors a second delivery that the receiver can safely collapse over a missed delivery that cannot be recovered from a clock tick. Your mileage may vary when the receiver has no idempotency facility; in that case, the business owner must choose between duplicate side effects and a stronger coordination protocol before increasing retries.

## References

- https://cloud.google.com/tasks/docs/dual-overview
- https://microservices.io/patterns/data/transactional-outbox.html
