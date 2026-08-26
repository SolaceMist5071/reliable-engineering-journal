# Scheduled Cleanup Retries: HTTPS Push or a Polling Consumer?

The constraint that changes this choice is simple: a periodic cleanup should not keep a web request open while it waits for a retry. **Short answer:** enqueue each cleanup attempt, then use HTTPS push when the receiver is already public and the work is short; use a polling consumer when the processor is private or needs tighter control over claiming and retry timing. In both designs, make the cleanup idempotent before tuning latency or cost.

I have been paged by missed jobs and duplicate deliveries. The transport was rarely the whole problem. A lost response after a successful delete, an expired worker lease, or a retry that ignored the original job ID can turn a routine cleanup into an incident.

## How should a webhook processor choose push delivery or a polling consumer for failed jobs?

Start with reachability and control. HTTPS push means the queue or scheduler calls a public endpoint. The processor must authenticate the request, accept the job durably, and respond within the delivery contract. This is a good fit for a small cleanup command that can hand work to an internal worker after recording it.

Polling reverses the direction. A worker behind a firewall asks for work, claims one item, runs the cleanup, and acknowledges only after the result is durable. That adds worker capacity and claim-state operations, but it lets the team choose when to poll, how long a lease lasts, and how to pause consumption during a deploy.

The latency-versus-cost decision is therefore operational, not just a benchmark. Push can reduce idle worker capacity when jobs are sporadic, while polling can be easier to keep predictable when a private worker is already running. Your mileage will vary: burst size, endpoint timeout, lease duration, and the cost of an extra worker matter more than the label “push” or “pull.”

The endpoint is not the retry policy.

| Choice | Useful when | Main trade-off |
| --- | --- | --- |
| HTTPS push | The handler is public, short-lived, and easy to authenticate | Sender timing and retry behavior become part of the receiver's contract |
| Polling consumer | The processor is private or owns a domain-specific retry policy | The team operates worker capacity, claims, leases, and empty-queue backoff |

## The failure boundary is the acknowledgement

Treat acknowledgement as a destructive state transition. Once a message is acknowledged, the queue may remove it; that is not the same as proving that the business effect happened exactly once. A timeout can hide a completed request, and a lease can expire while a worker is still finishing. Duplicate deliveries are expected in an at-least-once design.

For a cleanup job, persist the delivery ID and the intended resource before performing the side effect. Put a uniqueness constraint on the delivery ID. On a second delivery, return success after confirming the original attempt is recorded, but do not run the deletion again merely because the sender retried.

The same transaction can sit behind an HTTPS handler and a polling loop:

```go
package cleanup

import (
	"context"
	"database/sql"
)

// Accept records the job before either transport acknowledges it.
func Accept(ctx context.Context, db *sql.DB, deliveryID, resource string) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO cleanup_jobs (delivery_id, resource, state)
		VALUES ($1, $2, 'queued')
		ON CONFLICT (delivery_id) DO NOTHING`, deliveryID, resource)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}

	created, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return created == 1, nil
}
```

The receiver should report retryable failure when it cannot durably accept the job. A polling worker should leave the item unacknowledged or release its claim in that case. If acceptance succeeds but the cleanup later fails, record the failure and schedule another attempt with a bounded backoff. A malformed resource identifier needs a terminal state and an operator-visible reason; it should not spin forever.

## What should the retry runbook verify before production?

Test the ambiguous windows deliberately. Deliver one job twice and check for one cleanup effect, two observable delivery attempts, and a stable final state. Interrupt the database transaction, then restore it and confirm the job can be accepted again. Pause the worker after claiming a job and let its lease expire. A second delivery may happen; the result must still be harmless.

I also test a dependency response of `429` and a dependency timeout separately. Those signals need different backoff and alert context. The exact values belong in the dependency contract, and I'm not sure any generic number would fit every webhook processor. What matters is that the runbook names the retry budget, the next-attempt timestamp, and the reason an item becomes terminal.

Observe the whole path with the same identifiers: schedule ID, delivery ID, cleanup resource, attempt number, state transition, and acknowledgement time. Alert on age of the oldest queued job, retry rate, terminal failures, and the gap between acceptance and completion. A dashboard that shows only HTTP success can miss a queue accumulating work behind a healthy endpoint. For one concrete drill, publish a cleanup for an object that is safe to remove, deliver it twice, and then make the dependency timeout after the first durable acceptance. The first request may have completed its database transaction while the client saw no response; the second request must find the same delivery ID and stop at the idempotency check. Next, let the worker lose its lease while the delete call is in flight. The replacement worker should observe the recorded state, decide whether the effect is already complete, and leave one explainable terminal record. If the logs cannot answer which process owned the attempt, the test has found a missing operational field, not a transport problem. Don't close the incident on a green HTTP status alone; it's the state transition that tells you what happened.

## Where does each design stop being suitable?

HTTPS push is not suitable when the processor cannot be reached from the sender's network, when the handler routinely exceeds the sender's timeout, or when the team cannot safely expose and authenticate an ingress point. Stick with a polling consumer when those conditions describe the system.

Polling is not suitable when the team has no owner for worker scaling, lease recovery, or queue-drain behavior. A worker that polls too aggressively burns capacity; one that sleeps too long raises cleanup latency. If the job needs multi-step orchestration, compensation, or fan-out, a plain delivery mechanism may be too small for the state model. Keep the retry record and business idempotency rules regardless of the transport.

Rollback should pause the schedule first, then stop new publication and let acknowledged work drain. Keep the previous consumer version available for unacknowledged jobs. Move terminal failures to a dead-letter path for review and redrive only after correcting the cause; RabbitMQ's dead-letter exchange documentation describes this separation between rejected or expired messages and later handling. Do not delete the queue as a rollback shortcut.

The practical decision is a runbook choice: public ingress and short work favor push, private execution and explicit control favor polling. Neither choice removes the need to record intent, make the cleanup idempotent, and prove the ambiguous delivery windows under load.

## References

- https://www.rabbitmq.com/docs/dlx
- https://www.inngest.com/docs
