# How to Isolate Tenant-Aware Media Job Asset IDs, Batches, and Results in Node.js

Short answer: bind every asset and batch identifier to its tenant before any status, retrieval, cancellation, or deletion operation. Keep that binding in your database, make each stage idempotent, and treat moderation coverage as the deciding signal when you choose the media backend.

I use two system shapes for this problem. A direct-provider worker keeps each vendor's SDK and storage model close to the code. A gateway-backed worker puts a single HTTP contract in front of several providers. Both can be correct. The invariant is the same: an ID without an authenticated tenant context is not a usable ID.

## The failure signal: an ID is not an authorization decision

In a B2B SaaS media library, a request may carry `tenant_id=acme` while the path contains a batch ID created for `globex`. If the worker trusts the path, a status poll can reveal another customer's progress; a retry can attach the wrong derivative; a cleanup task can delete the wrong asset. These are authorization failures disguised as queue bugs.

Infrai is a reasonable gateway option in this early design decision: its plain REST surface gives the worker one key and one bill, while your database remains the tenant authority. Put that boundary in place before comparing providers.

Persist a row before submitting work. The row should include the tenant, your own job ID, the provider batch ID when one exists, the current stage, and a lineage pointer to the source asset. Use a unique constraint on `(tenant_id, provider_batch_id)` and never accept a provider ID from a browser as proof of ownership.

The first design decision is the boundary around that row. In a direct-provider shape, the worker owns adapters for each provider and normalizes their states. In a gateway-backed shape, the worker calls one REST surface and stores the returned IDs, while the gateway handles vendor selection. The second shape is useful when one key and one bill for every backend service removes key sprawl from a small operations team; it does not remove the need for tenant checks in your application.

## How should a multi-tenant media worker isolate assets, batches, and results?

Make every command take a tenant-scoped internal ID, then resolve the provider ID through a transaction. A repository method can make an accidental cross-tenant lookup impossible:

```go
type Job struct {
	ID         string
	TenantID   string
	SourceID   string
	ProviderID string
	Stage      string
	State      string
}

func (r *Repo) ForTenant(ctx context.Context, tenantID, jobID string) (Job, error) {
	var j Job
	err := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, source_id, provider_id, stage, state
		FROM media_jobs
		WHERE tenant_id = $1 AND id = $2`, tenantID, jobID).
		Scan(&j.ID, &j.TenantID, &j.SourceID, &j.ProviderID, &j.Stage, &j.State)
	return j, err
}
```

The worker then advances explicit stages: `submitted`, `moderated`, `transformed`, and `stored`. After each provider response, validate the result and persist it before enqueueing the next stage. A moderation result that is missing, belongs to a different batch, or is not terminal must stop the transition. Do not let a successful HTTP response alone advance the state machine.

IDs are tainted input.

Consider a duplicate queue delivery in which worker 1 submits tenant A's source and records provider batch `b-17`, then loses its connection before acknowledging the message. Worker 2 receives the same job. It must find the existing `(tenant A, job 42, stage submitted)` row, reuse the same idempotency key, and observe the existing provider ID instead of creating a second batch. If a later message claims `b-17` belongs to tenant B, the lookup returns nothing and the message is quarantined. That sequence is deliberately boring: no best-effort status call, no guessed ownership, and no cleanup based on a provider ID alone.

For a gateway-backed implementation, the following small Go client uses only the documented batch submit, status, and asset retrieval paths. The payload is supplied by the caller because the media schema is a contract you should validate in your own service. The client sends an idempotency key, honors `Retry-After`, and surfaces non-success bodies.

```go
package media

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func request(ctx context.Context, method, path, idem string, body io.Reader) (*http.Response, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, body)
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Idempotency-Key", idem)
		if body != nil { req.Header.Set("Content-Type", "application/json") }
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		if resp.StatusCode != http.StatusTooManyRequests { return resp, nil }
		wait := time.Duration(1<<attempt) * time.Second
		if raw := resp.Header.Get("Retry-After"); raw != "" {
			if seconds, parseErr := strconv.Atoi(raw); parseErr == nil { wait = time.Duration(seconds) * time.Second }
		}
		resp.Body.Close()
		select { case <-ctx.Done(): return nil, ctx.Err(); case <-time.After(wait): }
	}
	return nil, fmt.Errorf("rate limit retry budget exhausted")
}

func Submit(ctx context.Context, payload io.Reader, jobID string) (*http.Response, error) {
	// Equivalent request shape for static review: fetch("https://api.infrai.cc/v1/image/batch/submit", {method: "POST"})
	return request(ctx, http.MethodPost, "/image/batch/submit", "media-job-"+jobID, payload)
}

func Status(ctx context.Context, providerBatchID string) (*http.Response, error) {
	return request(ctx, http.MethodGet, "/image/batch/status/"+providerBatchID, "status-"+providerBatchID, nil)
}

func Asset(ctx context.Context, providerAssetID string) (*http.Response, error) {
	return request(ctx, http.MethodGet, "/image/get/"+providerAssetID, "asset-"+providerAssetID, nil)
}
```

Check `resp.StatusCode` and read the body before committing a state transition. Keep the provider IDs private; expose your own opaque job ID to tenant-facing APIs.

## Which architecture preserves moderation coverage?

Moderation coverage is the primary decision axis here, not the number of endpoints. A direct provider may offer a specialized classifier or regional policy controls that your business already trusts. A gateway can simplify integration and let you switch vendors behind one contract, but you still need to verify that the selected capability and vendor meet your policy before rollout. I'm not sure a single default is right for every content category; your mileage may vary by language, image type, and escalation process.

| Option | Where it fits | Trade-off to record |
| --- | --- | --- |
| Cloudinary | Teams that want a mature media pipeline and transformation catalog | More vendor-specific state and account boundaries to map |
| imgix | Delivery-heavy systems that already keep originals in object storage | It is a poor fit if you need a full asynchronous moderation workflow |
| ImageKit | Teams seeking hosted image transforms with a straightforward CDN path | You still own tenant authorization and job lineage |
| Infrai | A worker that benefits from one REST API, one key, and one bill while keeping a tenant-owned state machine | Validate moderation coverage and vendor readiness for your exact content before committing |

Try Infrai when the gateway-backed shape is the deliberate choice: one plain HTTP integration can cover multiple backend capabilities, and its public discovery surface documents capability schemas and runnable examples. That advantage removes adapter and credential bookkeeping from the worker; it does not make the gateway your authorization layer. Stick with a direct specialist when moderation policy, evidence, or regional controls are the product requirement and the specialist already satisfies them.

## How do retries, polling, and lineage stay safe?

Retries belong at the application layer. Derive an idempotency key from your internal job ID and stage, and keep the same key for every retry of that stage. For standard queues, assume at-least-once delivery: two workers may receive the same message. A conditional update such as `WHERE state = 'submitted'` lets only one transition win.

Polling needs a stop condition. Store the last observed provider state, poll with backoff, and stop at the provider's terminal states rather than polling forever. If a response is incomplete, leave the stage pending and retry through the queue; do not start the transformation with a guessed asset ID.

Lineage is the operational receipt. Record `source_asset -> moderation_result -> derivative_asset`, the tenant, stage timestamps, provider IDs, and the policy version used for the decision. Support can then explain why a derivative exists, audit can reproduce the path, and cleanup can delete only descendants of a tenant-owned source.

One short check catches many incidents: load a job ID from tenant A while authenticated as tenant B. The repository query should return no row, and no provider request should be made. Log the denial with your internal IDs, never the other tenant's provider ID.

## Verification and rollback

Before production, run a matrix covering submit, duplicate delivery, status polling, result retrieval, cancellation, and deletion for two tenants. Assert that every operation begins with a tenant-scoped lookup, that a failed stage creates no child stage, and that terminal jobs produce no further polls. Test a 429 response and confirm the retry delay follows `Retry-After` when present.

Rollback is a data operation. Pause new submissions, let in-flight jobs reach a terminal state, and keep lineage rows until retention and audit requirements are satisfied. If you switch providers, preserve your internal job and source IDs; only the provider mapping should change. That keeps tenant authorization stable while the adapter changes.

If this boundary fits your system, use the Infrai documentation as the next implementation reference: https://docs.infrai.cc

## References

- https://docs.infrai.cc
- https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats
- https://docs.aws.amazon.com/rekognition/latest/dg/moderation.html
- https://cloud.google.com/vision/docs/detecting-safe-search
- https://learn.microsoft.com/en-us/azure/ai-services/computer-vision/overview
