# Tenant Files Across US/EU: Why Browser Presigned PUT Preflight Fails

Short answer: a presigned PUT authorizes an object write, but it does not authorize browser JavaScript to cross an origin boundary. For a property-management portal retaining signed documents until an explicit deletion deadline, use direct object storage upload only when the team that owns the application also owns and tests the exact CORS policy; otherwise, send the file through the application backend and keep retention state beside the authorization decision.

This is an access-control decision before it is a delivery optimization. A direct path removes an application hop. A proxied path gives the application one place to reject an untrusted file, bind it to a lease, record its deletion deadline, and decide who may retrieve it. For small signed documents, I favor that control unless measured traffic makes the extra hop a real constraint.

I've been paged by missed jobs and duplicate deliveries. Consider the bounded failure that shapes this design: a tenant signs a renewal document, the browser completes the upload but loses the finalize response, and the user retries. Later, the deletion scheduler delivers the same due item twice. If each attempt creates a new object key or a new state transition, the property record can point at one object while an untracked duplicate survives its deadline. The invariant is blunt: an accepted document is not merely a successful byte transfer, and a deletion request is not merely a queued message. Stable operation identities and committed retention state matter more than which component carried the bytes.

Retries are normal.

## The missing PUT is diagnostic evidence

A signature and a browser preflight answer different questions. The signature tells the storage service that a particular request is authorized under a limited credential. CORS tells the browser whether script from one origin may make that cross-origin request. The browser may send `OPTIONS` before it sends `PUT`; if the response does not allow the page's precise origin, method, and requested headers, the browser stops there. The object write never starts. That distinction explains the familiar split result: a server-side client can upload with the same signed URL while the web page cannot, because the server-side client is not enforcing the browser's cross-origin policy. A successful command-line request therefore proves signature validity, not browser reachability. Origins are exact tuples of scheme, host, and port. “US” and “EU” are deployment labels, not origins. `us.portal.example` and `eu.portal.example` need separate consideration, as do a preview hostname and a different local-development port. Don't solve this by reflecting arbitrary origins or allowing arbitrary request headers. Build an allowlist from deployed origins, permit only the required method and headers, and treat changes to that list as production configuration changes. Keep the request boring: if the signing step expects `Content-Type`, the upload must send the same value, and if the browser proposes extra headers, they become part of the preflight negotiation. Capture `Origin`, `Access-Control-Request-Method`, and `Access-Control-Request-Headers` from the failing `OPTIONS` exchange before changing policy. A 403 response to the later `PUT` points toward authorization or signature mismatch; no `PUT` in the network trace points back to preflight. Those are different runbooks.

## How should a beginner debug browser direct avatar upload CORS and presigned PUT preflight?

Start in the browser's network panel, not with another server-side upload. Find the `OPTIONS` request and write down the page origin, requested method, and requested headers exactly as sent. Then inspect the preflight response. The allowed origin must match the requesting origin, `PUT` must be allowed, and every non-simple requested header must be accepted. After that gate passes, inspect the actual `PUT` as a separate request. Use a small, known file and a newly issued URL. Avoid changing the CORS policy, signer inputs, frontend headers, and object key in the same attempt; four moving parts produce an ambiguous green result. Repeat the check from the real US and EU portal hostnames because success from one origin says nothing about another.

It's tedious.

It is also much faster than debugging the word “CORS” as if it were a single failure mode.

The same checklist applies to an avatar, but the risk decision can differ. An avatar is usually replaceable and intended for repeated delivery. A signed lease or inspection acknowledgment may have a contractual retention deadline and narrow readers. In the latter case, upload success is only the first transition in a longer state machine:

1. The authenticated user receives permission for one bounded upload.
2. The service validates size, type, filename, and lease association.
3. The object is stored under a server-selected key.
4. A database transaction records the object identity and deletion deadline.
5. A scheduled deletion job acts only after that deadline and records its result.

OWASP recommends allowlisting extensions, validating the actual file type rather than trusting the `Content-Type` header, changing filenames, limiting size, restricting authorized uploaders, and storing files outside the webroot or on a separate host. Those controls are easiest to reason about at an application boundary. A direct upload can still use them, but it needs an explicit finalize step before the application treats the object as an accepted document.

## Implement the retention ledger before direct delivery

Give the upload attempt a stable operation ID. Let the server choose the object key, persist the deletion deadline as data rather than infer it from object age, and make finalization compare-and-set the document from `pending` to `retained`. The deletion worker should claim a due record, delete the corresponding object, and make a repeated claim converge on the same terminal state. Alert on overdue retained records, not merely on queue depth; a quiet queue can coexist with a missed schedule.

This small Go example shows the preventative application path. The storage and repository interfaces are intentionally generic. The important part is the order: authenticate, parse a bounded body, validate, store under a server-controlled key, then commit the deadline with an idempotency key.

```go
package documents

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const maxDocumentBytes = 10 << 20

type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader) error
}

type Repository interface {
	CommitRetention(ctx context.Context, operationID, leaseID, objectKey string, deleteAt time.Time) error
}

type Handler struct {
	Objects ObjectStore
	Records Repository
	Now     func() time.Time
}

func (h Handler) Upload(w http.ResponseWriter, r *http.Request) {
	operationID := r.Header.Get("Idempotency-Key")
	leaseID := r.Header.Get("X-Lease-ID")
	if operationID == "" || leaseID == "" {
		http.Error(w, "missing operation or lease identifier", http.StatusBadRequest)
		return
	}

	deleteAt, err := time.Parse(time.RFC3339, r.Header.Get("X-Delete-At"))
	if err != nil || !deleteAt.After(h.Now()) {
		http.Error(w, "invalid deletion deadline", http.StatusBadRequest)
		return
	}

	body := http.MaxBytesReader(w, r.Body, maxDocumentBytes)
	defer body.Close()
	objectKey := fmt.Sprintf("leases/%s/documents/%s", leaseID, operationID)

	if err := h.Objects.Put(r.Context(), objectKey, body); err != nil {
		http.Error(w, "upload rejected", http.StatusBadGateway)
		return
	}
	if err := h.Records.CommitRetention(r.Context(), operationID, leaseID, objectKey, deleteAt); err != nil {
		http.Error(w, "retention record rejected", http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
```

The production implementation still needs authentication, authorization against the lease, content inspection, audit logging, and reconciliation for an object stored before a database commit. The repository must make `CommitRetention` idempotent for the operation ID. The deletion worker needs the same discipline.

No magic here.

## Which team should own each part of the upload path?

| Path | Operational advantage | Cost or limitation | Prefer it when |
|---|---|---|---|
| Browser to object storage | Removes the application from the byte path | Requires managed CORS, a signer, a finalize step, and cleanup of abandoned uploads | Files are large or frequent, and the team can operate every browser origin and lifecycle transition |
| Browser through application | Centralizes authorization, validation, audit, and retention state | Adds application bandwidth and an extra hop | Files are small, private, and tied to a strict business record or deletion deadline |
| Separate upload service | Isolates file policy and scaling from the main application | Adds another deployed component and ownership boundary | Several applications share the same mature upload policy |

For signed property documents, the catch with direct upload is not that it cannot be secured. It is that the simple-looking data path pushes complexity into signing, CORS configuration, finalization, abandoned-object cleanup, and retention reconciliation. If the portal team cannot own those controls end to end, the extra backend hop is the smaller system.

Stick with direct upload when payload size or volume makes proxying demonstrably unsuitable and the storage policy is under the same operational ownership. Use the proxy when centralized access control and deadline recording matter more than shaving a hop. A separate upload service fits a larger organization with several portals, but it is not suitable when the team cannot staff another production boundary. Your mileage may vary with document size and regional topology; load tests and actual browser traces resolve that uncertainty.

Delivery needs its own rule. Signed documents should not become public merely because direct upload was convenient. Authorize reads, use short-lived delivery credentials where appropriate, and set response caching according to the sensitivity and reuse model. MDN documents that `no-store` tells caches not to store a response, while `private` permits storage in a private cache but not a shared cache. Pick intentionally; neither directive replaces application authorization.

## A release gate with two clocks

Before release, test one upload from each production origin, including the exact headers the shipping client sends. Verify the preflight, the write, the idempotent finalize retry, an authorized read, and a denied read. Then exercise deletion with a short test deadline in a non-production environment and confirm that duplicate worker delivery reaches one terminal database state.

Watch the invariant afterward. Count retained records past `delete_at`, objects with no committed record, repeated finalization attempts, and deletion age from deadline to completion. Put the remediation in the runbook: stop new finalizations if reconciliation is losing ownership, preserve audit records, and repair state from the authoritative retention table. Do not make a bucket listing the source of truth for a contractual deadline.

The final decision is narrow. Direct uploads are a delivery tool, not a retention model. For private property documents with explicit deletion deadlines, choose the path that keeps authorization, acceptance, and deletion ownership provable; optimize the byte path only after those transitions survive retries.

## Sources

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
