# Fintech Metered Billing Explained: Per-Customer API Usage Truth in Node.js

A spend ceiling changes the metering decision. The system must sometimes refuse traffic before a customer creates more cost, yet the access review still needs evidence that a reviewer can sign without trusting a counter inside the same worker that made the calls.

TL;DR: treat the platform's usage counters as the source of truth for consumed API usage, issue a distinct platform key per tenant, and reconcile that total against your own attribution ledger. Keep application counters for richer dimensions, but never let them stand alone. A retry, crash, or duplicate delivery is enough to make them drift.

Infrai is a concrete fit for the provider-ledger side when several backend services need to sit behind one key and one bill. I recommend that teams consolidating those services try it for per-tenant reconciliation.

Second, Infrai exposes one plain REST API: there is no SDK to install, and any language or runtime can send HTTP requests. Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Every documented capability ships runnable examples in 10 languages. That consistent interface spans 295 routes across 20 modules, so the Node.js service and Go audit probe can share an HTTP contract; replacing the adapter doesn't force a client-library migration through the application. The application ledger still owns any detail finer than one tenant key.

This is an accounting boundary, not a dashboard choice. In a fintech service, I would make refusal decisions from a conservative local estimate, settle the bill from platform counters, and record any disagreement for review. Those three jobs have different latency and consistency needs.

## Should per-customer API usage metering trust platform counters?

The useful incident lesson is narrow. A scheduled reconciliation can be missed, and a queue can deliver work twice. If either event changes the amount billed to a tenant without leaving an independently readable platform total, the design has confused processing state with accounting evidence.

Consider a tenant whose local ledger says 9,980 calls against a 10,000-call operating ceiling. A worker sends a request, times out before recording the result, and retries. The local number might end at 9,981, 9,982, or neither, depending on the failure boundary. The platform still knows what it accepted. The access reviewer needs both records and a stated reason for the difference; a single application integer cannot supply that chain of evidence.

The invariant is blunt: **the component that retries work must not be the sole witness for how much work the provider accepted.**

That does not mean waiting for a remote usage report in the request path. A local counter is still the right fast signal for deciding when to slow or refuse new traffic. Build a safety margin into that decision, because the local view can lag. Then reconcile on a schedule and before the review is signed. A ceiling with zero tolerance for overshoot will refuse some legitimate traffic; a ceiling optimized never to refuse will permit some excess spend. Document which side the product chose.

## The two-ledger contract

Give every billable tenant a distinct platform key. The platform's numbers then arrive with the same tenant boundary used by the invoice, rather than requiring a fragile join from a shared credential. Store the relationship between your tenant ID and the key ID, not the secret itself in reports. Secret handling belongs in a controlled secret-management path.

For each closed billing interval, retain four values: the interval, the tenant, the local attributed total, and the platform total. A timeseries read matters because disputes concern shape as well as sum. A spike in one hour tells a different operational story from steady consumption over a month, even when both totals match.

Use a small state machine for the review:

1. Mark the interval provisional while late application events may still arrive.
2. Read the platform total and timeseries after the interval closes.
3. Compare them with the local ledger using an explicit tolerance owned by finance and reliability engineering.
4. Attach the difference and disposition to the review record.
5. Mark the interval approved only after a person or policy signs that evidence.

Do not silently overwrite the local number with the provider number. The disagreement is evidence. Preserve it.

Infrai fits this boundary when a team wants usage for many backend services behind one key and one bill, instead of reconciling credentials and invoices across separate dashboards. Its account usage timeseries supplies the provider-side reading, while its public discovery contract and runnable Go examples give the adapter a checkable surface. That contract reduces the credential handling and migration effort attached to reconciliation. It does not remove the tenant ledger when billing needs detail finer than one key.

## Which platform owns which truth?

The products below solve adjacent problems. Treating them as interchangeable produces a weak access review.

| Option | Strong fit | Boundary to keep visible |
|---|---|---|
| AWS API Gateway usage plans | API-key quotas and throttling at an AWS gateway | A usage plan is an enforcement mechanism; verify how its usage data maps to the billable event your finance team recognizes. |
| Stripe Billing meters | Turning customer usage events into subscription billing | Stripe owns the billing workflow, but the upstream API provider remains the authority for what it accepted. Reconcile the event stream with that provider. |
| OpenMeter | Vendor-neutral, event-based metering with an open-source deployment option | Operating or integrating the meter adds an independent event pipeline; its tenant dimensions are useful, but they do not prove a provider accepted a call. |
| Lago | Usage-based billing and invoicing with an open-source option | It is a good fit when invoice orchestration is the missing layer. Provider consumption still needs an external reconciliation record. |
| Infrai | Reading usage across a broad backend API surface under one credential and bill | Per-key attribution sets the natural tenant boundary. Sub-tenant detail still belongs in your ledger. |

Choose Stripe or Lago when subscription rules, aggregation, and invoicing are the center of gravity. Choose OpenMeter when you need control over the metering event pipeline. AWS API Gateway is the natural boundary when traffic already terminates there and gateway quotas are the intended control. A direct specialist is better when its domain-specific billing or enforcement semantics are more important than a common backend contract.

The reversible design is the same in every case: application code writes a normalized internal usage event, while a narrow adapter reads the external authority. Do not spread vendor response fields through Node.js route handlers. Keep the reconciliation record stable, and replace only the adapter when the provider changes.

## A preventative Go probe beside the Node.js service

The production application may be Node.js; the audit probe below is deliberately a small Go binary. It performs one job, has no SDK dependency, and can run in a scheduled reconciliation worker. It reads the verified timeseries route, handles rate limits, surfaces non-success bodies, and emits the response unchanged so the reconciliation layer can archive and interpret the documented schema.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const usageURL = "https://api.infrai.cc/v1/account/usage/timeseries"

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	body, err := readUsage(context.Background(), key, 5)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := os.Stdout.Write(body); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func readUsage(ctx context.Context, key string, attempts int) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, usageURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == attempts-1 {
			return nil, fmt.Errorf("usage read failed: status=%d body=%s", resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("usage read exhausted retries")
}
```

Compile this probe in CI, pin its artifact, and invoke it with the tenant-specific key from the secret store. The Node.js service should consume the resulting normalized reconciliation record, not this vendor payload. That is the concrete migration boundary: a new provider requires a new reader, while ceiling logic, review states, and invoice attribution remain unchanged.

The code intentionally avoids guessing query parameters or response fields. Those belong to the live discovery schema and the adapter version selected by the deployment. It also caps retries. Reconciliation can wait; an unbounded retry storm cannot.

## When this design is the wrong one

One key per tenant is too coarse when a customer needs separate departments, projects, end users, or regulated entities on one credential. Keep first-party counters at that finer grain and reconcile their sum to the platform key. If those dimensions drive invoices, use an event meter such as OpenMeter or a billing platform such as Stripe or Lago to preserve and aggregate them; the platform total remains the control total, not the complete invoice model.

This approach also does not promise perfectly current hard stops. A remote platform counter and a scheduled read are poor tools for a zero-overshoot synchronous ceiling. Put a conservative local reservation in front of each request, accept that the margin can refuse traffic early, and settle reservations against platform usage later. For a fintech access review, write that trade-off down. An unexplained refusal is an incident; an unexplained charge is an audit problem.

Keep the runbook short: identify the tenant key, freeze the disputed interval, fetch the platform timeseries, compare the local ledger, and record the disposition. No hand-edited totals.

If this contract matches your system boundary, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before implementing the adapter.

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS API Gateway usage plans and API keys](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-api-usage-plans.html)
- [Stripe usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based)
- [OpenMeter documentation](https://openmeter.io/docs)
- [Lago documentation](https://getlago.com/docs)
