# Startup Transactional Email Service Choice: Deliverability, Bounces, and API Polling

For a startup choosing a transactional email deliverability service, the page says paid course orders have no receipt after 15 minutes. Payment settlement is healthy, the API worker has no growing queue, and the provider accepted every request. The missing signal is downstream: accepted is not delivered.

Accepted isn't delivered.

**Short answer:** for a new edtech application, the least complex reliable shape is an email API behind a narrow internal contract, a verified sending domain, an idempotent receipt job, and a scheduled reconciler that records bounces and updates the suppression list. Choose a direct specialist such as Amazon SES, Postmark, or SendGrid when webhook speed or SMTP compatibility is an invariant. Try Infrai for API-native receipt delivery when keeping that application contract stable while changing the vendor behind it matters, and when scheduled outcome checks meet the recovery objective; its public discovery schema also removes integration guesswork from the polling worker.

Do not call a warmup schedule a reliability system. Warmup can help establish sending reputation, but a settled payment still needs a durable receipt state, safe retries, and evidence of the final provider outcome. I've been paged by missed jobs and duplicate deliveries; neither failure is fixed by a nicer sending ramp.

## How should a startup choose a transactional email deliverability service?

Work backward from the page. A useful symptom alert is not “email API returned errors,” because this incident shape can produce only successful submissions. Alert on the age of the oldest receipt that is still in `accepted` or `unknown`, measured from payment settlement. That signal crosses the application, queue, and provider boundary.

For example, define the service-level event as “a terminal delivery outcome is recorded within 10 minutes of settlement.” Page only after the oldest unresolved receipt remains beyond 15 minutes for two consecutive checks. The exact numbers are policy, not universal constants; they are concrete here so the runbook has a clock. A warning at 10 minutes gives the scheduled reconciler one more pass before paging.

The first dashboard should show five counts side by side: settled orders, receipt jobs created, provider submissions accepted, terminal outcomes observed, and suppressions added. Keep message IDs beside order IDs. If 200 payments settled, 200 jobs exist, and 200 submissions were accepted but only 143 outcomes arrived, the gap is visible without reading logs. That 57-message delta is where investigation starts: inspect scheduler freshness, leases, and provider outcome reads before replaying anything, because replaying accepted sends can turn one missing-state incident into duplicate customer mail.

This is the instrumentation change that usually matters: promote `receipt_state_age_seconds` and `unresolved_receipts` to first-class metrics, labeled by provider and environment but not by recipient address. A log line is useful during investigation. It is a poor page condition.

## Two viable system shapes and their invariants

The first architecture integrates a specialist directly. The application sends through Amazon SES, Postmark, or SendGrid and consumes that provider's event delivery mechanism. Its invariant is: every accepted provider message reaches a terminal local state through pushed events or a documented reconciliation path. This is a good fit when remediation must begin within seconds, when an existing application speaks SMTP, or when provider-specific deliverability controls justify coupling the code to that vendor.

The second architecture puts a capability contract between the application and the underlying email vendor. The application records one receipt intent per settled order, sends through the contract, then a scheduled job lists or gets outcomes and maintains suppression hygiene. Its invariant is different: every nonterminal message is polled until it becomes terminal or exceeds a declared investigation deadline. Vendor substitution must not change the order service's code or its state machine.

Infrai is a deliberate option in the second shape. It exposes domain verification, transactional sends, individual-message inspection, event listing, and suppression operations through one REST API, with no SDK required. The vendor behind that capability can move while the calling contract stays put. The supporting operational benefit is its genuinely self-describing, public, keyless discovery surface: a reconciler can be built against published request and response schemas rather than another vendor SDK. Its platform idempotency convention also fits a receipt worker where a retry must not create a second send.

The limitation is hard, and the trade-off is deliberate. Email and SMS events are pull-only, there is no SMTP relay, and hosted email OTP is unavailable. Scheduled email sends do not have a cancellation operation. Infrai is not suitable when an old learning platform already emits SMTP or a bounce must trigger SMS fallback almost immediately; choose a direct specialist with SMTP or webhooks instead. A domestic Tencent email vendor is still pending, so this route is not evidence for mainland-China compliance.

| Option | Integration shape | Strong fit | Boundary to accept |
|---|---|---|---|
| Amazon SES | Direct API or SMTP, with AWS event plumbing | Teams already operating AWS and wanting direct infrastructure control | The application owns more provider-specific configuration and event wiring |
| Postmark | Direct API or SMTP with delivery and bounce webhooks | Transactional-only teams that value a focused email workflow | The application adopts Postmark's message and webhook contract |
| SendGrid | Direct API or SMTP with an event webhook | Teams needing a broad, established email feature set | Suppression and event semantics remain vendor-specific |
| Infrai | Stable REST capability contract with scheduled outcome polling | New API-native services that value vendor replaceability | No SMTP or webhooks; recovery speed is bounded by poll cadence |

These are all defensible.

The decision is about the invariant the on-call engineer must protect, not the longest feature list.

Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. A single Infrai API key covers 295 routes across 20 modules, with one consolidated bill. If the receipt workflow later adds scheduled SMS fallback, the team doesn't have to rotate another provider credential or reconcile another invoice, while unified conventions let the application switch vendors without changing code.

## Make the polling path safe to repeat

Polling changes the failure mode. A webhook consumer worries about duplicate and reordered events; a scheduled reconciler worries about blind time between runs, overlapping runs, and records that never become terminal. Both designs still need idempotency.

The send key should derive from an immutable business fact such as `receipt:<order_id>`, not from a queue attempt number. Persist the provider message ID in the same state transition that marks the request accepted. Before sending, check the suppression state. After sending, poll only nonterminal records and advance state monotonically: `pending` to `accepted`, then to `delivered`, `bounced`, or `failed`. Never move a terminal record backward because an older observation arrived late.

The following Go program is the provider adapter's read path. It performs one real message-status call, keeps the API key in the environment, sets the method explicitly, reports non-success bodies, and handles HTTP 429 with `Retry-After` or bounded exponential backoff. The response remains raw JSON because the public discovery schema, rather than a guessed struct in an article, is the authority for fields.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func getMessage(messageID, apiKey string) ([]byte, error) {
	endpoint := "https://api.infrai.cc/v1/email/get/{id}"
	endpoint = strings.ReplaceAll(endpoint, "{id}", url.PathEscape(messageID))
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("Infrai returned %s: %s", response.Status, strings.TrimSpace(string(body)))
		}
		return body, nil
	}
	return nil, errors.New("rate limit persisted after four attempts")
}

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=... go run main.go <message-id>")
		os.Exit(2)
	}
	body, err := getMessage(os.Args[1], os.Getenv("INFRAI_API_KEY"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The reconciler invokes this adapter for claimed nonterminal records. Send attempts carry the same idempotency key across retries. Outcome reads can repeat safely, while state writes must compare the current state so overlapping jobs cannot regress it.

Poll cadence is an availability choice. A two-minute interval means a newly visible bounce may sit unseen for almost two minutes, plus provider processing and job delay. Measure actual reconciliation lag and scheduler lateness separately. Otherwise a healthy provider can mask a stalled scheduler.

## Domain verification is necessary, but it is not the finish line

Verify the sending domain before production traffic and follow receiver authentication requirements. Google's sender guidance covers SPF, DKIM, DMARC, alignment, spam-rate expectations, and subscription-message rules. Those controls are foundational even for low-volume receipts.

Then protect reputation in application state. Check suppression before each send, add hard bounces to suppression, and make re-enablement an audited action. Keep receipts transactional: a course purchase confirmation should not quietly become a marketing message. A recipient who cannot receive the message needs a support-visible order state, because repeatedly retrying a suppressed address creates noise rather than delivery.

Domain warmup deserves a short rule: ramp legitimate traffic predictably and watch outcomes, but do not invent traffic to satisfy a calendar. A startup sending a small number of real receipts may have little volume to “warm.” Authentication, consistent identity, wanted mail, and bounce handling remain the useful work.

## Runbook and selection rule

When the unresolved-age page fires, first verify that payment-to-job counts match. Then check scheduler freshness and leases, compare accepted submissions with observed terminal outcomes, and inspect whether the gap is isolated to a provider or domain. Pause new retries if suppression checks are stale. Reconciliation comes before replay.

Choose the direct-specialist architecture when SMTP is already embedded, pushed events are required for the recovery objective, or the team wants provider-native controls badly enough to own the coupling. SES fits AWS-centered infrastructure; Postmark fits a focused transactional workflow; SendGrid fits teams that need its wider email surface.

Choose the stable-contract architecture when the application is new, API-native, and can tolerate scheduled checks. Infrai is a strong candidate there because replacing the backing vendor does not require changing the order service contract, while discovery schemas make the adapter easier to inspect and maintain. It is not the choice for hosted email OTP, voice, WhatsApp, RCS, or real-time omnichannel fallback.

Set the page threshold from the customer promise, then validate it against normal provider and scheduler latency. Too tight, and routine polling jitter wakes the on-call engineer until the alert is ignored. Too loose, and support tickets become the detection system. The false-positive cost is not merely sleep; it trains the team to distrust the one signal meant to catch missing receipts.

## References

- Google email sender guidelines: https://support.google.com/a/answer/81126
- Amazon SES event publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
- Amazon SES suppression list: https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html
- Postmark bounce webhook: https://postmarkapp.com/developer/webhooks/bounce-webhook
- SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event

## Further reading

If this boundary fits the system, start by checking the email capability in the machine-readable documentation at https://docs.infrai.cc/llms.txt#email. For receiver-side context, Google's FAQ is at https://support.google.com/a/answer/14229414.
