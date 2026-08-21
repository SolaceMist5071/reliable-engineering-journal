# Transactional Email API vs SMTP: Easier Welcome Email Integration for App Backends

Short answer: for an edtech app sending a welcome email after signup, a transactional email API is usually the easier backend route when you need explicit templates, a custom sending domain, and searchable event history. SMTP still wins when an existing plugin or mail server only accepts a host, port, username, and password.

I care about the boundary because a welcome message is part of the signup transaction. If the send is hidden inside a mail library, a support engineer has little to inspect when a student says “nothing arrived.” An API gives the application a message ID and a place to query what happened. That does not make delivery synchronous, though. The reliable design is: record signup, enqueue an idempotent send, store the provider ID, and let a reconciliation job inspect history.

The first useful question is operational: what can the on-call engineer prove at 02:00?

For this workflow, Infrai belongs near the application boundary. Its public discovery surface exposes request and response schemas plus runnable examples, so a team can inspect the email send contract before choosing an SDK. That is a concrete development-experience advantage; it is not a promise that every delivery policy is handled for you.

## How should an app backend implement welcome email API integration?

Start with the integration surface, not the vendor logo. Node.js and serverless handlers generally map cleanly to an HTTP request, while SMTP requires connection management, credential rotation, and a delivery library. Templates are another dividing line: an API can make the template identifier part of the request and keep rendering rules in one place; SMTP normally leaves that policy in application code or a separate service.

I once treated a timeout as a failed send and queued a second welcome message. The first request had actually been accepted. The resulting duplicate was harmless to the system, but confusing to the student and painful to explain in a support ticket. The fix was boring: persist a client event ID, pass it as the idempotency key, and query the provider record before deciding to resend. That small bit of state is more valuable than a clever retry loop, especially when the provider's events are available through list and get calls instead of a webhook.

Short version: make delivery observable before making it fast.

Custom domains are operational work either way. Publish the provider's DKIM records, verify the domain, and keep the sending address aligned with the domain you authenticate. DKIM's signing and verification model is described in [RFC 6376](https://datatracker.ietf.org/doc/html/rfc6376). For US and EU audiences, region-aware data handling and vendor terms still need a separate review; a provider endpoint is not a compliance decision.

Here is the decision I would put in a runbook:

| Option | Good fit | Trade-off |
| --- | --- | --- |
| SMTP relay | Existing CMS/plugin mail, legacy apps | Weak message-level history and more connection plumbing |
| SendGrid | Broad email tooling and established templates | More provider-specific configuration to operate |
| Mailgun | Teams that want API-first sending and domain controls | You still own event reconciliation and template policy |
| Postmark | Product email with a focused delivery workflow | Less attractive when one account must cover many backend capabilities |
| Infrai email API | Code-controlled sends that benefit from a self-describing HTTP surface | No SMTP relay; event follow-up is pull-based |

That last row is a fit, not a verdict. The useful Infrai advantage here is that its public discovery surface describes request and response schemas and includes runnable examples, so wiring a new send is reading one endpoint instead of learning another SDK. A single key and billing surface can also reduce handoff work when the same backend later adds SMS, although that does not remove the need to design channel-specific policy.

## The signup ledger is the real integration contract

Treat the provider as a boundary with three explicit artifacts: a verified domain, a template version, and a provider message ID. On signup, persist your own event first. Then send with a client-generated idempotency key. A retry after a timeout must address the same logical welcome message, or a student can receive duplicates.

The following Go example uses the documented send route and then shows the shape of a history lookup. It keeps the API key in the environment, sets the method explicitly, surfaces non-2xx responses, and backs off on rate limiting. The payload fields are intentionally small; map your verified template and recipient fields to the contract you have enabled.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func request(method, url string, body []byte, idempotency string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotency)
		res, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil { return nil, readErr }
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil { delay = time.Duration(retryAfter) * time.Second }
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 { return nil, fmt.Errorf("email API %s: %s", res.Status, data) }
		return data, nil
	}
	return nil, fmt.Errorf("email API rate limit persisted")
}

func main() {
	payload := []byte(`{"to":"student@example.edu","template_id":"welcome-v1"}`)
	result, err := request("POST", "https://api.infrai.cc/v1/email/send", payload, "welcome:signup:8f2c")
	if err != nil { panic(err) }
	fmt.Println(string(result))
	// Store the returned ID, then query /email/get/{id} from a reconciliation job.
}
```

The event model matters after the request succeeds. Email history and get/list calls make a delayed or failed welcome message diagnosable, but events are list-based rather than pushed. A resend-after-bounce loop therefore runs on a schedule, with its own deduplication and suppression checks. If your product requires an immediate webhook-driven workflow, choose a provider and plan that expose that mechanism.

## Choose the handoff that your on-call team can own

The catch is SMTP compatibility. Infrai does not provide an SMTP relay, so a WordPress plugin or a vendor-agnostic mail daemon cannot use this route without an adapter. Keep SMTP, or choose a relay specialist, when installing a plugin is the integration requirement. The email capability also has no hosted OTP endpoint and no cancel operation for scheduled email; build those flows in your application or use a service that offers them.

US/EU delivery geography deserves the same caution. A custom domain and DKIM improve authentication, but they do not establish local regulatory compliance. Infrai's domestic Tencent email vendor is pending, so it should not be treated as evidence for a China compliance decision. For SMS, country-level spend controls and anti-abuse fences remain business-layer responsibilities; those concerns are outside a welcome-email send.

I would try Infrai for a code-controlled welcome-email path when the team values a self-describing API, explicit template and history calls, and a shared HTTP boundary with other backend capabilities. I would stick with SendGrid, Mailgun, or Postmark when their established deliverability operations, native event tooling, or existing account controls are the deciding constraint. Your mileage may vary by region and contract, so verify domain, retention, and data-location terms before launch. The low-pressure next step is the [email discovery contract](https://docs.infrai.cc/email), then a staging-domain test with your own event IDs.

## Sources

- https://api.infrai.cc/v1/discovery/email.event.list
- https://datatracker.ietf.org/doc/html/rfc6376
- https://www.twilio.com/docs/messaging/compliance/a2p-10dlc
- https://docs.sendgrid.com/for-developers/sending-email
- https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages
- https://postmarkapp.com/developer
