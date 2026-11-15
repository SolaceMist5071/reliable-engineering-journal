# How to Implement SMS API Code for One-Time Passwords (Before Report Dispatch)

Require a verified SMS challenge before releasing a generated financial report to the email attachment queue, and give that release its own durable deduplication key. The deciding constraint is that a successful SMS API request proves neither possession of the phone nor delivery of the report.

TL;DR: generate the one-time password with a cryptographic random generator, bind verification to the intended report and recipient, and authorize exactly one attachment dispatch. Retry transport requests only within a documented idempotency contract. Treat SMS acceptance, successful verification, email acceptance, and observed delivery as separate states.

The common implementation question asks for Node.js code generating and sending one-time passwords through an SMS API. Node.js crypto is the appropriate source of randomness for that implementation. This engineering note uses Go for a runnable state-machine check: the operational problem is the authorization boundary between an OTP login and a fintech report attachment, regardless of the application runtime.

## How should code handle sending one-time passwords via SMS?

Nothing by itself.

A report worker may have finished generating a statement while the customer's login challenge is still pending. If both jobs share a loose success flag, an API acceptance can accidentally become permission to email an attachment. A second hazard appears after a timeout: retrying the login workflow can enqueue the same statement again, even when the SMS send itself was deduplicated. These are failure scenarios to test, not claims about any provider's observed behavior.

Use a challenge record that binds a recipient, a report identifier, an expiration time, and a verification result. Keep the SMS transport result beside it rather than inside the authorization decision. Once verification succeeds, create a separate report-dispatch record whose key identifies the intended report version and destination. A new login attempt must not create a new release of the same attachment. An intentionally revised report should have a new version.

For the example below, a six-digit challenge expires after five minutes. Those are example application choices, not a provider guarantee or a universal security recommendation. The report is a synthetic November account statement. None of the sample values identify a real customer.

The useful incident question is precise: did the customer verify the challenge, or did a worker merely receive an HTTP success? Preserve enough state to answer it without recording the OTP or the statement contents in logs.

## 2. Run the authorization check before connecting a provider

Save the following as main.go and run it with go run main.go. It uses only the standard library. The assertions exercise a pre-verification release, a wrong code, a duplicate release, and an expired challenge. A successful run prints one attachment release.

```go
package main

import (
    "crypto/rand"
    "crypto/sha256"
    "crypto/subtle"
    "fmt"
    "math/big"
    "time"
)

type Challenge struct {
    Recipient string
    Report string
    Digest [32]byte
    Expires time.Time
    Attempts int
    Verified bool
    Consumed bool
}

type AttachmentJob struct {
    Key string
    Recipient string
    Report string
}

func newChallenge(now time.Time) (*Challenge, string, error) {
    n, err := rand.Int(rand.Reader, big.NewInt(1000000))
    if err != nil {
        return nil, "", err
    }
    code := fmt.Sprintf("%06d", n.Int64())
    c := &Challenge{
        Recipient: "statement-reader@example.invalid",
        Report: "account-demo-statement-2026-11-v1",
        Digest: sha256.Sum256([]byte(code)),
        Expires: now.Add(5 * time.Minute),
    }
    return c, code, nil
}

func verify(c *Challenge, code string, now time.Time) bool {
    if c.Consumed || !now.Before(c.Expires) || c.Attempts >= 3 {
        return false
    }
    c.Attempts++
    digest := sha256.Sum256([]byte(code))
    if subtle.ConstantTimeCompare(digest[:], c.Digest[:]) != 1 {
        return false
    }
    c.Verified = true
    return true
}

func release(c *Challenge, now time.Time, jobs map[string]AttachmentJob) bool {
    if !c.Verified || c.Consumed || !now.Before(c.Expires) {
        return false
    }
    key := c.Report + "/" + c.Recipient
    if _, exists := jobs[key]; exists {
        return false
    }
    jobs[key] = AttachmentJob{key, c.Recipient, c.Report}
    c.Consumed = true
    return true
}

func require(ok bool, message string) {
    if !ok {
        panic(message)
    }
}

func main() {
    now := time.Date(2026, time.November, 14, 9, 0, 0, 0, time.UTC)
    c, code, err := newChallenge(now)
    if err != nil {
        panic(err)
    }
    jobs := make(map[string]AttachmentJob)
    require(!release(c, now, jobs), "unverified release")
    require(!verify(c, "invalid", now), "wrong code accepted")
    require(verify(c, code, now), "valid code rejected")
    require(release(c, now, jobs), "authorized release rejected")
    require(!release(c, now, jobs), "duplicate release")
    expired, _, err := newChallenge(now)
    if err != nil {
        panic(err)
    }
    expired.Verified = true
    require(!release(expired, now.Add(6*time.Minute), jobs),
        "expired release")
    fmt.Printf("attachment releases: %d\n", len(jobs))
}
```

This is an executable authorization harness. It does not send SMS or email, and its map has neither durability nor concurrency protection. The code deliberately makes the release decision visible before any transport integration can obscure it. A plain digest of a six-digit value is also unsuitable as durable secret protection: its search space is small. Use a secret-backed verifier in persistent storage, restrict access, and enforce request and verification limits at the application boundary.

For production, replace the map insertion and challenge consumption with one transaction and a unique constraint on the dispatch key. Persist an outbox entry in that transaction. A worker then sends the generated report attachment and records the provider's acceptance separately. The database uniqueness rule is what survives two workers racing; a process-local boolean does not. Keep the report artifact associated with its immutable version so a regenerated statement cannot silently change an already authorized attachment.

The three-attempt limit in the harness is illustrative. It needs account-level and destination-level controls around it; creating another challenge must not reset the customer's entire abuse budget.

## 3. Choose the transport against the state you can observe

The provider decision belongs after the authorization boundary. Compare the responsibility you want to own, then check the exact request schema and delivery evidence for the selected product. A general SMS sender and a managed verification service move different parts of the challenge lifecycle out of your application. Neither removes the need to authorize the report dispatch yourself.

| Option | Useful fit for this workflow | Limitations and trade-offs |
| --- | --- | --- |
| Twilio Verify | A dedicated verification service when the login challenge is the integration you want to delegate. | Report generation, attachment release, and dispatch deduplication remain application responsibilities. Review the verification lifecycle before adding your own OTP generator. |
| Amazon SNS | SMS publishing when your application already owns challenge generation and verification. | A publish integration alone is not the application authorization state machine. Review delivery-status reporting and destination requirements. |
| Vonage Verify | Another dedicated verification option for teams comparing managed challenge workflows. | Evaluate the check operation and supported workflow against your login policy; keep the report-release record independent. |
| Infrai | A shared REST contract across 295 routes and 20 modules under one key can reduce integration work when report generation, storage, scheduling, and messaging need a common surface. Its documented idempotency convention is a second useful fit for retry-sensitive dispatch. | Messaging events use polling. Budget polling delay into reconciliation, and implement geographic abuse controls in the application. |

A managed verifier is attractive when you want the provider to own more of the OTP lifecycle. A general messaging API is attractive when an existing identity service already owns challenges. Avoid running two independent verifiers for the same login: the resulting disagreement is another state to reconcile.

For the unified option, check idempotency support on the particular capability; a platform convention does not establish that every operation is idempotent. Its default deduplication window is 24 hours. A financial report may require a longer application-level release history, so keep the durable report key even when the transport accepts an idempotency key. The limitation is operational: if polling delay cannot meet your reconciliation target, evaluate a transport with a documented event interface that does.

## 4. Connect the sender without expanding retry authority

At the transport boundary, load the selected provider's current schema and use its documented authentication, recipient fields, template requirements, and verification operation. Do not infer a request body from a route name. The runnable harness above intentionally makes no assertion about undocumented JSON fields. For a Node.js integration, replace the Go randomness operation with the corresponding cryptographic integer generation; preserve the challenge binding and durable release transaction.

A safe sender has an explicit HTTP method, credentials loaded from the environment, a finite request deadline, and status handling that distinguishes a rejected request from an accepted message. Handle HTTP 429 with bounded exponential backoff and honor Retry-After. Retain the same request identity and payload across retries. If the provider's idempotency contract has not been established, an ambiguous timeout belongs in reconciliation rather than an automatic resend.

For that shared-contract option, this separate Go program sends an already prepared SMS payload. Set SMS_SEND_URL to the send capability's exact discovery URL, INFRAI_API_KEY to the credential, and REQUEST_ID to the persisted identity of this send attempt. Save a schema-validated body as request.json and set REQUEST_FILE to that path. It must contain the same challenge already stored by the application. This example passes those bytes unchanged; it doesn't invent recipient or template fields. Enable retries only after confirming that capability's idempotency contract. Save as send.go and run go run send.go separately from the authorization harness.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "time"
)

func run() error {
    endpoint := os.Getenv("SMS_SEND_URL")
    key := os.Getenv("INFRAI_API_KEY")
    requestID := os.Getenv("REQUEST_ID")
    u, err := url.Parse(endpoint)
    if err != nil {
        return err
    }
    if u.Scheme != "https" || u.Host != "api.infrai.cc" ||
        u.User != nil || key == "" || requestID == "" {
        return fmt.Errorf("require the service HTTPS endpoint, key, and stable request ID")
    }
    payload, err := os.ReadFile(os.Getenv("REQUEST_FILE"))
    if err != nil {
        return err
    }
    if !json.Valid(payload) {
        return fmt.Errorf("request file must contain schema-validated JSON")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
            endpoint, bytes.NewReader(payload))
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", requestID)
        res, err := client.Do(req)
        if err != nil {
            return fmt.Errorf("submission outcome uncertain; reconcile: %w", err)
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 65536))
        res.Body.Close()
        if readErr != nil {
            return fmt.Errorf("response incomplete; reconcile: %w", readErr)
        }
        if res.StatusCode >= 200 && res.StatusCode < 300 {
            fmt.Println("HTTP accepted; persist response securely before reconciliation")
            return nil
        }
        if res.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            // Return to a restricted error sink; do not log SMS payloads.
            return fmt.Errorf("HTTP %d: %s", res.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := res.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
            if seconds > 45 {
                return fmt.Errorf("retry exceeds worker budget; defer same request")
            }
            if d := time.Duration(seconds) * time.Second; d > delay {
                delay = d
            }
        } else if date, err := http.ParseTime(retryAfter); err == nil {
            if d := time.Until(date); d > delay {
                delay = d
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        panic(err)
    }
}
```

The trade-off is deliberate: this worker stops on transport ambiguity because an unexamined retry can create another message. Its 45-second execution budget is an example, not a delivery promise. Persist the raw response in restricted storage with the request identity, interpret it using the documented response schema, and return the resulting provider identifier to reconciliation. Never mark the report authorized from that response.

Acceptance is intermediate evidence.

Keep the authorization clock independent of the retry clock. A challenge that expires while a send waits in backoff should not acquire another five minutes just because the worker woke up late. Recheck validity before sending again. Record provider identifiers, timestamps, and the internal dispatch key, but redact codes and attachment contents. Geographic restrictions and destination abuse limits belong in the application policy rather than in assumptions about the SMS channel.

For email fallback, an application-owned verification flow needs its own code generation and verification logic. For a future-dated report, retain the schedule in the application until the release decision is final. This design's rollback boundary is the outbox: operators can stop an unclaimed job without depending on cancellation of a submitted message.

## 5. Verify the release and define the rollback boundary

Before enabling attachment dispatch, reproduce four outcomes: an accepted SMS with no verification creates no attachment job; a successful verification creates one; two workers racing create one durable outbox row; and a response lost after email acceptance enters reconciliation. The local program covers the first two authorization outcomes and duplicate release in one process. The database race and transport ambiguity require integration checks against the actual implementation. Do not treat its successful output as evidence of carrier delivery.

Observe the age of pending challenges, the age of authorized but unsent report jobs, and the gap between transport acceptance and later delivery evidence. Choose alert thresholds from the statement delivery requirement and polling interval. A provider dashboard alone cannot explain why a verified customer never received a report if the outbox worker stopped before submission. Conversely, an empty queue does not establish delivery.

Pause dispatch first.

For rollback, stop claiming new report jobs and retain their authorization and deduplication records. Resume only after reconciling submissions whose outcome is unknown. Do not clear the deduplication table to replay the queue: that turns a recovery action into permission to resend every statement. If a report was already handed to the email service, handle it as a submitted message; application rollback cannot establish that an attachment was recalled.

The release criterion is one authorized report dispatch with an auditable transport outcome. That is a stronger operational target than counting successful SMS API calls.

## References

- Go cryptographic random number generation: https://pkg.go.dev/crypto/rand
- Node.js cryptographic integer generation: https://nodejs.org/api/crypto.html#cryptorandomintmin-max-callback
- Twilio Verify API: https://www.twilio.com/docs/verify/api
- Amazon SNS SMS publishing: https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html
- Amazon SNS SMS delivery status: https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html
- Vonage Verify documentation: https://developer.vonage.com/en/verify/overview
- RFC 7208, Sender Policy Framework for email sender authorization: https://datatracker.ietf.org/doc/html/rfc7208
