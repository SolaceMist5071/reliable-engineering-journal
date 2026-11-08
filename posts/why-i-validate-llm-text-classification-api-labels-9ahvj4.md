# Why I Validate LLM Text Classification API Labels — Structured JSON Before Dispatch

Require validated structured JSON labels before an LLM text classification API can dispatch a marketplace support ticket. Compare candidates on whether each response names an allowed label for the correct ticket; a successful request alone doesn't establish that.

Short answer: compare OpenAI, Claude, Gemini, and a gateway against the same labeled ticket sample, then admit only validated results into the dispatch queue. Start with a small model. Prefer batch processing for backfills that can wait, and keep customer-facing triage separate from that backlog.

The operational choice here is to delay dispatch when validation fails. That sacrifices some automation coverage in exchange for a contract the downstream worker can enforce. This is an implementation recommendation, not a report of a production incident or a measured provider ranking.

## What should wake the operator before a ticket gets misrouted?

A classification job can finish its request without producing a usable classification. Treating those outcomes as the same event creates a dangerous signal: a dashboard may count completed requests while the dispatch worker receives an unfamiliar label, a missing ticket identifier, or prose surrounding the JSON. Those are failure cases to test, not claims about any particular provider's behavior.

Consider an illustrative marketplace ticket, marketplace-1042: the buyer reports a late parcel and asks for a refund. The proposed labels are billing, delivery, account, and manual_review. Both billing and delivery sound plausible until the labeling policy specifies which team owns this overlap. JSON validation can reject an invented label such as refund, but it cannot decide whether billing or delivery is correct. Write that decision into the annotation policy before comparing models. Otherwise the benchmark rewards agreement with an unstated preference.

Keep those separate.

Monitor structural rejections, accepted labels that disagree with reviewed answers, and tickets awaiting a decision. Measure dispatch age from ticket arrival. Resetting its clock when a batch completes hides time the customer has already spent waiting.

## How should I compare LLM text classification APIs for structured JSON labels?

Use identical ticket text, label definitions, and expected JSON fields for every candidate. Request fixed labels through JSON schema style prompting. Verify the specific model's output controls before relying on them; the application validator remains the last admission check. A provider name alone doesn't establish that a chosen model satisfies your contract.

| Option | Reason to include it | Boundary to resolve before dispatch |
| --- | --- | --- |
| OpenAI directly | A direct-provider baseline for the classification contract | Test the selected model's response shape and label accuracy on reviewed tickets. |
| Claude directly | An independent candidate for ambiguous support language | Apply the same overlap policy and the same label vocabulary. |
| Gemini directly | Another candidate under the identical ticket contract | Check structural acceptance separately from agreement with reviewed labels. |
| LiteLLM | An open-source, self-hosted LLM gateway option | Choose it when operating a gateway is an acceptable responsibility; validate each model's output. |

The direct-provider candidates keep selection focused on the model and its integration. A self-hosted gateway adds an operating responsibility in exchange for control over that gateway. These are deployment choices, not an accuracy ranking: no measured results here establish a winner among the three model providers. A fair selection needs the reviewed sample and its acceptance criteria.

Infrai offers one key, one bill, and one REST API with no SDK required. For the marketplace scheduler, consolidated credentials and invoices reduce key sprawl and month-end reconciliation across backend services. Plain HTTP works from any language or runtime, so the Go batch preflight below needs only the standard library rather than a service-specific client dependency. The same interface covers 295 routes across 20 modules. Public, self-describing discovery lets an operator inspect the full request JSON Schema, response schema, and runnable examples without a key before connecting the batch worker. That removes guesswork about the submission contract during a backfill preflight; runnable examples in 10 languages include Go for the scheduler team.

The OpenAI-compatible surface is another integration advantage: existing OpenAI clients can use it by configuring the base URL and API key. For a ticket classifier already using that client interface, this avoids rewriting the request transport when evaluating a gateway. It does not establish that different models produce equally correct labels.

The trade-off is an additional service boundary. A shared gateway is not suitable when your operating policy requires a direct provider relationship; choose OpenAI, Claude, or Gemini directly in that case. If classification is the only service being selected, shared backend billing may carry little weight. A self-hosted gateway makes sense only when the team is willing to own it. Estimate prompt and completion spend before a large backfill, but choose among candidates that meet the correctness requirement first.

The contract wins.

## Put a small validator between classification and dispatch

Start with a read-only batch contract preflight. Save the following program as preflight.go in a separate directory from the validator. Set INFRAI_ORIGIN to the documented HTTPS API origin, without a path, through environment configuration; this example keeps that deployment address out of the source. Discovery is public, so no authorization header is necessary; if supplied, INFRAI_API_KEY is read from the environment. The program prints the declared submission contract for inspection. It does not submit a batch or classify a ticket.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    base := strings.TrimRight(os.Getenv("INFRAI_ORIGIN"), "/")
    if !strings.HasPrefix(base, "https://") {
        return fmt.Errorf("INFRAI_ORIGIN must be an HTTPS API origin")
    }
    client := &http.Client{
        Timeout: 30 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            base+"/v1/discovery/ai.batch.submit", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            } else if until, err := http.ParseTime(retry); err == nil {
                delay = time.Until(until)
                if delay < 0 {
                    delay = 0
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode != http.StatusOK {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Inspect before enabling.

This complete Go program reads one classification from standard input and takes the expected ticket identifier as its sole argument. Save it as main.go and pass marketplace-1042 as the argument for the illustrative ticket. It is an application-side guard, with no API request or provider-specific fields. Use Go 1.18 or later for the any type alias.

It requires ticket_id and label, rejects unknown fields and trailing JSON values, checks the ticket identity, and accepts only the four labels above. Missing fields and null values fail. It uses Go's standard JSON decoding behavior, including its handling of duplicate object keys; it does not detect those duplicates.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "os"
)

type Classification struct {
    TicketID *string `json:"ticket_id"`
    Label    *string `json:"label"`
}

func validate(r io.Reader, expected string) (Classification, error) {
    var c Classification
    d := json.NewDecoder(r)
    d.DisallowUnknownFields()
    if err := d.Decode(&c); err != nil {
        return c, fmt.Errorf("decode classification: %w", err)
    }
    var extra any
    if err := d.Decode(&extra); err != io.EOF {
        return c, fmt.Errorf("expected exactly one JSON value")
    }
    if c.TicketID == nil || *c.TicketID != expected {
        return c, fmt.Errorf("ticket identity mismatch")
    }
    if c.Label == nil {
        return c, fmt.Errorf("label is required")
    }
    switch *c.Label {
    case "billing", "delivery", "account", "manual_review":
        return c, nil
    default:
        return c, fmt.Errorf("label is outside the dispatch contract")
    }
}

func run() error {
    if len(os.Args) != 2 || os.Args[1] == "" {
        return fmt.Errorf("usage: validator EXPECTED_TICKET_ID")
    }
    c, err := validate(os.Stdin, os.Args[1])
    if err != nil {
        return err
    }
    return json.NewEncoder(os.Stdout).Encode(c)
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

An accepted object for the example would contain ticket_id equal to marketplace-1042 and label equal to delivery, if the annotation policy assigns that overlap to delivery. The program checks the contract, not that semantic judgment. Printing a valid object is not a durable acknowledgment.

For the application integration, persist the accepted decision and an outgoing dispatch record atomically. Enforce a unique dispatch identity derived from the ticket and the triage-policy version. Keep model-run identifiers as audit data rather than making every rerun a new dispatch identity. Otherwise a retry that produces the same answer can create a second support action. Consider a worker that records the new classification, sends a dispatch, and crashes before acknowledging its queue message: replay must find the existing dispatch identity, rather than treating the next model-run identifier as permission to contact the customer again. That is why identity belongs to the intended business action, even when model execution has to be repeated.

Assume duplicate delivery.

The consumer must recognize an already-applied dispatch identity before performing the business effect. For an external write, use that destination's documented idempotency mechanism where available; a local success flag alone cannot settle an ambiguous external outcome. On a rate-limited model request, honor Retry-After when present and use bounded exponential backoff. Keep the ticket pending while retrying. Never turn a parsing failure into a default delivery label just to drain the queue.

## Verify admission, then rehearse the rollback

Before enabling automatic dispatch, run the classifier with dispatch disabled against a reviewed sample containing ambiguous refund requests, missing information, and tickets that belong in manual_review. Count structural rejections separately from incorrect accepted labels. The latter satisfy the machine contract and can still send a customer to the wrong team.

Exercise the validator with a missing label, an unknown field, an unrelated ticket identifier, a second JSON value, and each allowed label. Then replay one accepted dispatch twice through the consumer. The required result is one business effect. Also test a timeout after the destination may have accepted the write; that is where the retry policy has to reconcile the existing action before attempting another one. Test duplicate JSON keys separately, because the small validator above is insufficient if your admission contract requires rejecting them. Add that check before enabling dispatch under such a contract.

Stop dispatch first.

Make the rollback switch disable new automated dispatches while preserving pending tickets and audit records. Restore the last approved model configuration and label policy together, then review decisions made by the rejected configuration. Do not blindly replay the whole backfill: first reconcile dispatch identities against completed actions, and route unresolved tickets to manual review. Switching models cannot reverse a support action that has already happened.

For this marketplace workflow, select the smallest candidate that passes the reviewed label contract, use batch processing for work that can wait, and keep a human path for unresolved tickets. A useful classifier produces an accountable routing decision. A completed request is only an intermediate event.

## References

- OpenAI documentation: https://platform.openai.com/docs/
- Claude documentation: https://docs.anthropic.com/en/docs/
- Gemini API documentation: https://ai.google.dev/gemini-api/docs
- LiteLLM, open-source LLM gateway: https://github.com/BerriAI/litellm
- Go encoding/json reference, including decoder behavior: https://pkg.go.dev/encoding/json
- HTTP Retry-After semantics: https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
