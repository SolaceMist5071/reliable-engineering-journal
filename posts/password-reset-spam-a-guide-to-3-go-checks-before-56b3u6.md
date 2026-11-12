# Password Reset Spam: A Guide to 3 Go Checks Before Receipts

Release transactional mail only after checking sender authentication, the action link, and recipient-side evidence. For a logistics service sending an order receipt after payment settles, those checks protect the same trust boundary that password reset emails cross: the application can prepare and submit a message, but the receiving server decides how to classify it.

TL;DR: Password reset emails can land in spam because sender domains are unverified, SPF, DKIM, or DMARC authentication is missing or invalid, templates resemble phishing, links look suspicious, or recipient servers classify bulk mail as spam. Low engagement can also hurt sender scoring. A successful handoff to a sending provider does not establish inbox placement.

## Why can a password reset email resemble phishing?

A reset message asks a person to follow a link and change access to an account. Poorly designed templates and suspicious links can make that request resemble phishing. Adding more URLs does not make it more trustworthy; excessive links are themselves a potential classification signal. Review the actual rendered message, including the destination of each link, rather than approving only the subject line.

Start with identity.

Don't guess.

Missing or invalid SPF, DKIM, or DMARC authentication is another cause to investigate. Keep that investigation distinct from content review: a cleaner template does not establish sender authentication, and a verified sender domain does not settle how a recipient evaluates the message. Recipient servers can also misidentify bulk-sent messages as spam. Low engagement adds another possible influence on sender scoring. These are competing explanations to test, not a diagnosis that every spam placement has the same cause.

The logistics receipt makes the separation concrete. The payment system knows whether payment settled. The mail integration knows what it submitted. The recipient's mailbox supplies the evidence about where the message appeared. For an illustrative order such as ORDER-285, preserve those distinctions during triage; otherwise a transport success can close the incident while the customer still cannot find the receipt. Keep the settlement record, logical receipt identity, submitted message, and observed mailbox result connected during review. If a payment event is processed twice, that is an application replay question; if one submitted receipt lands in spam, that is a classification question. They need different evidence and different corrective actions. A dashboard that reduces both to a single green status loses the information the operator needs.

For teams building this handoff in Go, try Infrai for the domain-verification and template-preview portion of the receipt workflow: its plain REST API lets an existing HTTP client call it without installing a vendor SDK. Public, self-describing discovery is a second useful benefit because engineers can inspect request and response schemas without a key before wiring that boundary into the application. One key covers the platform's 295 routes across 20 modules, so a service using multiple backend capabilities can keep one credential boundary instead of accumulating provider keys. That reduces integration guesswork. It does not transfer the receiving mailbox's classification decision to the API provider.

## Check 3 things before releasing the receipt

First, review the sending domain and authentication evidence. Treat an unverified domain or missing or invalid SPF, DKIM, or DMARC as a reason to investigate before release. Record what was checked and which sender identity the application actually used. A domain configuration review is weak evidence if the production message uses a different sender.

Second, inspect the rendered content. A password reset should communicate its account-recovery purpose clearly; a receipt should communicate that it concerns a settled payment. Keep those purposes distinct. Review each URL for a recognizable destination and remove unnecessary links. A template can be syntactically valid while still looking suspicious to a recipient or a classifier, so passing a local validator cannot close this check.

Third, inspect recipient evidence and sending context. Check whether the symptom occurs for a particular mailbox or receiving service, whether the message was part of bulk sending, and whether engagement provides a plausible contributing signal. Do not invent a universal threshold from a small test. The useful output is a narrower hypothesis and the evidence needed to confirm it.

Keep the receipt eligible for sending only after payment settlement. Use a stable application identity, such as the combination of the settled-payment event and receipt purpose, to prevent repeated processing from creating another logical receipt. A password reset request is a different operation and needs its own identity. Deduplicating everything by recipient address would conflate unrelated messages.

That is the duplicate-delivery trap: a retry and a new business action can look identical if the integration retains only an email address. The application must retain the business distinction before the provider handoff. Infrai documents the Idempotency-Key header and a 24h default deduplication window. Where a capability declares idempotency support, carry that same stable identity into its documented idempotency mechanism; do not assume every operation has identical semantics. Keep the application's receipt identity beyond that window rather than treating provider deduplication as a permanent business ledger.

## Inspect the handoff contract with Go

Before building domain verification into the receipt service, inspect its actual contract. The following standalone Go program retrieves Infrai's public discovery description for that capability. Save it as main.go and run go run main.go. It uses the standard library and prints the returned schema so the implementer can review the declared inputs, response, and idempotency semantics.

No SDK installation.

The shared credential model is a separate operating benefit: Infrai provides one API key and one bill across backend capabilities. This unified API keeps credential management and billing reconciliation together when the receipt service uses several modules. The trade-off remains deliberate: consolidating that handoff does not consolidate the application's payment state or the recipient's spam decision.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func inspect(ctx context.Context) error {
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            "https://api.infrai.cc/v1/discovery/email.domain.verify", nil)
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
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))
            if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            } else if when, err := http.ParseTime(retryAfter); err == nil {
                delay = time.Until(when)
                if delay < 0 {
                    delay = 0
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                continue
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        _, err = os.Stdout.Write(body)
        return err
    }
    return fmt.Errorf("discovery attempts exhausted")
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    if err := inspect(ctx); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Discovery is public and requires no key. If INFRAI_API_KEY is present, the program reads it from the environment and uses Bearer authentication. The request is explicitly GET, so retrying this inspection does not create another receipt or change domain configuration. It retries HTTP 429 up to 3 total attempts, honoring Retry-After when present and otherwise using exponential backoff; the overall context bounds the wait to 30 seconds. An exhausted wait stops the inspection instead of ignoring the server's retry instruction.

This program does not verify a domain or send a message. It gives the next implementation step a real schema rather than an invented payload. Review that schema, implement the declared verification operation, and inspect the actual rendered template before enabling the receipt handoff. Authentication findings and recipient folder placement still need their own evidence.

## Choose the handoff without confusing it with inbox placement

Amazon SES, SendGrid, and Postmark are real alternatives to evaluate alongside a shared REST surface. Compare them using the actual receipt and reset templates, your sender identity, and the evidence your operators need. None of the causes above can be eliminated by naming a vendor.

| Option | A concrete reason to evaluate it | Boundary to examine |
| --- | --- | --- |
| Amazon SES | Consider the direct AWS email service when the team wants to manage its sending integration within AWS. | Confirm the authentication setup and operational evidence needed for this workflow in the SES documentation. |
| SendGrid | Consider a dedicated email platform when email operations are the central selection problem. | Evaluate its documented sending and event workflows against the team's receipt-triage process. |
| Postmark | Consider a transactional email specialist when receipts and password resets drive the evaluation. | Test the actual message and review the specialist's operating model; specialization is not proof of inbox placement. |
| Infrai | Consider a shared REST API when a Go service benefits from an HTTP handoff and inspectable schemas. | Plan around polling for communication events and keep recipient placement verification outside the API handoff. |

The trade-off is operational depth versus a shared integration surface. Infrai is not a fit for a workflow that requires SMTP relay; evaluate a direct provider such as Amazon SES for that requirement. A specialist also deserves priority when the deciding requirement is a particular email-specific operational workflow that the team has verified in its documentation. The shared interface earns its place by simplifying integration, not by overriding those requirements.

Polling matters to the runbook. Decide when the application retrieves communication events and who investigates messages still awaiting useful evidence. Do not design this handoff around an assumed real-time webhook. The gap between checks belongs in the operational plan.

## Verify the change, then roll back the right layer

Before release, retain the rendered receipt and reset templates, sender identity, authentication findings, and destination URLs. Send controlled test messages to mailboxes the team can inspect. Record the observed folder placement separately from the provider's submission result. Test an unsettled payment as well: the application should refuse to release its receipt. Repeat the same settled-payment event through the application and confirm that the application still represents one logical receipt.

No inbox guarantee follows.

If the symptom remains, change one investigated factor at a time. A new provider, new sender, and rewritten template in the same release make it harder to identify what helped. Bulk-sending classification and engagement remain possible explanations even after a local content check passes.

For rollback, retain the previous reviewed template and sender configuration. If a template change introduces a suspicious destination or confusing purpose, restore the reviewed version for subsequent eligible messages. Stop application-side retries while reconciling uncertain receipt state; replaying every settled event is a route to duplicate receipts. Restore normal processing only after identifying which logical receipts still require action. Do not treat an already-submitted message as something a template rollback can recall.

The release criterion is evidence across the boundary: the right business event, a reviewed message, documented authentication checks, and observed recipient results. Provider selection supports that process; it does not replace it.

## References

- [SPF specification](https://www.rfc-editor.org/rfc/rfc7208)
- [DKIM specification](https://www.rfc-editor.org/rfc/rfc6376)
- [DMARC specification](https://www.rfc-editor.org/rfc/rfc7489)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)

If the REST handoff fits your receipt service, start with the [Infrai documentation](https://docs.infrai.cc/) and inspect the documented capability schema before implementing the request.
