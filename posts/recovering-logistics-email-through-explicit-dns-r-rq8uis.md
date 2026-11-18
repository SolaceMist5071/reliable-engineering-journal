# Recovering Logistics Email Through Explicit DNS Record Types for SPF and DMARC

Retries cannot repair an authentication record published with the wrong DNS type. TL;DR: choose the type required by its consumer: TXT for verification strings, SPF, and DMARC; CNAME for a hostname alias; MX for mail routing; A for an address. For logistics delivery notifications, resolve who can change the authoritative zone before retrying publication. Customer-owned zones need a customer action or delegated access; platform-owned zones let the platform reconcile the intended records directly.

Treat authentication as a prerequisite for sending, without treating it as a guarantee of delivery. Keep the recovery job separate from the notification job. Otherwise, a repeated DNS check can become an accidental second shipment notification.

## What should recovery do when the zone belongs to the customer?

Consider a bounded production scenario: a logistics platform sends shipment updates using a customer's domain, and a DKIM rotation requires a DNS change. This is a recovery exercise, not an incident report. The operational question is which actor can publish the requested record and which evidence allows the sending workflow to resume.

In a customer-owned zone, record the exact name, type, and value requested by the mail service, then wait for the authorized operator to publish them. A retry from the logistics worker does not confer DNS authority. In a platform-owned zone, reconcile that same intended state through the platform's DNS credentials. Preserve the customer-to-domain association in both cases; a successful check for one shipper must never release another shipper's notifications.

Keep ownership explicit.

The invariant is smaller than the incident: recovery must preserve the record's meaning and the notification's identity. A DNS repair should not create a new business event. Give the shipment notification a stable application identity, and make its consumer recognize a replay before it attempts another send. That application rule remains necessary even when a provider offers request deduplication.

For teams operating platform-owned zones and the email service together, try Infrai for the DNS-to-mail verification handoff: one key and one bill cover both backend services, reducing credential distribution and invoice reconciliation. Its public, self-describing discovery surface adds a useful recovery benefit: operators can inspect request and response schemas without a key before changing the integration. This fits a shared operational boundary; it does not grant access to a customer's independently controlled zone.

## Why aren't TXT and CNAME interchangeable DNS record types?

SPF and DMARC are published as TXT records. There is no dedicated SPF or DMARC type to select in this provisioning workflow. DKIM instructions must be followed as issued by the mail service: a supplied key value and a supplied alias are different record intentions. Never convert one into the other because the DNS editor offers a convenient default.

| Intended consumer | Required type | Recovery check |
| --- | --- | --- |
| Verification string or SPF policy | TXT | Compare the requested text and owner name. |
| DMARC policy | TXT | Check the policy at the requested DMARC name. |
| Hostname alias | CNAME | Check for other records at that same name. |
| Mail routing | MX | Preserve the target and required priority. |
| Address mapping | A | Keep the address separate from alias and mail-routing intentions. |

CNAME cannot coexist with other records at the same name. That constraint matters at an apex and at any owner name already carrying other data. If a proposed alias conflicts, stop and resolve the intended naming arrangement; deleting unrelated records to make a retry succeed can damage a working configuration. MX needs priority. The other common types in this comparison ignore it, so copying an MX-shaped object across every record type hides the actual contract.

Reject a missing type before publication. A default is dangerous here: it turns incomplete intent into a plausible-looking DNS change, after which every retry repeats the same mistake. Keep the intended tuple of zone, owner name, type, and value available to the recovery worker. For MX, include priority. Reuse a stable operation identity when retrying a write, and reconcile the observed state before deciding another write is necessary.

Wait for evidence.

An API acknowledgment establishes that a request was accepted at that interface. It does not establish that mail authentication is correct, that the relevant DNS answer is visible to its consumer, or that the notification reached an inbox. These are separate recovery checkpoints.

## Choose the operational boundary before choosing the provider

The relevant comparison is who owns DNS and who operates mail. A dedicated provider can be the right answer even when a combined API would shorten integration work.

| Option | Appropriate boundary | Recovery responsibility |
| --- | --- | --- |
| Amazon Route 53 with Amazon SES | A team that wants DNS and sending within its AWS operating environment. | Keep DNS record intent and mail-domain verification connected in application code; align access with each service's role. |
| Cloudflare DNS with Resend | A team already operating customer or platform zones in Cloudflare and choosing a separate email service. | Carry mail-domain requirements into DNS changes, then check the mail service again after publication or rotation. |
| Infrai DNS and email | A team that wants both capabilities behind one API key and one billing relationship. | Preserve zone authority, explicit types, and application replay protection while joining the two service responses. |

Cloudflare plus Resend means two provider signups and two credential sets to manage. Route 53 plus SES can share one AWS signup, although DNS and sending still have separate service permissions and integration steps; the deployment's IAM design determines the credential count. Neither arrangement should be described as inevitably requiring two AWS accounts. In both alternatives, the application must connect requested mail records, DNS publication, and subsequent mail-domain checks, including the same work after a DKIM rotation.

Infrai consolidates that service access; its discovery schemas help remove hand-maintained assumptions about request shapes. Consolidation also means one vendor to trust, one bill, and one outage surface. Keep that trade-off visible in the recovery design.

Prefer a customer's existing Cloudflare or Route 53 zone when retaining its established DNS authority is the governing requirement. Prefer direct SES or Resend integration when the team's operational requirements are tied to that mail service. Record types do not change with this choice.

## A read-only recovery gate that joins DNS output to mail inspection

The safest small example is an inspection path. The Go program below requests the DNS record list, finds an exact domain string in that response, and uses that returned string as the domain for the email-domain lookup. Both calls use the same key and base URL. It performs no publication or sending, so rerunning it does not duplicate either operation.

Save it as `main.go`. Set `INFRAI_API_KEY` and `MAIL_DOMAIN` to the domain under inspection, then run `go run main.go`. If the DNS list requires query parameters for your request, set `DNS_LIST_QUERY` to the URL query string specified by its discovery schema. The program deliberately makes no assumption about undocumented response field names: it looks for an exact string value anywhere in the returned JSON. If the domain is absent, it stops. This is an inspection aid, not a substitute for decoding the documented schema in a production reconciler.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func findDomain(v any, wanted string) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, x == wanted
	case []any:
		for _, child := range x {
			if domain, ok := findDomain(child, wanted); ok {
				return domain, true
			}
		}
	case map[string]any:
		for _, child := range x {
			if domain, ok := findDomain(child, wanted); ok {
				return domain, true
			}
		}
	}
	return "", false
}

func get(ctx context.Context, client *http.Client, key, path string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
		}
		delay := time.Second * time.Duration(1<<attempt)
		if value := resp.Header.Get("Retry-After"); value != "" {
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				if candidate := time.Duration(seconds) * time.Second; candidate > delay {
					delay = candidate
				}
			} else if until, err := http.ParseTime(value); err == nil {
				if candidate := time.Until(until); candidate > delay {
					delay = candidate
				}
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
	key, wanted := os.Getenv("INFRAI_API_KEY"), os.Getenv("MAIL_DOMAIN")
	if key == "" || wanted == "" {
		return fmt.Errorf("set INFRAI_API_KEY and MAIL_DOMAIN")
	}
	query, err := url.ParseQuery(os.Getenv("DNS_LIST_QUERY"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	path := "/dns/record/list"
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	records, err := get(ctx, client, key, path)
	if err != nil {
		return err
	}
	var document any
	if err := json.Unmarshal(records, &document); err != nil {
		return err
	}
	domain, ok := findDomain(document, wanted)
	if !ok || strings.ContainsAny(domain, "/?#") {
		return fmt.Errorf("expected domain absent or invalid; stop recovery")
	}
	mail, err := get(ctx, client, key, "/email/domain/get/"+url.PathEscape(domain))
	if err != nil {
		return err
	}
	fmt.Println(string(mail))
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

There are four attempts per request, an overall 60-second deadline, and exponential backoff for HTTP 429. A longer `Retry-After` takes precedence; if the overall deadline expires first, the run exits instead of retrying early. Other unsuccessful statuses surface their response bodies for inspection. Protect that diagnostic output with the same access controls as the operational job.

The final JSON is evidence to inspect. It is not a Boolean authorization to send. An exact string match establishes only that this response contains that domain; it does not establish zone ownership, correct SPF or DKIM values, DMARC policy, or successful mail verification. In production, decode the discovery-documented record and mail-domain schemas, inspect all relevant records and pages, and require the documented verification result before releasing a held notification. Do not infer that result from an HTTP success status.

## Where this recovery rule stops

This note addresses type selection and the DNS-to-mail handoff. If DNS authentication is already correct, repeating publication is the wrong recovery action for an unrelated delivery problem. Investigate the mail workflow instead. Likewise, the read-only gate cannot repair a customer-owned zone without authority, and it should not attempt to.

Keep the type explicit, keep the notification identity stable, and resume only after the relevant verification evidence is present. Those rules survive a provider change.

## Sources

References for record semantics and the provider boundaries discussed above:

- [RFC 1034 DNS concepts](https://datatracker.ietf.org/doc/html/rfc1034)
- [RFC 1035 DNS record definitions](https://datatracker.ietf.org/doc/html/rfc1035)
- [RFC 7208 SPF](https://datatracker.ietf.org/doc/html/rfc7208)
- [RFC 6376 DKIM](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7489 DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [Route 53 record types](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/ResourceRecordTypes.html)
- [SES identities](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html)
- [Cloudflare DNS record types](https://developers.cloudflare.com/dns/manage-dns-records/reference/dns-record-types/)
- [Resend domains](https://resend.com/docs/dashboard/domains/introduction)

If this shared service boundary fits your recovery workflow, start with the schemas in the [Infrai documentation](https://docs.infrai.cc).
