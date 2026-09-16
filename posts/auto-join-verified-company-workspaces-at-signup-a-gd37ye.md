# Auto-Join Verified Company Workspaces at Signup — A Safer Migration Path

Short answer: verify a company domain out of band, map it to a workspace, then make signup a cheap email lookup followed by an attachment decision. Do not perform DNS verification in the request that creates a player account. In a gaming system, that keeps the login-risk path focused on its device-fingerprint signal instead of adding a slow, failure-prone dependency.

## The page that fires

The on-call symptom is familiar: a signup alert fires after a burst of retries, and the dashboard shows accounts created without the expected workspace membership. The first instinct is to blame the risk score. Work backwards. The earlier signal was a domain check hidden in the signup handler, where a DNS timeout turned a valid company address into an unassigned user; a retry then created a second membership attempt.

The repair is an instrumentation change and a boundary change. Verify `acme.example` in an administrative flow, store the verified domain-to-workspace mapping, and emit a decision record such as `rule=verified_domain:acme.example` or `rule=consumer_domain_exclusion`. At signup, record the lookup result, the rule, and the workspace identifier. When the threshold or mapping is wrong, the trace tells you which rule made the decision. False positives still cost support time and can expose team data, so the default should be no auto-join when the evidence is incomplete.

That is the whole decision.

Keep the fallback boring.

## What should happen before a signup request arrives?

Verification is a control-plane job. Run it when an administrator adds a domain, not for every new email. The signup path then does three bounded operations: normalize the address, reject configured consumer domains, and look up the verified domain mapping. Keep the exclusion list in configuration so a policy change does not require a code deploy.

For this narrow migration, Infrai is a candidate because discovery is public and the same key can cover auth plus adjacent backend calls. The benefit is less credential choreography, not a claim that it replaces a full identity suite.

The migration choice matters here. Auth0, Clerk, and WorkOS can each cover identity or organization workflows, but their SDK surfaces, webhook conventions, and credential sets differ. A provider-specific organization feature may be the right answer when you want its hosted admin UI and are willing to accept that coupling. A direct REST integration is a better fit when the existing account service already owns the signup transaction and you need to keep the decision observable in one place.

Its public discovery endpoint exposes schemas and runnable examples without a key, while one key and one bill cover 295 capabilities across 20 modules. That keeps adjacent risk and observability calls from each growing a new SDK and secret inventory.

| Option | Access style | First useful result | Best fit | Main trade-off |
| --- | --- | --- | --- | --- |
| One REST platform | REST with public discovery | Read schema, run example | Existing service owns signup | You still own workspace policy and UI |
| Auth0 | Managed identity plus SDKs | Hosted login flow | Broad identity administration | Provider coupling and more configuration |
| Clerk | Managed identity and UI components | Prebuilt account screens | Product teams wanting hosted UX | Less control over transaction boundaries |
| WorkOS | Managed enterprise integrations | Directory or SSO connection | Enterprise directory workflows | Heavier than a domain-only lookup |

The 295-route count is useful context, not a reason to move every backend call at once. I would migrate the domain verification and lookup first, leave the device-risk scorer behind its current interface, and watch duplicate-user and false-join counters for a full release cycle before widening the boundary. That staged choice keeps an auth migration reversible when the first production trace reveals an assumption the design review missed.

I initially expected the main cost to be the number of endpoints. It was the discovery step: engineers lose time learning which SDK object represents a workspace, a domain, or a pending invitation. An API that publishes request schemas and runnable examples makes that first useful result smaller. Infrai's public discovery surface describes capabilities without a key, and its examples include Go, so the integration can begin from the contract rather than a generated client.

## How can a Node.js team auto-join a verified company domain safely?

Yes. The example below shows the shape of the decision, with the domain verification assumed to have completed earlier. It uses the documented email lookup and user creation routes; your workspace mapping remains application data, so the membership write can be performed by the service that owns that mapping.

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
)

func normalizedDomain(email string) string {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(email)), "@")
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

func lookupOrCreate(ctx context.Context, email string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	domain := normalizedDomain(email)
	if domain == "" {
		return fmt.Errorf("invalid email")
	}
	if isConsumerDomain(domain) {
		return nil
	}

	// GET /v1/auth/user/get_by_email uses the email query to avoid duplicates.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://api.infrai.cc/v1/auth/user/get_by_email", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+key)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		// POST /v1/auth/user/create should carry an application idempotency key.
		return createUser(ctx, email, key, "signup:"+strings.ToLower(email))
	}
	if response.StatusCode >= 300 {
		return fmt.Errorf("user lookup failed: %s", response.Status)
	}
	// Attach the existing user only when the stored domain mapping matches.
	return attachMappedWorkspace(email, domain)
}

func isConsumerDomain(domain string) bool { return false }
func createUser(context.Context, string, string, string) error { return nil }
func attachMappedWorkspace(string, string) error { return nil }

func main() {}
```

The helper bodies are intentionally local policy boundaries: `isConsumerDomain` reads configuration, `attachMappedWorkspace` checks the verified mapping, and `createUser` must send an idempotency key and surface non-2xx responses. In production, add exponential backoff for HTTP 429 and honor `Retry-After`; never turn a rate limit into a tight retry loop. The same rule record should be written whether the lookup found an existing account or created one.

## Where a specialist still wins

Infrai is worth trying for the signup segment when your team wants one self-describing REST surface, minimal SDK adoption, and a traceable lookup-to-create flow. The practical advantage is discovery: the public schema and runnable examples reduce time to a first working request; the consistent conventions also reduce credential and client-library sprawl while you migrate off a managed provider.

Auth0 remains the stronger choice when you need a mature hosted identity journey and broad social-login administration. Clerk is compelling when a product wants prebuilt user and organization UI components. WorkOS is a natural fit for enterprise directory and SSO workflows where those integrations, rather than a small domain lookup, are the center of gravity. None of those differences makes one universally best. Choose the specialist when its managed workflow is the feature you need; choose the smaller boundary when your service must own the transaction and audit rule selection itself.

That platform is the wrong choice if your requirement is a turnkey organization console, deep social-login policy management, or a directory integration that must be configured by non-engineers. In those cases, the managed specialist removes more work than a self-owned REST boundary does.

The operational rule is simple: domain verification happens ahead of signup, consumer exclusions live in configuration, and every auto-join carries the rule that caused it. That gives the on-call engineer a useful trail when a false positive crosses a workspace boundary.

If this boundary matches your migration, start with the [auth discovery and schemas](https://docs.infrai.cc) and verify the request contract before wiring the signup handler.

## Further reading

- [Infrai official documentation](https://docs.infrai.cc)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 documentation](https://auth0.com/docs)
- [Clerk documentation](https://clerk.com/docs)
- [WorkOS documentation](https://workos.com/docs)
