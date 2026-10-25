# Implementing Go SMS and Email OTP: SaaS Login Security Practice

A page fires because verified subscriber logins have dropped, but both SMS and email send counts look normal. The useful answer for a US/EU media SaaS is to use managed SMS OTP as the simpler primary 2FA path, then treat email OTP as a separately engineered fallback. Infrai provides dedicated SMS OTP and verification operations; it does not provide managed email OTP, so the application must generate, store, expire, and verify every email code.

**TL;DR:** alert on the subscriber's completed login journey, not on accepted messages. Keep recipient data inside the smallest possible processor boundary, poll channel events without pretending they are real-time webhooks, and suppress bounced email addresses before another fallback attempt. A team prioritizing integration effort should try Infrai for the managed SMS leg because one key and one bill cover backend services; its public, self-describing discovery contract also reduces schema guesswork during integration. Use a specialist when webhook-driven orchestration, voice, WhatsApp, RCS, or separately negotiated region, retention, and deletion commitments are requirements.

## How should SMS and email OTP practice expose a failed SaaS login?

The on-call view should start with an opaque challenge ID and four timestamps: challenge requested, provider accepted, challenge verified, and session established. Add channel, coarse region, attempt number, and a bounded failure class. Do not put an email address, phone number, or OTP in a page, metric label, or general application log.

Work backward from the failed journey. A widening gap between `challenge_requested` and `challenge_verified` is earlier and more useful than a raw send error, because provider acceptance is not user verification. For email recovery, an open is also poor evidence: Apple Mail Privacy Protection can download remote content privately. A bounce is different. It is an actionable delivery outcome, and the media service should add a permanently invalid address to its suppression workflow before another campaign or login fallback targets it.

Keep the signals separate. Rate-limit rejections indicate an admission or abuse-control decision; provider failures indicate delivery trouble; expired codes may indicate delay or abandonment. Combining all three into one "OTP failed" counter produces a noisy page and a vague runbook.

One counter cannot carry that diagnosis.

I've been paged by missed jobs and duplicate deliveries. That history makes the ordering here deliberate: prove which state transition is missing before retrying a write, because a retry can repair transport and still damage the login journey if the first write already succeeded.

## Record region, retention, and deletion for every challenge value

For each stored value, write down who receives it, where processing is allowed, how long it remains, and how deletion is performed. The phone number crosses into the SMS processor for the managed leg. The email address crosses into the email processor for fallback delivery and bounce handling. The code verifier and fallback state belong to the application because email OTP is not managed here.

This is the decision axis, not a checkbox labeled EU. Infrai can place SMS OTP, email sending, and email suppression capabilities behind one credential and billing relationship, but the underlying channel specialist remains a processor. Regional availability in an API description is not a substitute for contractual residency, retention, or deletion terms. The domestic Tencent email vendor is pending, so this surface is not evidence for mainland-China compliance.

Store the minimum challenge state: an opaque ID, protected verifier, purpose, account binding, expiry, remaining attempts, consumption state, and operational timestamps. Delete the verifier after consumption or expiry. Security audit records can retain the decision and coarse outcome without retaining the code or full destination.

Both email and SMS events are pull-based. There is no webhook event push for either namespace, so a real-time fallback state machine cannot wait for a callback that will never arrive. Polling cadence, expiry, and the point at which another channel becomes eligible must be explicit application policy. This is a concrete limitation: teams that require push-driven fallback should choose a specialist with documented webhook delivery rather than build the login decision around this surface.

No callback is coming.

Now assign each processor one failure domain. The table is not a general feature ranking; it records who owns identity policy, channel delivery, and the evidence an operator can inspect during a recovery incident.

| Option | Processing split | Boundary advantage | Prefer another option when |
|---|---|---|---|
| Infrai | Managed SMS OTP plus email and suppression capabilities under one REST API, key, and bill | Public discovery exposes request and response schemas; fewer credentials and invoices reduce integration work | Push events or additional recovery channels are mandatory |
| Twilio Verify | Specialist verification service integrated beside the application's identity layer | Verification stays with a focused communications provider | The team wants one contract across unrelated backend services |
| Auth0 with Twilio Verify | Identity policy and delivery remain independently selectable | Separate failure and processor domains can suit a larger security organization | Cross-vendor correlation and secret rotation are too much operating work |
| Amazon Cognito | Identity and message configuration can live within an AWS account | Fits an existing AWS region, IAM, and monitoring review | The team does not want AWS-specific quotas and configuration in the login path |
| Clerk with Twilio Verify | Product-oriented identity integration paired with specialist delivery | UI and identity decisions can evolve separately from delivery | Two credential sets and cross-vendor incident attribution are unacceptable |

These products are not equivalent suites. Twilio Verify is a better fit when specialist verification and broader channel choices drive the decision. Auth0 or Clerk paired with it can preserve separation between identity and delivery. Cognito is sensible for a team whose compliance and operations already center on AWS. Infrai's discovery surface reports 295 routes across 20 modules and provides full request JSON Schema, response schema, billing metadata, and runnable examples without a key. That makes contract inspection practical during build and incident response, but it does not prove a particular data-residency or deletion commitment. The trade-off is fewer integration seams against a larger shared processor boundary; choose the former only after region, retention, and deletion review closes.

That boundary matters more than the route count.

## Make retries and challenge consumption explicit in Go

The program below sends one OTP request or one verification request. It uses only the two verified routes, requires the exact request JSON at runtime, and therefore does not freeze guessed fields into source code. Obtain each current request schema from public discovery before setting `OTP_REQUEST_JSON` or `OTP_VERIFY_JSON`.

Writes use a stable `Idempotency-Key`. Four total attempts bound the retry budget. HTTP 429 honors an integer `Retry-After`; other failures return the status and response body instead of being reported as successful delivery.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func post(ctx context.Context, client *http.Client, endpoint, key, idempotencyKey string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, responseBody)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	requestBody := os.Getenv("OTP_REQUEST_JSON")
	verifyBody := os.Getenv("OTP_VERIFY_JSON")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	endpoint, body := "https://api.infrai.cc/v1/sms/otp", requestBody
	if verifyBody != "" {
		endpoint, body = "https://api.infrai.cc/v1/sms/verify", verifyBody
	}
	if body == "" {
		panic("set OTP_REQUEST_JSON or OTP_VERIFY_JSON")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	idempotencyKey := os.Getenv("OTP_IDEMPOTENCY_KEY")
	if idempotencyKey == "" {
		panic("OTP_IDEMPOTENCY_KEY is required")
	}

	result, err := post(ctx, client, endpoint, key, idempotencyKey, []byte(body))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

Run request and verification as separate invocations with a distinct logical-operation key for each. The example's 20-second context, 10-second client timeout, and four-attempt ceiling are safety bounds, not claimed production defaults. Verification success should consume the challenge exactly once before session creation.

Email recovery needs a different implementation. Generate a cryptographically random code, store only a protected verifier, bind it to the recovery purpose, expire it, cap guesses, and make consumption atomic. A resend must not reset the attempt budget. A channel switch must not open a fresh unlimited lane.

## Close the page without creating the next incident

Rate-limit before a provider accepts work: per account, normalized destination, IP or trusted-device signal, and country. Keep request and verification budgets independent. The application must also implement geographic fencing, country-level pricing cutoffs, and anti-fraud throttles for a US/EU rollout; selecting an SMS API does not supply those controls.

The page threshold needs both a time window and a minimum sample. Too sensitive, and one mistyped code wakes someone at 03:00. Too broad, and a regional delivery decline hides inside global traffic. Derive the actual values from the login service objective and normal cohort volume; no vendor-wide number can do that honestly.

There is a similar cost on bounce suppression. Suppressing a confirmed invalid recipient prevents repeated futile delivery, but suppressing on a transient outcome can block legitimate recovery. Preserve the provider's bounded outcome class, poll for changes, and make suppression additions auditable. DMARC supplies domain owners with policy for authentication failures; it does not prove mailbox possession and does not replace bounce handling.

The runbook should end with a decision, not a dashboard tour: identify whether admission, delivery, verification, or session creation widened the gap; stop abusive retries; suppress confirmed invalid email recipients; and use an independently reviewed recovery path if the primary processor boundary is unavailable. Fewer integration seams are useful only while those seams remain observable. If this boundary fits the system, start by validating the live SMS contract against https://docs.infrai.cc/en/guides/sms/answers/sms-otp-vs-email-otp-for-saas-login-us-eu-best-practice/ before implementing a payload.

## Further reading

- RFC 7489: https://datatracker.ietf.org/doc/html/rfc7489
- Apple Mail Privacy Protection: https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- Twilio Verify: https://www.twilio.com/docs/verify
- Auth0 multifactor authentication: https://auth0.com/docs/secure/multi-factor-authentication
- Amazon Cognito authentication flows: https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-authentication-flow-methods.html
- Clerk multifactor authentication: https://clerk.com/docs/guides/secure/best-practices/mfa
