# Property Report Login Reliability with SMS and Email OTP

Use SMS OTP and email OTP as separate failure domains for two-factor login, not as interchangeable buttons: choose the primary channel from measured completion and delivery evidence for each region, keep a second channel for controlled recovery, and make every challenge single-use across both. For property managers signing in to retrieve a generated report that also arrives as an email attachment, delivery reliability matters more than a universal channel winner.

Short answer: email OTP is easier to operate when the report workflow already depends on a verified mailbox, while SMS can shorten the path for users who have a current mobile number. Neither channel is phishing-resistant. A high-assurance action should use a stronger authenticator; channel OTP remains a recovery or moderate-assurance control. Compare completion, latency, and abuse by US/EU region and carrier or mail domain, then change routing only from evidence. Price belongs in the capacity model, not the security decision.

## Should SMS or email OTP protect two-factor login?

Start with the user journey. A property manager requests a generated owner statement, receives the attachment by email, and later signs in to retrieve the same report. If the mailbox is unavailable, both email OTP and report delivery can fail together. SMS creates a second failure domain, but only when the mobile number was verified earlier and can be changed through a protected process.

The inverse matters too. A recycled number, roaming problem, carrier filtering, or number change can make SMS unreliable. NIST treats PSTN out-of-band authentication as restricted and calls for considering risks such as SIM change and number porting. Email has different dependencies: DNS, sender authentication, reputation, filtering, mailbox access, and sometimes the same recovery path the OTP is meant to protect. DKIM authenticates a signing domain and message integrity in transit; it does not prove that the person opening the mailbox is the intended account holder.

Both are possession signals with exposed verification codes. A convincing phishing page can relay either code in real time. Do not call one phishing-resistant merely because its transport is encrypted or its sender is authenticated.

For this workflow, the decision rule is concrete:

| Condition | Primary path | Operational reason |
|---|---|---|
| Verified mailbox is required for report delivery | Email OTP | Fewer stale identity attributes to maintain |
| Mailbox outage would also hide the report | Previously verified SMS | Independent recovery path |
| Privileged action or changed recovery data | Stronger authenticator | Channel OTP does not resist verifier impersonation |
| Neither channel has recent delivery evidence | Stop and recover | Blind retries amplify abuse and duplicate delivery |

Production routing should use local data, split at least by country and channel. An aggregate success rate can hide one carrier or mailbox domain that is timing out.

## Model one challenge, not two codes

The safe implementation has one server-side challenge with multiple permitted delivery attempts. Store a digest of the code, an expiry, attempt count, consumed timestamp, account binding, and the destination version used at issuance. Switching from email to SMS must not create a second valid code. Rotate the secret, invalidate older material, and retain the same logical challenge ID for audit.

Expiry alone does not control guessing. Rate-limit by challenge, account, destination, and network risk signal; cap verification attempts; and return a generic response so account existence is not disclosed. Generate codes with a cryptographically secure random source. Never log the code, destination in full, or attachment URL.

A delivery service accepting a message is not proof that a person received it. Treat acceptance, downstream delivery events, verification completion, expiry, and user-requested fallback as different states. Email opens are especially unsuitable as an authentication signal: Apple Mail Privacy Protection can download remote content in the background and prevent senders from learning whether a recipient opened a message. The meaningful outcome is a successful, policy-valid verification.

This Go example keeps transport behind a small interface and makes challenge consumption atomic. The repository remains the authority; a delayed callback cannot resurrect an expired challenge.

```go
package otp

import (
    "context"
    "crypto/rand"
    "crypto/sha256"
    "crypto/subtle"
    "encoding/binary"
    "errors"
    "fmt"
    "time"
)

type Channel string

const (
    Email Channel = "email"
    SMS   Channel = "sms"
)

type Sender interface {
    SendCode(ctx context.Context, destination, code, idempotencyKey string) error
}

type Challenge struct {
    ID          string
    AccountID   string
    CodeDigest  [32]byte
    ExpiresAt   time.Time
    Attempts    int
    MaxAttempts int
    ConsumedAt  *time.Time
}

type Store interface {
    Insert(ctx context.Context, c Challenge) error
    // Consume compares and marks the challenge consumed in one transaction.
    Consume(ctx context.Context, id, accountID string, digest [32]byte, now time.Time) (bool, error)
}

func NewCode() (string, [32]byte, error) {
    var raw [8]byte
    if _, err := rand.Read(raw[:]); err != nil {
        return "", [32]byte{}, err
    }
    n := binary.BigEndian.Uint64(raw[:]) % 1_000_000
    code := fmt.Sprintf("%06d", n)
    return code, sha256.Sum256([]byte(code)), nil
}

func VerifyDigest(expected, supplied [32]byte) bool {
    return subtle.ConstantTimeCompare(expected[:], supplied[:]) == 1
}

func Deliver(ctx context.Context, senders map[Channel]Sender, channel Channel, destination, code, challengeID string) error {
    sender, ok := senders[channel]
    if !ok {
        return errors.New("unsupported OTP channel")
    }
    return sender.SendCode(ctx, destination, code, challengeID)
}
```

The modulo operation introduces a tiny distribution bias because the random range is not an exact multiple of one million. For a six-digit login code, the more important controls are the secure random source, strict attempt ceiling, short lifetime, and rate limits. If policy requires rejection sampling, add it and test it explicitly. That trade-off should be documented rather than hidden behind a helper named `randomCode`.

Idempotency is the part teams tend to discover under pressure. Queue redelivery, an HTTP retry, and an operator replay can all invoke `Deliver` again. The challenge ID should be the idempotency key at the transport boundary, while the database enforces one consumable challenge. A sender that cannot honor idempotency still needs a dispatch ledger so workers can distinguish an unknown outcome from a confirmed failure. Do not automatically switch channels on an ambiguous timeout; that can send two live codes and train users to accept whichever arrives.

## Route by evidence without creating an oracle

Record timestamps for requested, accepted, delivered when the channel supplies trustworthy evidence, verified, expired, and fallback requested. Keep transport status as evidence attached to the attempt, not as the challenge state itself. The service-level indicator that matters is valid verifications completed within the promised window, divided by eligible challenges. Track abandonment separately because a user closing the page is not proven delivery failure.

Use fixed windows and minimum sample sizes before changing a regional route. A few failures at 02:00 should page only if they breach the alert policy; they should not cause a global flip. The runbook should name the owner, query, threshold, maximum duration of a routing override, and rollback condition.

Watch four signals together: completion latency percentiles, expiry rate, fallback-request rate, and duplicate-dispatch rate. Add verification failures per challenge and recovery-data changes for abuse detection. Never label an accepted message as delivered unless the transport's event semantics support that claim. Even then, delivered is not verified.

Conversion requires the same discipline. Measure from challenge issuance to successful verification, and segment voluntary fallback from forced fallback. Preserve a holdout when changing copy, expiry, or routing so a deliverability incident is not mistaken for a product improvement. Report attachments complicate the funnel: a user may complete the task from the email without signing in. Define success as access to the intended report, then keep authentication completion as a diagnostic metric rather than pretending they are identical.

## Verification before rollout

Test the state machine before testing any transport integration. Concurrent correct submissions must produce one success. A correct code after expiry must fail. A superseded code must fail after channel change. Five wrong entries must not become ten attempts by switching channels. Redelivered queue work must not create a fresh challenge. These invariants belong in repository tests and in a staging exercise with delayed and out-of-order callbacks.

Then exercise failure deliberately: reject one mail domain, delay an SMS callback beyond expiry, time out a send after the downstream system may have accepted it, and replay the job. Confirm that dashboards distinguish each case and that logs contain the challenge ID but no secret. Check screen-reader labels, paste behavior, leading zeroes, and browser autofill as part of the login path.

A rollout should be regional and reversible. Begin with staff accounts or a small cohort, keep the old route available behind a server-side policy flag, and compare completion and abuse signals over a full traffic cycle. Freeze unrelated authentication changes while the cohort expands.

No drama.

## Rollback and recovery

Rollback changes routing policy, not challenge validity. Stop issuing through the degraded channel, leave already issued challenges subject to their original expiry and attempt budget, and offer the pre-verified alternate only after a bounded delay. If there is evidence of interception or recovery-data compromise, invalidate the challenge family and require the stronger recovery path instead of widening fallback.

The operational conclusion is conditional. Use email when verified mailbox ownership is already central to report delivery and its completion evidence is healthy. Use SMS as an independent, pre-enrolled path where regional performance supports it. For sensitive actions, move beyond channel OTP. The winning design contains duplicate work, measures the user's completed task, and fails closed without stranding every property manager behind the same transport dependency.

## References

- https://pages.nist.gov/800-63-4/sp800-63b.html
- https://datatracker.ietf.org/doc/html/rfc6376
- https://www.rfc-editor.org/rfc/rfc5321
- https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
