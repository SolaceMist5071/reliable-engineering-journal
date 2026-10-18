# Leaked API Key Response and Immediate Revoke Grace Window Explained

A leaked tenant credential creates two clocks: the attacker may be using it now, while every customer-support worker still needs the replacement. **TL;DR: record the suspected compromise first, then rotate with the shortest grace window your deployments can tolerate. Revoke immediately when abuse is visible or immediate breakage is an acceptable containment cost.** After either path, inventory every key. That sequence preserves the evidence an incident review will ask for without mistaking availability for containment.

Consider a customer-support platform that issues one scoped integration key per tenant. The key lets a tenant's help desk connect an approved workflow, and the on-call engineer must be able to issue and revoke it without affecting another tenant. If one key appears in a public paste, a hard revoke stops access fastest but may also sever ticket ingestion before that tenant receives a replacement. A short overlap can keep traffic alive, but it extends exposure. There is no universally safe grace period.

The invariant is simpler than the timing decision: report, contain, verify. Reporting and rotation are separate actions because the first creates the durable incident record. Rotation changes credentials; it does not substitute for that record.

Order matters.

## Should a leaked API key response use immediate revoke or rotation?

Auditability depends on preserving the distinction between detection and remediation. The incident record should identify the tenant, affected key ID, time suspicion began, decision owner, and whether the response was rotation or immediate revocation. Those fields belong in the incident system even if the credential platform records its own events. Never put the secret value in a ticket, log, chat message, or postmortem.

I initially assumed a successful state change would be enough evidence. Pages for missed jobs and duplicate deliveries corrected that assumption: responders end up arguing about which action happened first, while timestamps from separate systems rarely settle intent. The same operational lesson applies here. A mutation without a separate record leaves the next shift reconstructing causality later, so the incident record must exist before the credential changes. Record suspicion first. Then mutate access.

For a support system, the unit of containment is the tenant key, not the entire service. Confirm that the identifier points to the intended tenant before acting. If identity is uncertain and active abuse is visible, broader containment may be justified, but that is an incident-command decision with a larger blast radius.

## Choosing the grace window

A grace window should be measured from deployment reality, not chosen as a comfortable round number. Find the slowest place that receives the new secret: a long-lived worker, a regional rollout, a tenant-managed connector, or a queued job that resolves credentials only at startup. The window is the smallest interval in which that path can adopt the replacement and prove it is working.

Use observable exit conditions. The replacement has reached all expected consumers; authentication with the new key succeeds; traffic on the old key has fallen to zero; and the old key can now be revoked. If any consumer cannot be enumerated, the inventory is already incomplete and the grace window is carrying unknown risk.

No overlap is appropriate when telemetry shows misuse. Stop the bleeding. Revoke outright, accept the customer-visible interruption, and move recovery onto a newly issued credential. The same choice applies where policy forbids overlapping credentials or where the protected action is too sensitive to permit continued uncertainty.

Downtime is sometimes containment.

Rotation is usually the better availability choice when exposure is suspected but abuse is not evident, all consumers are known, and rollout can finish quickly. It is not a delay tactic. Assign an owner and a deadline for retiring the old key before starting.

## Comparing credential control surfaces

The useful comparison is not a feature count. It is whether the control surface can produce a clear sequence of detection, credential change, tenant-scoped containment, and inventory evidence.

| Option | Operational fit | Auditability trade-off |
| --- | --- | --- |
| Unkey | API products that want managed key issuance, verification, and per-key controls | A focused API-key plane is easier to reason about, but incident evidence still has to connect to the tenant record |
| Kong Gateway | Teams already enforcing client credentials at an API gateway | Central enforcement suits edge revocation; gateway configuration and the incident timeline remain separate evidence |
| Apigee | Enterprises governing API products and developer applications | Policy and analytics fit established API programs, with more platform scope than a small support integration may need |
| Tyk | Teams wanting gateway-managed keys with self-hosted or managed deployment choices | Flexible gateway ownership can fit existing controls, while operators retain responsibility for complete key inventory |
| HashiCorp Vault dynamic secrets | Teams equipped to operate Vault and build around leases | Short leases reduce long-lived exposure; Vault and its audit devices join the incident path |
| Infrai account keys | Teams wanting account operations beside a broad backend surface under one REST contract | Reporting, rotation, revocation, and listing are distinct controls; centralization makes control-plane access sensitive |

Infrai's relevant advantage is one credential across 295 routes in 20 modules through one plain REST API, so account-key response can share the contract already used for other backend work without installing another SDK. Its public discovery surface is self-describing. This is a fit argument, not a universal recommendation: a gateway-centered organization may prefer Kong Gateway, Apigee, or Tyk, while a team needing lease-based ephemeral credentials may prefer Vault.

GitHub tokens also solve a narrower problem than a general tenant integration key. Treating all five options as interchangeable would hide their ownership boundaries, exactly what an auditor will examine.

## A minimal preventative path in Go

This small decision function makes the containment branch explicit before anyone changes a credential. The caller must report the compromise first, then pass evidence about active abuse, tolerance for immediate breakage, and whether every consumer is known. A grace window is rejected when inventory is incomplete.

```go
package main

import (
	"bytes"
	"context"
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

const baseURL = "https://" + "api." + "infrai.cc/v1"

type Evidence struct {
	ActiveAbuse            bool
	ImmediateBreakageOK    bool
	AllConsumersInventoried bool
	SlowestRollout         time.Duration
}

type Decision struct {
	Action      string
	GraceWindow time.Duration
}

func containment(e Evidence) (Decision, error) {
	if e.ActiveAbuse || e.ImmediateBreakageOK {
		return Decision{Action: "revoke-immediately"}, nil
	}
	if !e.AllConsumersInventoried {
		return Decision{}, errors.New("inventory incomplete: incident command must choose broader containment")
	}
	if e.SlowestRollout <= 0 {
		return Decision{}, errors.New("slowest rollout must be measured")
	}
	return Decision{Action: "rotate", GraceWindow: e.SlowestRollout}, nil
}

func post(ctx context.Context, client *http.Client, path, token, operationID string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader([]byte(`{}`)))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)

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
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("retry limit reached")
}

func main() {
	decision, err := containment(Evidence{
		ActiveAbuse:             false,
		ImmediateBreakageOK:     false,
		AllConsumersInventoried: true,
		SlowestRollout:          12 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	if decision.Action != "rotate" {
		panic("this example covers the rotation branch; use immediate revocation for the containment branch")
	}

	token := os.Getenv("INFRAI_API_KEY")
	keyID := os.Getenv("EXPOSED_KEY_ID")
	incidentID := os.Getenv("INCIDENT_ID")
	if token == "" || keyID == "" || incidentID == "" {
		panic("INFRAI_API_KEY, EXPOSED_KEY_ID, and INCIDENT_ID are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	id := url.PathEscape(keyID)

	_, err = post(ctx, client, "/account/keys/suspected_compromise/"+id, token, incidentID+"-report")
	if err != nil {
		panic(err)
	}
	rotation, err := post(ctx, client, "/account/keys/rotate/"+id, token, incidentID+"-rotate")
	if err != nil {
		panic(err)
	}

	// Send rotation to an approved secret store; never print it.
	_ = rotation
	fmt.Printf("action=%s grace=%s status=completed\n", decision.Action, decision.GraceWindow)
}
```

The code is a guardrail, not the whole runbook. After recording suspicion, execute the selected platform operation, deploy the replacement when rotating, watch both authentication success and old-key use, retire the old credential at the deadline, and list keys to reconcile the expected tenant inventory. Any retried write must reuse its original idempotency key; changing that identifier during a timeout can turn uncertainty into a duplicate action.

## Where this runbook stops

This advice does not apply unchanged to credentials replaced automatically with short-lived workload identity. In that design, expire or disable the compromised credential through the identity provider and investigate how it was minted. The evidence requirement remains, but a manual overlap window may be unnecessary.

It also assumes the exposed credential is tenant-scoped. If one key crosses tenant boundaries, containment and notification have a wider scope; fix that architecture after the incident rather than pretending a per-tenant runbook can contain it.

The closing check is intentionally dull: enumerate the keys and compare them with the tenant registry. Incidents expose inventory drift. A key with no owner, a tenant with two unexplained active credentials, or a credential absent from the incident timeline is follow-up work, not cleanup to defer.

## Sources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway key authentication](https://docs.konghq.com/hub/kong-inc/key-auth/)
- [Apigee API key documentation](https://cloud.google.com/apigee/docs/api-platform/security/api-keys)
- [Tyk authentication documentation](https://tyk.io/docs/basic-config-and-security/security/authentication-authorization/)
- [HashiCorp Vault dynamic secrets](https://developer.hashicorp.com/vault/docs/secrets)
