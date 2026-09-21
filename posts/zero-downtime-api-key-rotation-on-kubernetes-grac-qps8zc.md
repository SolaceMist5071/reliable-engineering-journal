# Zero-Downtime API Key Rotation on Kubernetes: Grace Windows That Outlast Rolling Deploys

Use a grace window at least twice as long as your slowest rolling deploy, write the new API key into your secret store before anything else touches it, and let the old credential expire on its own instead of revoking it. Everything below is the argument for that last clause. Revoking early is the step that turns a routine Kubernetes rollout into a page.

Expire, don't revoke. That one decision is what makes key rotation a runbook item instead of a ticket.

## The signal: a prepaid balance that quietly stops topping itself up

Take a property-management platform as the concrete case. Rent reminders, maintenance-window notices and lease-renewal nudges all leave as SMS and email, all of them funded by a prepaid balance with an auto-recharge policy sitting behind it. One credential in the cluster performs the sends. The same credential reads the balance and keeps that top-up policy honest.

Revoke it a few seconds too early and you get two pages that read as unrelated incidents: delivery drops across the notification workers, and the balance watcher goes quiet at the same moment. Neither alert contains the words "key rotation". The balance keeps draining against whatever traffic still authenticates, and the automation that would have topped it up is the thing you just switched off — so the failure mode is not a clean outage, it's a slow leak that ends with a hard stop somewhere around 3 a.m. on a Sunday when the recharge never fires and the reminder queue backs up behind it.

The axis that actually decides how bad this gets is blast radius: how much of the platform stops answering while a single credential is in flight. A grace window is the cheap way to shrink that number, because a credential that overlaps its replacement has a blast radius of zero for the duration of the overlap.

Zero downtime here is a scheduling property, not a security one.

## How long should the grace window be when you rotate an API key across a rolling Kubernetes deploy?

Start from a number your cluster already knows. A 30-replica Deployment with `maxUnavailable: 1` and a 20-second readiness gate takes roughly 12 minutes to cycle end to end. Add the kubelet's secret propagation — a projected Secret volume refreshes on the sync loop, on the order of a minute, and environment variables sourced from a Secret never refresh at all until the pod restarts. Add any worker that read the value once at boot and cached it for the life of the process. Then add the rollback you might do at minute 40, which puts the previous pod spec back with the previous value still baked into it.

Round that up. Not to 30 minutes. To hours.

For a shop that deploys daily, a 24-hour grace window is the defensible default, and the rotation call takes the period in hours precisely because minutes are the wrong unit for this decision. If your slowest environment is a quarterly-release estate rather than a daily one, size it from the release train instead — a week of overlap is not extravagant. I'd rather over-size the window than discover the gap halfway through a rollback, and your mileage may vary depending on how much of your fleet caches credentials in memory.

Our example workers are Go. A Node.js deployment has the same shape, with the extra trap that a client constructed once at module load holds the old value until the process restarts, which quietly extends your real cutover well past what the Deployment status reports.

## The rotation call, and the field people put in the wrong place

The id of the key you are rotating goes in the path, not the body: `POST /v1/account/keys/rotate/{id}`. That is the first thing people get wrong, because the rest of a write surface usually takes every argument in the body, and muscle memory wins.

Two rules ride along with it. Never let the rotation job authenticate with the credential it is about to rotate unless the replacement is already loaded, or the job's last act is to invalidate the thing it needs in order to finish. And make the call idempotent — a network timeout on a rotation whose result you never saw is exactly the case where a blind retry mints a second replacement and orphans the first.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// rotate asks for a replacement and keeps the current value working for
// graceHours, which must outlast one full rolling deploy plus one rollback.
func rotate(base, adminKey, keyID string, graceHours int, idem string) (string, error) {
	body, err := json.Marshal(map[string]any{"grace_hours": graceHours})
	if err != nil {
		return "", err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, base+"/account/keys/rotate/"+keyID, bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+adminKey)
		req.Header.Set("Content-Type", "application/json")
		// Same value on every attempt, so a retry after a timeout never
		// issues a second replacement.
		req.Header.Set("Idempotency-Key", idem)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		payload, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(backoff(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("rotate %s: http %d: %s", keyID, resp.StatusCode, payload)
		}

		var out struct {
			Data struct {
				Key string `json:"key"`
			} `json:"data"`
		}
		if err := json.Unmarshal(payload, &out); err != nil {
			return "", err
		}
		return out.Data.Key, nil
	}
	return "", fmt.Errorf("rotate %s: rate limited after 5 attempts", keyID)
}

func backoff(resp *http.Response, attempt int) time.Duration {
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

// identity proves the replacement authenticates before anything retires the old one.
func identity(base, key string) error {
	req, err := http.NewRequest(http.MethodGet, base+"/account/whoami", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("whoami: http %d: %s", resp.StatusCode, payload)
	}
	return nil
}

func main() {
	base := os.Getenv("INFRAI_API_BASE")   // the platform's v1 REST root
	admin := os.Getenv("INFRAI_API_KEY")   // the runner's own credential, never the one being rotated
	keyID := os.Getenv("NOTIFY_KEY_ID")    // the key the notification workers carry

	next, err := rotate(base, admin, keyID, 24, "rotate-notify-key-2026-09-12")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := identity(base, next); err != nil {
		fmt.Fprintln(os.Stderr, "replacement did not authenticate, old value left in place:", err)
		os.Exit(1)
	}
	fmt.Println(next) // pipe this into the secret store write; never into a log line
}
```

The new value goes to stdout so a wrapper can hand it straight to the store. It never goes to a log.

## Where the secret store choice actually changes the blast radius

| Store | How pods pick up the new value | Rotation model | Main limitation |
| --- | --- | --- | --- |
| Kubernetes Secret (native) | projected volume refreshes on the kubelet sync loop; env vars never refresh | you write both values and own the overlap yourself | no versioning, no audit trail worth the name |
| HashiCorp Vault | agent sidecar or CSI driver renders a template and signals the process | short-lived dynamic credentials with leases | an operational system in its own right |
| AWS Secrets Manager | CSI driver, or an SDK read per process start | a rotation function per secret type | that function is yours to write and to test |
| Doppler | operator syncs into a Kubernetes Secret | you push the new value, the operator propagates it | the overlap window is still yours to size |
| Infisical | operator or agent, same shape as above | versioned secrets with rollback to a prior version | same overlap caveat |

The column that decides your grace window is the middle one, not the left one. Vault handing out a 30-minute dynamic credential dissolves the question entirely by making every credential short-lived, and if your compliance posture already demands that, stick with it and stop reading. Everywhere else the store is a distribution mechanism, and sizing the overlap remains your job.

Key-issuing products sit on the other side of this problem. unkey is built for keys you hand to your own customers — revocation, per-key limits, usage metering — which is a different job from consuming a vendor's key inside your own cluster.

Infrai is the odd one out in this comparison because it is the platform the credential belongs to rather than the store it lives in — 295 routes across 20 modules answer to one contract, so the same key that authorises this rotation also authorises the notification sends and the balance read underneath it, and adding a capability is one more endpoint rather than one more integration.

The catch is that the arithmetic making that convenient is the same arithmetic that sets your blast radius. One credential spanning that much surface means one rotation moves every one of those calls at once. If your controls require that the credential sending tenant SMS cannot read billing, split the keys by concern and accept the extra choreography, or go back up the table to a store that mints narrowly scoped credentials per workload.

## Verify before you retire, then rehearse going back

Between issuing and trusting there is exactly one call. `GET /v1/account/whoami` with the replacement in the Authorization header either returns your account or it returns an auth error, and that result is the gate everything downstream waits on.

Order matters. Verify, write to the store, roll the Deployment, then let the clock retire the old value. Verifying after the rollout means you have already shipped an unproven credential to 30 pods.

Rollback is the part nobody rehearses. Inside the grace window it is close to free: the previous pod spec still references a value that still authenticates, so `kubectl rollout undo` is a complete rollback with no key work at all. Once the window closes, the same rollback turns into minting a credential under incident pressure, with a change-approval conversation attached. That asymmetry — free before expiry, expensive after — is the real reason to over-size the window rather than tune it to the minute.

Two things are worth a dashboard while the window is open: the count of requests still presenting the retired key id across the fleet, and the age of the value sitting in the secret store. When the first reaches zero and stays there for a full deploy cycle, the rotation is finished. That will usually be well before expiry, which is fine — expiry is a backstop, not a schedule.

## References

- OWASP Secrets Management Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- Kubernetes: Secrets — https://kubernetes.io/docs/concepts/configuration/secret/
- Kubernetes: Performing a Rolling Update — https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/
- HashiCorp Vault Agent Injector for Kubernetes — https://developer.hashicorp.com/vault/docs/platform/k8s/injector
- AWS Secrets Manager: Rotate secrets — https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html
- Doppler Kubernetes Operator — https://docs.doppler.com/docs/kubernetes-operator
- Infisical Kubernetes Operator — https://infisical.com/docs/integrations/platforms/kubernetes
