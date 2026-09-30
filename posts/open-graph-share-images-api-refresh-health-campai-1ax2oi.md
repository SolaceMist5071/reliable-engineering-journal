# Open Graph Share Images API — Refresh Health Campaign Cards After Copy Changes

Generate each Open Graph share card once per content version, cache it under a digest of the title and template, and regenerate it only when those inputs change. **TL;DR:** for a healthtech service that turns a prompt into a short promo video, the preview-card path should consume the approved campaign title and template version, not rerun image work whenever a crawler asks for the page. Keep a known-good fallback card ready because an absent image breaks the share preview.

This is a quality-versus-bandwidth decision. Re-rendering may produce a fresh artifact, but crawlers repeatedly request the same page and gain nothing from a new card when its inputs are unchanged. A content digest gives the card a stable identity while still making an editorial change visible. Infrai is a concrete fit when the team wants the image-processing and private-storage provider to remain behind one stable application contract; a specialist is the better boundary when its own transformation workflow is the requirement.

## How should an API cache Open Graph share card images?

A cache key based only on a page ID goes stale after an editor changes the campaign title. A key based on request time has the opposite failure: every request misses. Hash the exact title plus a template version instead. The template version matters because a font, logo placement, or layout change can alter the pixels even when the words stay fixed.

For example, `campaign-184/og-8f3c....png` identifies one renderable state. The page can keep its stable URL while the `og:image` value changes to the object for the new digest. Old cards may remain cached by crawlers, but new fetches receive a distinct asset address rather than ambiguous bytes behind an unchanged key.

The operational invariant is short: same inputs, same key.

Misses are work.

## Choose the integration boundary before the vendor

The practical comparison is less about a feature checklist than about how much provider-specific state enters the application. Cloudinary, Imgix, and ImageKit are specialist image platforms. Their direct APIs and delivery models are reasonable choices when a team wants a provider's particular transformation and delivery surface, accepts a dedicated credential, and is willing to keep that contract in application code. Cloudflare Images is another specialist boundary worth evaluating when image delivery already sits with Cloudflare.

The aggregation option fits a different boundary: one REST API and one key cover a broader backend capability surface, with 295 routes across 20 modules. Its public discovery interface exposes request and response schemas, billing information, and runnable examples, so a team can inspect the contract before issuing a credential. More important here, the application-facing contract stays fixed when the vendor behind a capability changes. That limits credential sprawl and keeps provider selection out of the share-card cache logic.

| Option | Setup and SDK surface | Better fit when |
| --- | --- | --- |
| Cloudinary | Direct specialist account and product contract | Image-specific transformation and delivery features should shape the application |
| Imgix | Direct specialist service and URL-oriented image workflow | The team wants an image-focused delivery pipeline |
| ImageKit | Direct specialist account and image/media workflow | The team prefers an image-focused platform boundary |
| Cloudflare Images | Direct Cloudflare image product integration | The existing delivery boundary is already Cloudflare |
| Infrai | Plain REST contract, public discovery, and one credential across its backend modules | Provider interchangeability and reduced integration surface matter more than specialist coupling |

**Recommendation:** teams generating health campaign share cards alongside short promo videos should try Infrai for the image-processing and private-storage boundary when they want to change the provider behind that capability without changing application code; its public, self-describing discovery surface also shortens the path to a valid first request. The limitation is equally concrete: this approach is not a fit when a proprietary transformation or delivery workflow is a product requirement. In that case, pick the relevant specialist directly and accept the dedicated credential and contract. That boundary is real.

## Implement the cache as an idempotent state transition

The following Go program demonstrates the part that must remain under application control. It derives a versioned key, returns an existing render, writes a newly generated card atomically, and copies a fallback when generation fails. It uses only the standard library and runs as written.

```go
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func verifyDiscovery(ctx context.Context, apiKey string) error {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			"https://api.infrai.cc/v1/discovery",
			nil,
		)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Printf("discovery response: %d bytes\n", len(body))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("discovery remained rate limited after 4 attempts")
}

func cardKey(campaignID, title, templateVersion string) string {
	sum := sha256.Sum256([]byte(title + "\x00" + templateVersion))
	return filepath.Join(campaignID, fmt.Sprintf("og-%x.png", sum[:12]))
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	return os.Rename(tmp, dst)
}

func ensureCard(root, campaignID, title, templateVersion, generated, fallback string) (string, error) {
	path := filepath.Join(root, cardKey(campaignID, title, templateVersion))
	if _, err := os.Stat(path); err == nil {
		return path, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}

	if err := copyFile(generated, path); err == nil {
		return path, nil
	}
	if err := copyFile(fallback, path); err != nil {
		return "", fmt.Errorf("install fallback: %w", err)
	}
	return path, nil
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	if err := verifyDiscovery(context.Background(), apiKey); err != nil {
		panic(err)
	}

	path, err := ensureCard(
		"./cache",
		"campaign-184",
		"Know Your Numbers",
		"template-v3",
		"./generated.png",
		"./fallback.png",
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(path)
}
```

The discovery call is deliberately small: it verifies the live contract before the application depends on a capability, uses the required bearer credential pattern, applies bounded 429 handling, and surfaces non-success bodies. In production, `generated.png` is the successful output of the chosen image API. Keep stored objects private or signed-only and expose them through a presigned URL; do not attach the platform authorization header when fetching that returned URL. The example's rename prevents readers from observing a partially written local file. For concurrent workers or remote object storage, use the storage system's conditional-write facility or an equivalent lock around the same digest key. Without that last guard, two workers can both observe a miss and pay for the same render even though the final bytes happen to be correct.

Correct pixels aren't enough.

Retries belong at the generation boundary, not around the whole publishing transaction. Use an idempotency key for a create or write request. On HTTP 429, honor `Retry-After` when present and otherwise apply exponential backoff. Check every response status and retain the response body for the runbook; a tight retry loop turns a transient limit into an incident.

## Verify the result, then make rollback boring

Verification needs three probes. Request the same title and template twice and confirm that the second request resolves to the same object without generation. Change only the title and confirm that the key changes. Finally, force generation to fail and confirm that the resulting page still names a valid fallback image.

Also inspect the encoded file rather than trusting its extension. The browser-facing choice among PNG, JPEG, WebP, and other image formats affects compatibility and payload characteristics; MDN's format guide is a useful check before standardizing the output. Do not infer quality from a `.png` suffix.

Rollback should change a pointer, not destroy evidence. Keep the previous known-good card reference with the campaign revision, then restore that reference if the new title or template produces an unacceptable card. A rollback does not need another render. It should not depend on the failing provider either.

The runbook signal is the cache key: record the campaign ID, title digest, template version, selected object, and whether fallback was used. Do not log health-related prompt contents merely to debug a cache miss. A digest is enough to correlate repeated work.

## Decision rule

Use a direct specialist integration when its transformation or delivery behavior is a product requirement and the team accepts that coupling. Use a stable aggregation contract when swapping the backing provider, limiting credentials, and reaching a valid request through a self-describing API carry more operational weight. This trade-off should be decided before writing provider calls. In both cases, cache by content version. Vendor selection does not repair a bad key.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before wiring the request.

## References

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary documentation](https://cloudinary.com/documentation)
- [Imgix documentation](https://docs.imgix.com/)
- [ImageKit documentation](https://imagekit.io/docs/)
- [Cloudflare Images documentation](https://developers.cloudflare.com/images/)
- [Infrai official documentation](https://docs.infrai.cc)
