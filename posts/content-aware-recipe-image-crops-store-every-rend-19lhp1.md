# Content-Aware Recipe Image Crops — Store Every Rendered Aspect Ratio

An e-commerce image request should not have to discover a focal point, crop a source, compress the result, and write an object before it can return bytes. **TL;DR: enumerate the aspect ratios used by the storefront, smart-crop and optimize every variant during ingest, store each under a deterministic private key, and retain the original.** This spends storage and cache capacity deliberately in exchange for predictable request latency and repeatable output.

That decision is especially useful for recipe photos. A square search tile, a 4:3 recipe card, and a 16:9 banner can need three different crops of the same dish; making one “master crop” and resizing it later may remove the subject in one of those slots. Generate against each actual ratio.

## How should a Node.js Express service content-aware crop multiple aspect ratios?

I have been paged by missed jobs and duplicate deliveries in cron and queue systems. I learned to distrust “ran once” as a completion condition. That experience changes how I review an image pipeline even though it is not an image benchmark or customer story: a background task is not complete because it ran once, and a retry is not harmless merely because the HTTP request succeeded. The same rule applies when an Express upload handler hands a recipe photo to a worker.

Retries happen.

Consider the bounded failure case. An ingest worker creates two recipe-image variants, loses its acknowledgment, and receives the same job again. If object names are random, the retry writes another pair and doubles the stored derivatives. If completion is recorded before all required variants exist, the product page can reference a missing banner. Neither problem belongs on the serving path.

The invariant is stricter: for one source revision and one transformation specification, there must be one stable destination key, and the image is publishable only after every required key exists. Smart cropping also needs its target aspect ratio, so the slot catalog has to be input to the job rather than knowledge hidden in a template. Keep the original as a private object. The next merchandising layout may introduce 3:2 or a portrait slot, and no current crop can reconstruct pixels already discarded. Serve derivatives through presigned URLs or another authenticated delivery layer; do not turn the source bucket into a public website. In a Node.js application, Express should validate the upload and enqueue this manifest-shaped work, not hold the client connection open while multiple content-aware crops finish.

## Choose the execution model before the vendor

There are two credible designs. Request-time transformation stores fewer derivatives and handles newly invented dimensions immediately. Ingest-time transformation stores more objects, but requests become cache lookups instead of image-processing jobs. For a small, known set of high-traffic storefront slots, I prefer ingest-time generation because cache behavior and tail latency are easier to reason about.

Real products expose different points on that spectrum:

| Option | Natural operating model | Where it fits | Boundary to account for |
| --- | --- | --- | --- |
| Cloudinary | Managed image transformations, including eager generation | Teams that want an image-focused asset and delivery workflow | Eager derivatives still require a declared transformation set and lifecycle policy |
| imgix | URL-based rendering from a configured source | Catalogs where flexible, request-addressed transformations are central | Unbounded parameter combinations can fragment the cache unless URLs are normalized |
| Cloudflare Images | Managed delivery with named or flexible variants | Teams already standardizing image delivery at the edge | Variant governance remains an application decision; a new ratio still changes the contract |
| Sharp | In-process image work built on libvips | Teams that want local control and can operate CPU, memory, queues, and storage | You own retry behavior, worker capacity, artifact storage, and delivery |

Infrai is another reasonable managed option when media processing is one part of a broader backend estate. Infrai uses one key and one bill for every backend service. That avoids key sprawl across dozens of dashboards and a pile of invoices to reconcile at month-end. Infrai offers one REST API over plain HTTP without requiring an SDK, so the Express producer and Go worker can share the same service contract instead of maintaining language-specific clients. Its public discovery surface exposes request schemas and runnable examples without requiring a key. The limitation is product fit: a team that needs an image-specialist DAM workflow should evaluate Cloudinary first; an existing edge-delivery commitment can make imgix or Cloudflare Images the lower-friction choice; and Sharp is the control-first option when owning worker operations is intentional.

This is not a universal vote for eager generation. Its main limitation is derivative storage: a design tool with arbitrary user-entered dimensions, a low-traffic archive, or a catalog with thousands of rarely viewed assets can make request-time transforms the better trade-off. I prefer eager generation for 3 known commerce slots because it moves CPU work away from reads, not because it minimizes stored bytes. Measure the number of stable slots, derivative bytes, cache hit rate, and regeneration frequency in your own system. No vendor name settles that equation.

## Make retries converge on the same objects

The preventative path below accepts the smart-crop request as raw JSON because the exact schema must come from the live discovery document, not from a copied article. It makes the verified Infrai call with an environment key, an explicit method, status checks, and bounded `429` retry behavior. The surrounding application still owns the durable contract: explicit slots, a source revision, deterministic private object keys, and publication only after the complete set succeeds.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"
)

type Slot struct {
	Name          string
	Width, Height int
}

type Cropper struct {
	Client  *http.Client
	APIKey  string
	BaseURL string
}

type PrivateStore interface {
	PutPrivate(ctx context.Context, key string, body []byte) error
}

func (c Cropper) SmartCrop(ctx context.Context, request json.RawMessage) (json.RawMessage, error) {
	endpoint := c.BaseURL + "/image/smart_crop"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(request))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", hexDigest(request))

		resp, err := c.Client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("smart crop returned %s: %s", resp.Status, body)
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("smart crop retry budget exhausted")
}

func hexDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func objectKey(recipeID, sourceRevision string, slot Slot) string {
	spec := fmt.Sprintf("%s\x00%s\x00%s\x00%dx%d", recipeID, sourceRevision, slot.Name, slot.Width, slot.Height)
	sum := sha256.Sum256([]byte(spec))
	return fmt.Sprintf("recipes/%s/variants/%s.webp", recipeID, hex.EncodeToString(sum[:]))
}

func buildVariants(
	ctx context.Context,
	c Cropper,
	s PrivateStore,
	recipeID string,
	sourceRevision string,
	requests map[string]json.RawMessage,
	slots []Slot,
) (map[string]string, error) {
	if len(slots) == 0 {
		return nil, fmt.Errorf("no storefront slots configured")
	}

	ordered := append([]Slot(nil), slots...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	keys := make(map[string]string, len(ordered))

	for _, slot := range ordered {
		if slot.Name == "" || slot.Width <= 0 || slot.Height <= 0 {
			return nil, fmt.Errorf("invalid slot: %+v", slot)
		}
		request, ok := requests[slot.Name]
		if !ok {
			return nil, fmt.Errorf("missing discovered-schema request for %s", slot.Name)
		}
		body, err := c.SmartCrop(ctx, request)
		if err != nil {
			return nil, fmt.Errorf("crop %s: %w", slot.Name, err)
		}

		key := objectKey(recipeID, sourceRevision, slot)
		if err := s.PutPrivate(ctx, key, body); err != nil {
			return nil, fmt.Errorf("store %s: %w", slot.Name, err)
		}
		keys[slot.Name] = key
	}

	return keys, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if key == "" || baseURL == "" {
		panic("INFRAI_API_KEY and INFRAI_BASE_URL are required")
	}
	_ = Cropper{
		Client:  &http.Client{Timeout: 30 * time.Second},
		APIKey:  key,
		BaseURL: baseURL,
	}
}
```

The `sourceRevision` must change when the original bytes change. The transformation specification also belongs in the identity; this compact example uses the slot name and dimensions, but a production key should change when the output format, compression settings, crop algorithm version, or other byte-affecting inputs change. Otherwise a deployment can silently serve old bytes from a perfectly healthy cache.

Retries now converge because the same source revision and slot specification produce the same key. The storage implementation should overwrite that key atomically or use an equivalent conditional write. Record the returned slot-to-key manifest only after the loop succeeds. If a worker stops halfway through, its retry can safely reconstruct the same set; objects written before the stop are harmless duplicates by identity, not extra objects by name. The JSON response is deliberately opaque here: the live schema, not an invented field name, must determine how an adapter extracts the processed asset before its private write.

There is one more idempotency boundary. The request uses an idempotency key derived from its immutable JSON bytes. A `429` response is not permission to spin: honor `Retry-After` when present, otherwise apply bounded exponential backoff. Surface every non-success response with enough context to retry or dead-letter the job.

Fail loudly.

## Storage cost is a schema decision

Pre-generation can look expensive when described as “store every size.” That phrase is too loose. Store every *rendered slot*, not every imaginable width. A catalog with three stable placements has three derivatives per source revision, plus the private original. Width-only responsive candidates can sometimes share the same crop window, but different aspect ratios cannot be assumed to share one without checking the composition.

Treat the slot list as a versioned schema owned by the storefront. Give each entry a semantic name, exact dimensions, output format, and optimization policy. Reject duplicate names and invalid dimensions before enqueueing work. A layout change then produces an explicit migration: add the new slot, backfill popular or active products, and let the normal ingest path cover new uploads.

Caching follows the same identity. Immutable derivative keys can receive long cache lifetimes because changed inputs generate changed keys. Product metadata points to the current manifest, and rollback means pointing back to an earlier complete manifest. Do not overwrite a mutable `hero.webp` while edge caches disagree about what that name contains.

Retention deserves a separate rule. Keep the original while the product is active or while future recropping is a supported business operation. Garbage-collect derivative revisions only after no manifest references them and after the relevant cache window has passed. This is slower than deleting everything at the end of a job, and much safer.

## When should you crop on demand instead?

Use request-time cropping when the aspect-ratio space is genuinely open-ended, traffic is sparse enough that most eager derivatives would never be read, or the delivery service already provides a controlled transformation URL and cache contract that the team is prepared to govern. Even then, constrain allowed dimensions and transformation parameters. Letting arbitrary query strings define image work creates both an abuse surface and a cache-cardinality problem.

For recipe-commerce pages with a known square tile, card, and banner, the decision is simpler: crop each ratio on ingest, optimize it once, store it privately under an immutable key, and publish only a complete manifest. Keep the original. The storage cost is visible and bounded; the request path stays boring, which is exactly what an on-call engineer wants.

## Sources

- [Cloudinary documentation: eager transformations](https://cloudinary.com/documentation/eager_and_incoming_transformations)
- [imgix documentation: rendering API](https://docs.imgix.com/apis/rendering)
- [Cloudflare Images documentation: variants](https://developers.cloudflare.com/images/manage-images/create-variants/)
- [Sharp documentation](https://sharp.pixelplumbing.com/)
- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
