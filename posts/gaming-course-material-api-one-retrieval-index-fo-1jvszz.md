# Gaming Course Material API: One Retrieval Index for Answering Questions

Use one API key at the application boundary, but do not let one request blindly embed every course file and every upstream gaming listing again. The operational recommendation is to separate ingestion from answering, address chunks by normalized content, and make retrieval the only online dependency before generation. For a course that teaches analysts from frequently refreshed marketplace listings, this is the difference between index cost tracking changed evidence and index cost tracking the polling schedule.

**TL;DR:** authenticate once, ingest asynchronously, deduplicate before embedding, retrieve a small evidence set, and require citations in the answer contract. A one-key developer experience can sit over those stages; it should not erase their failure boundaries.

## How should one API retrieve course material for question answering?

Retrieval-augmented generation joins a model's parametric memory with retrieved non-parametric memory. That pattern is useful for course-material Q&A because the answer can be conditioned on selected passages instead of asking the model to carry the changing corpus in its parameters. The original RAG paper establishes the pattern; it does not remove the need to operate ingestion, indexing, retrieval, and generation as distinct work.

Consider a game-economy course built from instructor notes plus item listings aggregated from several marketplaces. Notes change occasionally. Listings may be fetched much more often, and two sources may publish the same description with different whitespace, ordering, or source identifiers. If every fetch becomes a new vector, the index records delivery events rather than knowledge. Cost rises, retrieval fills with near-duplicates, and an answer can cite several copies of one claim as though they were independent evidence.

The safe boundary is a durable ingestion queue. A successful upload means the source revision was accepted, not that every downstream step completed synchronously. Workers normalize, chunk, hash, embed, and publish a manifest. The answering path reads only published manifests. A partially processed revision therefore cannot leak half an index into a student's answer.

Short paths fail loudly.

Retries will happen.

## Make unchanged content free to ingest

Treat source identity, revision identity, and chunk identity as different fields. The source says where material came from. The revision says which observed version is being processed. The chunk hash says whether its retrieval content is new. Mixing these identities is the common route to duplicate work.

For an initial operating policy, use explicit values and record them with the manifest: chunk target 512 tokens, overlap 64 tokens, retrieval depth 8, embedding policy `embed-v3`, and a 2-second retrieval budget. These are configuration choices, not universal optima. They make a rollout inspectable. Change one only after an evaluation set shows why, because changing chunk boundaries can invalidate nearly every hash and trigger a full re-embedding event.

The following Go sketch keeps the idempotency decision close to the write. The interfaces are deliberately generic. A production store must make `PutIfAbsent` atomic, and the manifest should be published only after all referenced chunks are available.

```go
package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type Embedder interface {
	Embed(context.Context, string) ([]float32, error)
}

type Store interface {
	Has(context.Context, string) (bool, error)
	PutIfAbsent(context.Context, string, []float32, map[string]string) error
}

func Index(ctx context.Context, sourceID, text string, e Embedder, s Store) (string, error) {
	normalized := strings.Join(strings.Fields(text), " ")
	sum := sha256.Sum256([]byte("embed-v3\x00" + normalized))
	id := hex.EncodeToString(sum[:])

	exists, err := s.Has(ctx, id)
	if err != nil || exists {
		return id, err
	}
	vector, err := e.Embed(ctx, normalized)
	if err != nil {
		return "", err
	}
	metadata := map[string]string{"source_id": sourceID, "policy": "embed-v3"}
	if err := s.PutIfAbsent(ctx, id, vector, metadata); err != nil {
		return "", err
	}
	return id, nil
}
```

Whitespace normalization is intentionally modest. Lowercasing, punctuation removal, or aggressive markup stripping can merge text whose meaning differs. Exact normalized hashes eliminate repeated work for byte-equivalent retrieval text; semantic deduplication is a separate, riskier policy and needs its own evaluation.

This approach has a real limitation: exact hashes do not catch paraphrases, and overlapping chunks still consume index space. Semantic deduplication can reduce that duplication, but a false merge can hide evidence that a student should see. For a small, static syllabus, a scheduled full rebuild may be easier to operate than manifests, reference counts, and garbage collection. Choose content addressing when repeated delivery and corpus churn justify those moving parts.

There is also a deletion trap. A shared chunk may be referenced by three source manifests. Removing one source must remove that reference, not the shared vector, until no published manifest refers to it. Reference counting or mark-and-sweep both work. Immediate deletion does not.

Publish once.

## Put a cost ledger beside the index

Count decisions, not invoices. For every revision, emit accepted chunks, cache hits, embedding attempts, embedding failures, published chunks, retired references, and bytes of normalized text. Those counters explain index growth without depending on a provider's current pricing. They also expose a parser regression: if one ordinary listing refresh suddenly changes nearly every chunk hash, stop publication and inspect the normalized text.

A useful deployment gate compares the candidate manifest with the current one. Set a review threshold appropriate to the feed, then require an explicit override when changed-chunk ratio or total chunk count crosses it. The threshold is local policy; the invariant is that a surprisingly expensive rebuild cannot publish unnoticed.

Test the gate with a 10,000-chunk synthetic corpus before production traffic. That number is a test fixture, not a capacity claim. Force one unchanged replay, one single-chunk edit, one parser-wide formatting change, and one interrupted publication; the ledger should distinguish all four.

Keep retrieval telemetry equally plain: query ID, manifest version, filters, candidate count, returned chunk IDs, scores, latency, and citation coverage. Do not log the complete student question or retrieved passage by default; course notes and user prompts may contain material that does not belong in broad operational logs. Retention and access controls should be decided before launch.

Measure first.

## Verify answers before shifting traffic

Build a fixed evaluation set from questions instructors can answer against known course passages. Include answerable questions, questions whose evidence spans two chunks, stale-listing questions, and unanswerable questions. Store the expected source identifiers and acceptable abstention behavior. This set checks the whole path after a chunking, embedding, filter, or ranking change.

Deploy a new manifest as a version, never as an in-place mutation. First verify that every manifest reference resolves. Then replay the evaluation set, compare retrieved evidence and citation coverage, and shadow a sample of live queries without serving the candidate answer. Only after those checks should traffic move.

Rollback is a pointer change to the last verified manifest. Keep that manifest and its referenced chunks until the new version has survived the observation window. If answer quality drops, retrieval latency exceeds its budget, or index growth is unexplained, restore the pointer first and investigate second. Re-running ingestion during an incident adds another moving part.

The answer endpoint should return cited chunk IDs and the manifest version alongside generated text. If retrieval finds no adequate evidence, return an explicit abstention rather than letting generation fill the gap. That response may look less magical, but it is much easier to debug and safer for a course whose underlying marketplace data keeps changing.

## Operating rule

The key count is an access-control detail. The durable design decision is whether repeated deliveries can create repeated index work. Use content-addressed chunks, immutable manifests, atomic publication, measurable retrieval, and a pointer-based rollback. This keeps the course Q&A surface simple while preserving the controls needed when multi-source gaming listings refresh at different rates.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
