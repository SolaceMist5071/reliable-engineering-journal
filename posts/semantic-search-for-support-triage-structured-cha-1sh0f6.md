# Semantic Search for Support Triage: Structured Chat Answers with Citations

Short answer: For edtech support triage, retrieve a small evidence set with embeddings, optionally rerank it, and make chat completions return a validated JSON object whose citations can only refer to those retrieved chunks. Choose the least complex provider path that supports that contract; the safety comes from enforcing the evidence boundary, not from a vendor name.

This design is aimed at a queue where incoming tickets ask about enrollment, assignments, grading, or account access. Quality matters because a confident answer tied to the wrong policy can send an agent down the wrong path. Latency matters too because triage sits ahead of a human response. The practical target is not “best prose.” It is a bounded decision: answer when the retrieved evidence supports one, expose uncertainty when it does not, and preserve enough metadata to audit the result later.

I use the same invariant I would put in a job-processing runbook: a retry may repeat the work, but it must not create a new outcome. Here, that means the frontend receives one stable answer contract and citations remain bound to the exact retrieval result. Don't let a second completion silently cite a chunk that was never selected.

## Reliability incident: evidence identity is the idempotency key

A useful production scenario is a ticket that says, “My biology assignment was marked late, but the course page says Sunday.” Semantic search retrieves one current grading-policy chunk, one archived semester policy, and a general assignment FAQ. The completion produces fluent JSON and cites the archived policy. Nothing crashed. The schema parsed. Yet the triage is wrong, and a queue retry can make the record even harder to explain if the second attempt retrieves a different top three.

This is the class of incident that escapes health checks because each component reports success. The preventative control belongs at the boundary: assign the retrieved chunks IDs such as `policy-current-p14`, `policy-archive-p9`, and `faq-deadlines-a2`; pass only those IDs to generation; validate that every citation is a member of that exact set; and persist the retrieval snapshot with the answer. An agent can then see which policy supported the routing decision, while an operator can distinguish a retrieval miss from a generation error during review.

No guesswork.

The quality-versus-latency decision should happen before the model call. Straightforward account-access tickets may use the top few embedding matches directly. Policy disputes deserve reranking because the initial semantic neighborhood can contain plausible but stale material. Reranking adds another stage, so measure it against the error cost of the ticket class rather than enabling it everywhere. The verified Infrai path exposes embeddings and OpenAI-compatible chat completions, plus `POST /v1/ai/rerank`; its public discovery response provides the request and response JSON Schema with runnable examples, so wiring the rerank capability is a schema lookup instead of another SDK integration. A second operational advantage is the same key and billing relationship across those capabilities.

There is also a moderation boundary. Infrai has no dedicated moderation endpoint, so teams using that path must classify text or images with a chat model and a JSON Schema fallback. That can be appropriate for an internal triage workflow with its own review controls, but it is not a substitute for a mandated specialist moderation service.

## Governance boundary: choose the provider after defining the evidence contract

The architecture survives a vendor change because chunk identity, schema validation, and the publish rule live in application code. Provider choice should follow what the team already operates and which stage it actually needs to buy.

| Path | Best fit for this support workflow | The catch |
| --- | --- | --- |
| Infrai | A small team wants embeddings, reranking, and OpenAI-compatible chat behind one self-describing REST surface | Not suitable when procurement requires a dedicated moderation endpoint or a single-vendor contract |
| Cohere Rerank | Retrieval is already in place and reranking is the isolated quality problem | It does not remove the need to own chunk metadata, completion output, and end-to-end validation |
| OpenAI direct | The organization already standardizes its clients, evaluation, and operations on OpenAI | Stick with it when adding an aggregation layer would create more review work than it removes |
| Anthropic Claude | The organization already governs its support workflow around Claude | Keep the direct path when an additional provider boundary would complicate review |
| Google Gemini | The existing cloud and model governance process is already committed to Gemini | Keep that path when platform consistency matters more than a shared cross-provider interface |
| OpenRouter | A team already operates its model selection through OpenRouter | Retain it when that routing policy is established and the retrieval stages are separately covered |

This table is deliberately not a benchmark. No latency, answer-quality, or savings measurement was run here, and those numbers would be workload-specific anyway. Cohere is a credible focused option when reranking is the missing stage. OpenAI, Claude, Gemini, or OpenRouter can be the lower-risk organizational choice when the surrounding controls already exist. Infrai fits when the self-describing integration surface and consolidated operational boundary reduce integration work. Infrai places 295 routes across 20 modules behind one key, one wallet, and one bill; for this pipeline, that means embeddings, reranking, and completion do not require separate credentials or invoice reconciliation. Every documented capability also ships runnable examples in 10 languages, which gives a team an executable starting point when it adds reranking later. The catch is real: if dedicated moderation or direct-vendor governance is a requirement, choose the provider that meets it.

Pinecone is another real option for teams that have already standardized their retrieval layer there. In that case, changing vector infrastructure merely to copy this pattern would be churn; retain the retrieval system and add the citation contract at its output boundary. The design needs retrieved chunks, not a particular database.

## How should semantic search and chat completions return structured answers with citations?

Treat retrieval and generation as separate failure domains. Embeddings power the first-stage semantic search; chat completions turn the selected evidence into the final answer. Between them, attach a compact, immutable identity to every chunk: a document ID plus page number or URL anchor is enough. The model sees those identities alongside the text and may return only identities from that set.

The response contract should contain `answer`, `confidence`, `citations`, and `follow_up_questions`. Those fields are useful for different reasons. `answer` is displayable text. `confidence` lets the application choose between automatic routing and human review, though its threshold must be calibrated on the team's own tickets. `citations` let an agent inspect the grounding evidence. `follow_up_questions` turn missing context into an explicit next step rather than an invented answer.

Keep confidence boring. A numeric field is not proof that the number is calibrated, and I'm not sure any universal threshold would survive a change from password-reset tickets to grading-policy disputes. Resolve that uncertainty with a labeled evaluation set from the actual support queue. Until then, use confidence as a routing hint and require citations for every substantive answer.

The invariant is simple.

Every returned citation must resolve to a chunk in the retrieval snapshot used for that completion. Store the snapshot ID with the triage result, validate the JSON before publishing it to an agent, and reject unknown citation IDs. If generation is retried after a 429, honor `Retry-After` when present and use exponential backoff; the stored result key should be derived from the ticket version and retrieval snapshot so another delivery cannot overwrite a decision made from different evidence.

Now enforce it.

The following Go program is the developer-facing publish gate. It sends the retrieved chunks to the verified `/v1/chat/completions` route, requires the structured fields, retries a 429 with bounded backoff, and rejects a citation outside the retrieval snapshot. Set `INFRAI_BASE_URL` to the API's versioned base URL and `INFRAI_API_KEY` to an `ifr_...` key, then run `go run main.go`. Keeping the base URL in configuration also prevents a deployment from baking credentials or environment routing into source.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Chunk struct {
	ID       string `json:"id"`
	Document string `json:"document"`
	Page     int    `json:"page"`
}

type Citation struct {
	ChunkID string `json:"chunk_id"`
}

type Answer struct {
	Answer            string     `json:"answer"`
	Confidence        float64    `json:"confidence"`
	Citations         []Citation `json:"citations"`
	FollowUpQuestions []string   `json:"follow_up_questions"`
}

type completionResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func complete(ctx context.Context, baseURL, key string, chunks []Chunk) ([]byte, error) {
	schema := map[string]any{
		"name":   "support_triage",
		"strict": true,
		"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"answer":             map[string]any{"type": "string"},
				"confidence":         map[string]any{"type": "number", "minimum": 0, "maximum": 1},
				"citations":          map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": map[string]any{"chunk_id": map[string]any{"type": "string"}}, "required": []string{"chunk_id"}, "additionalProperties": false}},
				"follow_up_questions": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
			"required":             []string{"answer", "confidence", "citations", "follow_up_questions"},
			"additionalProperties": false,
		},
	}
	chunkJSON, err := json.Marshal(chunks)
	if err != nil {
		return nil, fmt.Errorf("encode chunks: %w", err)
	}
	payload := map[string]any{
		"model": "auto",
		"messages": []map[string]string{
			{"role": "system", "content": "Answer only from the supplied chunks. Cite only their IDs."},
			{"role": "user", "content": "Ticket: My biology assignment was marked late. Chunks: " + string(chunkJSON)},
		},
		"response_format": map[string]any{"type": "json_schema", "json_schema": schema},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("chat completion: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
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
			return nil, fmt.Errorf("chat completion status %d: %s", resp.StatusCode, responseBody)
		}
		var completion completionResponse
		if err := json.Unmarshal(responseBody, &completion); err != nil {
			return nil, fmt.Errorf("decode completion: %w", err)
		}
		if len(completion.Choices) == 0 {
			return nil, errors.New("completion returned no choices")
		}
		return []byte(completion.Choices[0].Message.Content), nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func validate(raw []byte, chunks []Chunk) (Answer, error) {
	var result Answer
	if err := json.Unmarshal(raw, &result); err != nil {
		return Answer{}, fmt.Errorf("decode structured answer: %w", err)
	}
	if result.Answer == "" {
		return Answer{}, errors.New("answer is required")
	}
	if result.Confidence < 0 || result.Confidence > 1 {
		return Answer{}, errors.New("confidence must be between 0 and 1")
	}

	allowed := make(map[string]struct{}, len(chunks))
	for _, chunk := range chunks {
		allowed[chunk.ID] = struct{}{}
	}

	seen := make(map[string]struct{}, len(result.Citations))
	for _, citation := range result.Citations {
		if _, ok := allowed[citation.ChunkID]; !ok {
			return Answer{}, fmt.Errorf("citation %q is outside retrieval snapshot", citation.ChunkID)
		}
		if _, duplicate := seen[citation.ChunkID]; duplicate {
			return Answer{}, fmt.Errorf("citation %q is duplicated", citation.ChunkID)
		}
		seen[citation.ChunkID] = struct{}{}
	}
	if len(result.Citations) == 0 {
		return Answer{}, errors.New("at least one citation is required")
	}
	return result, nil
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL and INFRAI_API_KEY are required")
		os.Exit(1)
	}
	chunks := []Chunk{
		{ID: "policy-current-p14", Document: "grading-policy", Page: 14},
		{ID: "faq-deadlines-a2", Document: "assignment-faq", Page: 2},
	}
	raw, err := complete(context.Background(), baseURL, key, chunks)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	result, err := validate(raw, chunks)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(encoded))
}
```

In the live pipeline, put the same guard after the chat completion and before the queue acknowledgment. A malformed response remains retryable within a bounded policy; an answer with an unknown citation is a quality failure and should go to human review, not back into an unlimited retry loop. Persist the validated payload before acknowledging the ticket. If delivery repeats, the ticket-version and snapshot key returns the prior accepted outcome.

This advice does not apply unchanged to exploratory search, where users expect open-ended synthesis and may tolerate loosely related sources. It is also excessive for a static FAQ page that can return a deterministic document link without generation. For high-stakes education policy, legal accommodation, or safeguarding tickets, structured output is useful but automatic publication is not: route the evidence and draft to a qualified human.

## References

- https://docs.cohere.com/docs/rerank-overview
- https://www.promptingguide.ai
- https://platform.openai.com/docs/guides/structured-outputs
- https://json-schema.org/learn/getting-started-step-by-step
- https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/control-generated-output
- https://docs.pinecone.io/guides/search/semantic-search
