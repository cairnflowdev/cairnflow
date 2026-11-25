// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

// LambdaEvent is the part of an AWS Lambda HTTP event (payload format 2.0: a Function URL
// or an API Gateway HTTP API) that [LambdaHandler] reads. It decodes from the event JSON
// as is.
type LambdaEvent struct {
	// RawPath is the path as the client sent it, mount prefix (an API Gateway stage)
	// included: what the request identity JWT signs.
	RawPath        string `json:"rawPath"`
	RawQueryString string `json:"rawQueryString,omitempty"`
	// Headers has lower-case names; Lambda joins repeated headers with commas.
	Headers         map[string]string    `json:"headers,omitempty"`
	Body            string               `json:"body,omitempty"`
	IsBase64Encoded bool                 `json:"isBase64Encoded,omitempty"`
	RequestContext  LambdaRequestContext `json:"requestContext"`
}

// LambdaRequestContext is the `requestContext` of a [LambdaEvent].
type LambdaRequestContext struct {
	HTTP LambdaHTTP `json:"http"`
}

// LambdaHTTP is the `requestContext.http` of a [LambdaEvent].
type LambdaHTTP struct {
	Method string `json:"method"`
}

// LambdaResult is the response [LambdaHandler] returns to Lambda. The body is always
// base64: protocol frames are binary.
type LambdaResult struct {
	StatusCode      int               `json:"statusCode"`
	Headers         map[string]string `json:"headers"`
	Body            string            `json:"body"`
	IsBase64Encoded bool              `json:"isBase64Encoded"`
}

// LambdaHandlerFunc is the handler [LambdaHandler] returns, in the shape `lambda.Start`
// of github.com/aws/aws-lambda-go accepts.
type LambdaHandlerFunc func(ctx context.Context, event LambdaEvent) (LambdaResult, error)

// lambdaPassHeaders are the response headers the runtime reads; the others are dropped.
var lambdaPassHeaders = []string{"content-encoding", "cairn-replay", "cairn-replay-cache", "cairn-replay-held", "cairn-affinity"}

// lambdaRoute finds the endpoint's route at the end of a path: the function may be mounted
// under any prefix (an API Gateway stage, a path of your own).
var lambdaRoute = regexp.MustCompile(`(/discover|/invoke/[^/]+/[^/]+)$`)

// LambdaHandler serves the services passed among opts on AWS Lambda, behind a Function URL
// or an API Gateway HTTP API (payload format 2.0). The SDK has no dependency on the AWS
// libraries: pass the result to `lambda.Start` in your own main package.
//
//	func main() {
//		h, err := cairn.LambdaHandler(orders, cairn.IdentityKeys(kid))
//		if err != nil {
//			log.Fatal(err)
//		}
//		lambda.Start(h) // github.com/aws/aws-lambda-go/lambda
//	}
//
// Register the deployment with the function's base URL: the runtime calls
// `<url>/discover` and `<url>/invoke/{service}/{handler}`, and the handler routes on the end
// of the raw path, so any mount prefix works (the JWT of request identity signs the whole
// path); the Host header plays no part in routing, and one that is not a plain host
// (with `/`, `?`, `#`, `@` or a space) gets 400. Lambda serves REQUEST_RESPONSE only (there is no long-lived stream), so [Bidi] is
// refused. Every turn is a new request carrying the whole journal; the segment cache of
// large values ([ValueCacheBytes]) only lives as long as the function instance.
func LambdaHandler(opts ...ServeOption) (LambdaHandlerFunc, error) {
	// The Lambda defaults go first, so the caller's options override them.
	e, err := newEndpoint(append([]ServeOption{lambdaDefaults()}, opts...))
	if err != nil {
		return nil, err
	}
	return e.serveLambda, nil
}

// lambdaDefaults are the endpoint settings for a platform that keeps no process and no
// affinity between turns: no replay cache (a delta would almost always miss and cost a
// second round trip). They come first, so a caller's own [ReplayCache] (which needs
// [IdentityKeys]) still wins.
func lambdaDefaults() ServeOption {
	return serveOptionFunc(func(c *endpointConfig) {
		c.lambda = true
		c.replayCache = replayOff
	})
}

func lambdaJSON(status int, message string) LambdaResult {
	return LambdaResult{
		StatusCode:      status,
		Headers:         map[string]string{"content-type": "application/json"},
		Body:            base64.StdEncoding.EncodeToString(fmt.Appendf(nil, `{"error":%q}`, message)),
		IsBase64Encoded: true,
	}
}

func (e *endpoint) serveLambda(ctx context.Context, ev LambdaEvent) (res LambdaResult, err error) {
	defer func() {
		// Not from user code (turns recover their handlers): an error of the adapter is a
		// 500, which the runtime retries with backoff.
		if p := recover(); p != nil {
			slog.Error("cairn: the Lambda adapter failed", "panic", fmt.Sprint(p))
			res, err = lambdaJSON(http.StatusInternalServerError, "internal error"), nil
		}
	}()
	method := strings.ToUpper(ev.RequestContext.HTTP.Method)
	if method == "" {
		method = http.MethodPost
	}
	path := ev.RawPath
	if path == "" {
		path = "/"
	}
	m := lambdaRoute.FindStringSubmatch(path)
	if m == nil {
		return lambdaJSON(http.StatusNotFound, "no route for "+method+" "+path), nil
	}
	var body []byte
	if ev.Body != "" && method != http.MethodGet && method != http.MethodHead {
		if ev.IsBase64Encoded {
			if body, err = base64.StdEncoding.DecodeString(ev.Body); err != nil {
				return lambdaJSON(http.StatusBadRequest, "the body is not valid base64"), nil
			}
		} else {
			body = []byte(ev.Body)
		}
	}
	host := ev.Headers["host"]
	if host == "" {
		host = "lambda.local"
	}
	if strings.ContainsAny(host, "/?#@\\") || strings.IndexFunc(host, unicode.IsSpace) >= 0 {
		return lambdaJSON(http.StatusBadRequest, "invalid host header"), nil
	}
	// Routed on the raw path itself, the bytes the JWT signs as aud (with its prefix), never
	// on a URL rebuilt from the client's Host header.
	unescaped, err := url.PathUnescape(m[1])
	if err != nil {
		return lambdaJSON(http.StatusBadRequest, "invalid request path"), nil
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://lambda.local/", bytes.NewReader(body))
	if err != nil {
		return lambdaJSON(http.StatusBadRequest, "invalid request path"), nil
	}
	req.URL = &url.URL{Scheme: "https", Host: host, Path: unescaped, RawPath: m[1]}
	// The path as the client sent it, prefix included: what the JWT signs as aud.
	req.RequestURI = path
	if ev.RawQueryString != "" {
		req.RequestURI += "?" + ev.RawQueryString
	}
	req.Host = host
	req.ContentLength = int64(len(body))
	for k, v := range ev.Headers {
		if !strings.EqualFold(k, "content-length") {
			req.Header.Set(k, v)
		}
	}
	w := &lambdaWriter{header: http.Header{}}
	e.ServeHTTP(w, req)
	if w.status == 0 {
		w.status = http.StatusOK
	}
	headers := map[string]string{"content-type": w.header.Get("Content-Type")}
	if headers["content-type"] == "" {
		headers["content-type"] = "application/octet-stream"
	}
	// A compressed body without its content-encoding would be read as a bare frame.
	for _, h := range lambdaPassHeaders {
		if v := w.header.Get(h); v != "" {
			headers[h] = v
		}
	}
	return LambdaResult{
		StatusCode:      w.status,
		Headers:         headers,
		Body:            base64.StdEncoding.EncodeToString(w.body.Bytes()),
		IsBase64Encoded: true,
	}, nil
}

// lambdaWriter collects a response in memory.
type lambdaWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *lambdaWriter) Header() http.Header { return w.header }

func (w *lambdaWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *lambdaWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}
