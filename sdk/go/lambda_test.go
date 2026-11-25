// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package cairn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cairnflowdev/cairnflow/sdk/go/internal/identity"
)

// functionURLEvent is a Function URL event as Lambda delivers it (payload format 2.0),
// trimmed of the fields the SDK does not read but keeping their shape.
const functionURLEvent = `{
  "version": "2.0",
  "routeKey": "$default",
  "rawPath": "/prod/cairn/invoke/Echo/hello",
  "rawQueryString": "",
  "headers": {"host": "abc.lambda-url.eu-west-1.on.aws", "content-type": "application/vnd.cairnflow.invocation.v1",
    "x-amzn-trace-id": "Root=1-0"},
  "requestContext": {"accountId": "123456789012", "http": {"method": "POST", "path": "/prod/cairn/invoke/Echo/hello",
    "protocol": "HTTP/1.1", "sourceIp": "203.0.113.1"}, "stage": "$default"},
  "body": "BODY",
  "isBase64Encoded": true
}`

func lambdaEvent(t *testing.T, method, path string, headers map[string]string, body []byte) LambdaEvent {
	t.Helper()
	ev := LambdaEvent{RawPath: path, Headers: map[string]string{"host": "abc.lambda-url.eu-west-1.on.aws"}}
	ev.RequestContext.HTTP.Method = method
	for k, v := range headers {
		ev.Headers[strings.ToLower(k)] = v
	}
	if body != nil {
		ev.Body, ev.IsBase64Encoded = base64.StdEncoding.EncodeToString(body), true
	}
	return ev
}

func callLambda(t *testing.T, h LambdaHandlerFunc, ev LambdaEvent) (LambdaResult, []byte) {
	t.Helper()
	res, err := h(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if !res.IsBase64Encoded {
		t.Fatal("the result body is not base64")
	}
	b, err := base64.StdEncoding.DecodeString(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, b
}

func TestLambdaServesTheEndpoint(t *testing.T) {
	h, err := LambdaHandler(echoService)
	if err != nil {
		t.Fatal(err)
	}
	// The event JSON as Lambda delivers it decodes into LambdaEvent.
	var ev LambdaEvent
	raw := strings.Replace(functionURLEvent, "BODY", base64.StdEncoding.EncodeToString(startBody([]byte{1}, 0)), 1)
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		t.Fatal(err)
	}
	res, b := callLambda(t, h, ev)
	if res.StatusCode != 200 || res.Headers["content-type"] != invocationV1 || kinds(decodeFrames(t, b)) != "Output End" {
		t.Fatalf("invoke: %d %v %x", res.StatusCode, res.Headers, b)
	}
	// The result JSON is what Lambda expects.
	out, _ := json.Marshal(res)
	for _, k := range []string{`"statusCode":200`, `"headers":{`, `"body":"`, `"isBase64Encoded":true`} {
		if !strings.Contains(string(out), k) {
			t.Fatalf("result JSON %s lacks %s", out, k)
		}
	}

	// Discovery, under the same prefix: REQUEST_RESPONSE.
	res, b = callLambda(t, h, lambdaEvent(t, "GET", "/prod/cairn/discover", nil, nil))
	if res.StatusCode != 200 || res.Headers["content-type"] != discoveryV1 || !strings.Contains(string(b), `"protocolMode":"REQUEST_RESPONSE"`) {
		t.Fatalf("discover: %d %s", res.StatusCode, b)
	}
	// Not a route.
	if res, _ := callLambda(t, h, lambdaEvent(t, "POST", "/prod/other", nil, nil)); res.StatusCode != 404 {
		t.Fatalf("other path: %d", res.StatusCode)
	}
	bad := lambdaEvent(t, "POST", "/invoke/Echo/hello", invokeHeaders(1), nil)
	bad.Body, bad.IsBase64Encoded = "not base64!", true
	if res, _ := callLambda(t, h, bad); res.StatusCode != 400 {
		t.Fatalf("bad base64: %d", res.StatusCode)
	}
	// A too new runtime: 415, no frames.
	if res, _ := callLambda(t, h, lambdaEvent(t, "POST", "/invoke/Echo/hello", invokeHeaders(7), startBody([]byte{1}, 0))); res.StatusCode != 415 {
		t.Fatalf("too new: %d", res.StatusCode)
	}
}

func TestLambdaCompression(t *testing.T) {
	h, err := LambdaHandler(echoService)
	if err != nil {
		t.Fatal(err)
	}
	hd := invokeHeaders(1)
	hd["Content-Encoding"] = "gzip"
	hd["Accept-Encoding"] = "gzip"
	res, b := callLambda(t, h, lambdaEvent(t, "POST", "/invoke/Echo/big", hd, gzipped(t, startBody([]byte{1}, 0))))
	// The compressed body keeps its content-encoding, or the runtime would read gzip as a frame.
	if res.StatusCode != 200 || res.Headers["content-encoding"] != "gzip" {
		t.Fatalf("big: %d %v", res.StatusCode, res.Headers)
	}
	plain, err := gunzip(b, maxRequestBody)
	if err != nil {
		t.Fatal(err)
	}
	if kinds(decodeFrames(t, plain)) != "Output End" {
		t.Fatalf("frames %x", plain)
	}
	// Turned off: no compressed answer, and a compressed request is refused.
	h, _ = LambdaHandler(echoService, WithoutCompression())
	if res, _ := callLambda(t, h, lambdaEvent(t, "POST", "/invoke/Echo/big", hd, gzipped(t, startBody([]byte{1}, 0)))); res.StatusCode != 415 {
		t.Fatalf("compression off: %d", res.StatusCode)
	}
}

func TestLambdaRequestIdentity(t *testing.T) {
	key := newSigner(t)
	var ran atomic.Int32
	svc := NewService("Echo", Handler("hello", NoInput(func(Context) (string, error) {
		ran.Add(1)
		return "hi", nil
	})))
	h, err := LambdaHandler(svc, IdentityKeys(key.kid))
	if err != nil {
		t.Fatal(err)
	}
	const host = "abc.lambda-url.eu-west-1.on.aws"
	// The JWT signs the whole path the runtime sent, mount prefix included.
	disc := lambdaEvent(t, "GET", "/prod/discover", key.headers(nil, key.jwt("/prod/discover", identity.DiscoverJTI, nil, host)), nil)
	if res, b := callLambda(t, h, disc); res.StatusCode != 200 || !strings.Contains(string(b), `"requestIdentity":"v1"`) {
		t.Fatalf("signed discover: %d %s", res.StatusCode, b)
	}
	id := []byte{0xab, 0xcd}
	body := startBody(id, 0)
	jti := identity.ExpectedJTI(id, 0)
	for _, c := range []struct {
		name  string
		token string
		want  int
	}{
		{"unsigned", "", 401},
		{"without the prefix", key.jwt("/invoke/Echo/hello", jti, body, host), 401},
		{"another host", key.jwt("/prod/invoke/Echo/hello", jti, body, "elsewhere.example"), 401},
		{"another body", key.jwt("/prod/invoke/Echo/hello", jti, startBody(id, 1), host), 401},
		{"signed", key.jwt("/prod/invoke/Echo/hello", jti, body, host), 200},
	} {
		hd := invokeHeaders(1)
		if c.token != "" {
			hd = key.headers(hd, c.token)
		}
		if res, b := callLambda(t, h, lambdaEvent(t, "POST", "/prod/invoke/Echo/hello", hd, body)); res.StatusCode != c.want {
			t.Fatalf("%s: %d %s", c.name, res.StatusCode, b)
		}
	}
	if ran.Load() != 1 {
		t.Fatalf("the handler ran %d times", ran.Load())
	}
}

func TestLambdaRefusesBidi(t *testing.T) {
	if _, err := LambdaHandler(echoService, Bidi()); err == nil || !strings.Contains(err.Error(), "REQUEST_RESPONSE only") {
		t.Fatalf("err %v", err)
	}
	if _, err := LambdaHandler(); err == nil {
		t.Fatal("no service accepted")
	}
	// The same options make an ordinary HTTP endpoint with Bidi.
	if _, err := newEndpoint([]ServeOption{echoService, Bidi()}); err != nil {
		t.Fatal(err)
	}
}

// Lambda keeps no process and no affinity between turns, so the endpoint does not declare
// the replay cache there (a delta would almost always miss and cost a second round trip),
// as the TypeScript and Python adapters do, even with request identity; an explicit
// ReplayCache option (which needs IdentityKeys) still turns it on.
func TestLambdaDeclaresNoReplayCache(t *testing.T) {
	key := newSigner(t)
	discover := lambdaEvent(t, "GET", "/discover", key.headers(nil, key.jwt("/discover", identity.DiscoverJTI, nil, "")), nil)
	h, err := LambdaHandler(echoService, IdentityKeys(key.kid))
	if err != nil {
		t.Fatal(err)
	}
	res, b := callLambda(t, h, discover)
	if res.StatusCode != 200 || strings.Contains(string(b), "replayCache") {
		t.Fatalf("discover: %d %s", res.StatusCode, b)
	}
	h, err = LambdaHandler(echoService, IdentityKeys(key.kid), ReplayCache(ReplayCacheOptions{}))
	if err != nil {
		t.Fatal(err)
	}
	res, b = callLambda(t, h, discover)
	if res.StatusCode != 200 || !strings.Contains(string(b), `"replayCache":true`) {
		t.Fatalf("discover with ReplayCache: %d %s", res.StatusCode, b)
	}
}

// The adapter routes on the path the JWT signs, never on a URL rebuilt from the client's
// Host header.
//
//  1. The runtime signs a turn for /prod/invoke/Echo/hello (no host claim).
//  2. Someone replays it with Host `x/invoke/Echo/big?`: "https://" + host + path used to
//     parse as the path /invoke/Echo/big.
//
// Must see: the handler named by the signed path, or nothing, never `big`; a host with
// a character no host has is a 400.
func TestLambdaRoutesOnTheSignedPath(t *testing.T) {
	key := newSigner(t)
	var hello, big atomic.Int32
	svc := NewService("Echo",
		Handler("hello", NoInput(func(Context) (string, error) { hello.Add(1); return "hi", nil })),
		Handler("big", NoInput(func(Context) (string, error) { big.Add(1); return "BIG", nil })),
	)
	h, err := LambdaHandler(svc, IdentityKeys(key.kid))
	if err != nil {
		t.Fatal(err)
	}
	id := []byte{0xab, 0xcd}
	body := startBody(id, 0)
	tok := key.jwt("/prod/invoke/Echo/hello", identity.ExpectedJTI(id, 0), body, "")
	for _, host := range []string{"x/invoke/Echo/big?", "x?/invoke/Echo/big", "x#y", "u@x", "x y", "x\ty"} {
		ev := lambdaEvent(t, "POST", "/prod/invoke/Echo/hello", key.headers(invokeHeaders(1), tok), body)
		ev.Headers["host"] = host
		if res, b := callLambda(t, h, ev); res.StatusCode != 400 {
			t.Fatalf("host %q: %d %s", host, res.StatusCode, b)
		}
	}
	if big.Load() != 0 || hello.Load() != 0 {
		t.Fatalf("handlers ran: hello %d, big %d", hello.Load(), big.Load())
	}
	ev := lambdaEvent(t, "POST", "/prod/invoke/Echo/hello", key.headers(invokeHeaders(1), tok), body)
	if res, b := callLambda(t, h, ev); res.StatusCode != 200 || hello.Load() != 1 || big.Load() != 0 {
		t.Fatalf("signed turn: %d %s, hello %d, big %d", res.StatusCode, b, hello.Load(), big.Load())
	}
}
