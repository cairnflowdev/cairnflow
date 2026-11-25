// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"net/http"
	"time"
)

// Option configures Connect.
type Option interface {
	applyConnect(*config)
}

// StreamOption configures ReadStream.
type StreamOption interface {
	applyStream(*streamConfig)
}

// TargetOption configures a service, object, workflow or promise handle, and the schedules
// calls.
type TargetOption interface {
	applyTarget(*targetConfig)
}

// CallOption configures Call, Send and RunWorkflow.
type CallOption interface {
	applyCall(*callConfig)
}

// AttachOption configures Attach.
type AttachOption interface {
	applyAttach(*attachConfig)
}

// TriggerOption configures Schedules.Trigger.
type TriggerOption interface {
	applyTrigger(*triggerConfig)
}

type config struct {
	header       http.Header
	hc           *http.Client
	followLeader bool
	maxAttempts  int
	maxElapsed   time.Duration
	attachWait   time.Duration
	timeout      time.Duration
}

type streamConfig struct {
	header       http.Header
	hc           *http.Client
	followLeader bool
	from         int64
	retries      int
}

type targetConfig struct {
	scope string
}

type keyMode int

const (
	keyAuto keyMode = iota
	keyNone
	keyGiven
)

type callConfig struct {
	keyMode    keyMode
	key        string
	limitKey   string
	logLevel   string
	header     http.Header
	timeout    time.Duration
	hasTimeout bool
}

type attachConfig struct {
	timeout    time.Duration
	hasTimeout bool
}

type triggerConfig struct {
	scope string
	key   string
	given bool
}

// ConnectionOption is an option of both Connect and ReadStream.
type ConnectionOption struct {
	apply func(header http.Header, hc **http.Client, followLeader *bool)
}

func (o ConnectionOption) applyConnect(c *config) { o.apply(c.header, &c.hc, &c.followLeader) }

func (o ConnectionOption) applyStream(c *streamConfig) { o.apply(c.header, &c.hc, &c.followLeader) }

// WithAPIKey sends the API key (cf_...) as a Bearer token; the tenant is the key's tenant.
func WithAPIKey(key string) ConnectionOption {
	return ConnectionOption{func(h http.Header, _ **http.Client, _ *bool) {
		h.Set("Authorization", "Bearer "+key)
	}}
}

// WithHeader adds a header to every request.
func WithHeader(name, value string) ConnectionOption {
	return ConnectionOption{func(h http.Header, _ **http.Client, _ *bool) {
		h.Add(name, value)
	}}
}

// WithHTTPClient sends requests through hc (default http.DefaultClient). Do not give it a
// Timeout shorter than the attach wait: an attach holds its request open for that long, and
// a stream for up to ten minutes. Use WithRequestTimeout instead.
func WithHTTPClient(hc *http.Client) ConnectionOption {
	return ConnectionOption{func(_ http.Header, c **http.Client, _ *bool) {
		*c = hc
	}}
}

// WithFollowLeader turns off (or back on) following the leader of a 503 and of a stream's
// moved event. On by default.
func WithFollowLeader(follow bool) ConnectionOption {
	return ConnectionOption{func(_ http.Header, _ **http.Client, f *bool) {
		*f = follow
	}}
}

type connectOption func(*config)

func (f connectOption) applyConnect(c *config) { f(c) }

// WithMaxAttempts caps the attempts of one request (default 8).
func WithMaxAttempts(n int) Option {
	return connectOption(func(c *config) { c.maxAttempts = n })
}

// WithMaxElapsed caps the time of one request, waits included (default 30 s).
func WithMaxElapsed(d time.Duration) Option {
	return connectOption(func(c *config) { c.maxElapsed = d })
}

// WithAttachWait sets the long-poll of each attach (default 25 s): below the 30 s idle cap of
// common load balancers. The server clamps it to --attach-max-wait-ms. Each attach is one
// read at the range's leader.
func WithAttachWait(d time.Duration) Option {
	return connectOption(func(c *config) { c.attachWait = d })
}

// WithRequestTimeout caps one HTTP exchange (default: no cap). An attach holds its request
// open for up to the attach wait, so keep this above it.
func WithRequestTimeout(d time.Duration) Option {
	return connectOption(func(c *config) { c.timeout = d })
}

type streamOption func(*streamConfig)

func (f streamOption) applyStream(c *streamConfig) { f(c) }

// From starts reading a stream at offset (default 0).
func From(offset int64) StreamOption {
	return streamOption(func(c *streamConfig) { c.from = offset })
}

// Retries caps the consecutive failures of a stream reader before it gives up (default 10).
func Retries(n int) StreamOption {
	return streamOption(func(c *streamConfig) { c.retries = n })
}

// ScopeOption puts a handle, a schedule or a trigger in a scope (flow control): the
// /scope/{scope}/... paths. Made by InScope.
type ScopeOption struct{ scope string }

// InScope puts the target in scope (flow control): the /scope/{scope}/... paths.
func InScope(scope string) ScopeOption { return ScopeOption{scope} }

func (o ScopeOption) applyTarget(c *targetConfig) { c.scope = o.scope }

func (o ScopeOption) applyTrigger(c *triggerConfig) { c.scope = o.scope }

// IdempotencyKeyOption sets the Idempotency-Key of a call, a send or a schedule trigger.
// Made by IdempotencyKey.
type IdempotencyKeyOption struct{ key string }

// IdempotencyKey sends key as the Idempotency-Key: the same key (within tenant, scope,
// service, handler and object key) means the same invocation. Without it the client
// generates a random key per call, kept across every attempt of that call.
func IdempotencyKey(key string) IdempotencyKeyOption { return IdempotencyKeyOption{key} }

func (o IdempotencyKeyOption) applyCall(c *callConfig) {
	c.keyMode = keyGiven
	c.key = o.key
}

func (o IdempotencyKeyOption) applyTrigger(c *triggerConfig) {
	c.key = o.key
	c.given = true
}

// TimeoutOption caps the wait for a result. Made by Timeout.
type TimeoutOption struct{ d time.Duration }

// Timeout caps the wait for the result of a call or an attach (default: wait until done).
// When it runs out the call returns *AttachTimeout; the invocation keeps running. Send
// ignores it.
func Timeout(d time.Duration) TimeoutOption { return TimeoutOption{d} }

func (o TimeoutOption) applyCall(c *callConfig) {
	c.timeout = o.d
	c.hasTimeout = true
}

func (o TimeoutOption) applyAttach(c *attachConfig) {
	c.timeout = o.d
	c.hasTimeout = true
}

type callOption func(*callConfig)

func (f callOption) applyCall(c *callConfig) { f(c) }

// NoIdempotencyKey sends the call without an Idempotency-Key: a resend after a lost
// response may then create a second invocation.
func NoIdempotencyKey() CallOption {
	return callOption(func(c *callConfig) {
		c.keyMode = keyNone
		c.key = ""
	})
}

// LimitKey sets x-cairn-limit-key (flow control).
func LimitKey(key string) CallOption {
	return callOption(func(c *callConfig) { c.limitKey = key })
}

// LogLevel sets the invocation's minimum log level (cairnflow-log-level): debug, info,
// warn or error. Any other value is refused with 400 before anything is created.
func LogLevel(level string) CallOption {
	return callOption(func(c *callConfig) { c.logLevel = level })
}

// Header adds a header to one call.
func Header(name, value string) CallOption {
	return callOption(func(c *callConfig) { c.header.Add(name, value) })
}

// Tag adds a user tag (cairnflow-tag: name=value) to the invocation. Tags cannot change
// after creation.
func Tag(name, value string) CallOption {
	return callOption(func(c *callConfig) { c.header.Add("Cairnflow-Tag", name+"="+value) })
}

func callConfigOf(opts []CallOption) *callConfig {
	c := &callConfig{header: http.Header{}}
	for _, o := range opts {
		if o != nil {
			o.applyCall(c)
		}
	}
	return c
}

func targetConfigOf(opts []TargetOption) *targetConfig {
	c := &targetConfig{}
	for _, o := range opts {
		if o != nil {
			o.applyTarget(c)
		}
	}
	return c
}
