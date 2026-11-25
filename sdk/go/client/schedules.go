// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// ScheduleTarget is the target of a schedule: service and handler, plus a key for a virtual
// object.
type ScheduleTarget struct {
	Service string `json:"service"`
	Handler string `json:"handler"`
	Key     string `json:"key,omitempty"`
}

// ScheduleDefinition is the body of PUT /schedules/{name}. Exactly one of Cron (5 fields,
// IANA TimeZone, default UTC) and Every (anchored at the Unix epoch).
type ScheduleDefinition struct {
	Cron     string
	TimeZone string
	Every    time.Duration
	Target   ScheduleTarget
	// Input is the JSON input of every run (nil: no input; encoded as in Call).
	Input    any
	LimitKey string
	// Overlap is "skip" (the default for an unkeyed target) or "allow" (the only option for
	// a keyed or scoped target); "" leaves the default.
	Overlap string
	Paused  bool
	Jitter  time.Duration
}

type scheduleSpec struct {
	Cron     string          `json:"cron,omitempty"`
	TimeZone string          `json:"timeZone,omitempty"`
	EveryMs  int64           `json:"everyMs,omitempty"`
	Target   ScheduleTarget  `json:"target"`
	Input    json.RawMessage `json:"input,omitempty"`
	LimitKey string          `json:"limitKey,omitempty"`
	Overlap  string          `json:"overlap,omitempty"`
	Paused   bool            `json:"paused,omitempty"`
	JitterMs int64           `json:"jitterMs,omitempty"`
}

func roundMs(d time.Duration) int64 { return d.Round(time.Millisecond).Milliseconds() }

// ScheduleLast is a recent firing. Status is set only when the run is on the same range as
// the schedule.
type ScheduleLast struct {
	At int64  `json:"at"`
	ID string `json:"id"`
	// Outcome is fired or skipped.
	Outcome string `json:"outcome"`
	Status  string `json:"status,omitempty"`
}

// Schedule is a schedule as the server returns it (Schedule in docs/api/openapi.json).
type Schedule struct {
	Name     string          `json:"name"`
	ID       string          `json:"id"`
	Scope    string          `json:"scope,omitempty"`
	Cron     string          `json:"cron,omitempty"`
	TimeZone string          `json:"timeZone,omitempty"`
	EveryMs  int64           `json:"everyMs,omitempty"`
	Target   ScheduleTarget  `json:"target"`
	Input    json.RawMessage `json:"input,omitempty"`
	LimitKey string          `json:"limitKey,omitempty"`
	Overlap  string          `json:"overlap"`
	Paused   bool            `json:"paused"`
	JitterMs int64           `json:"jitterMs"`
	// NextAt is the next nominal firing time (epoch ms); nil while paused.
	NextAt *int64 `json:"nextAt"`
	// FireAt is the actual firing deadline of NextAt (jitter added).
	FireAt int64 `json:"fireAt,omitempty"`
	// Upcoming is the next 10 firing times (Get only).
	Upcoming  []int64        `json:"upcoming,omitempty"`
	Last      []ScheduleLast `json:"last"`
	Fired     int64          `json:"fired"`
	Missed    int64          `json:"missed"`
	Skipped   int64          `json:"skipped"`
	CreatedAt int64          `json:"createdAt"`
	UpdatedAt int64          `json:"updatedAt"`
	// Created (Put only): a new schedule.
	Created bool `json:"created,omitempty"`
	// Changed (Put only): the definition changed; false means a no-op, NextAt not moved.
	Changed bool `json:"changed,omitempty"`
}

// ScheduleList is the answer of List. Partial means the listing is incomplete; Missing,
// MissingRanges and TruncatedRanges say where.
type ScheduleList struct {
	Schedules       []Schedule `json:"schedules"`
	Partial         bool       `json:"partial"`
	Missing         []string   `json:"missing"`
	MissingRanges   []int64    `json:"missingRanges"`
	TruncatedRanges []int64    `json:"truncatedRanges,omitempty"`
}

// Schedules manages the server-side schedules of the key's tenant.
type Schedules struct {
	c *Client
}

// Schedules is the schedules client.
func (c *Client) Schedules() Schedules { return Schedules{c: c} }

func schedulesBase(scope string) string { return scopePrefix(scope) + "/schedules" }

func scheduleOne(name, scope string) string { return schedulesBase(scope) + "/" + enc(name) }

// Put creates or updates a schedule; the same definition is a no-op (safe to call at app
// startup).
func (s Schedules) Put(ctx context.Context, name string, def ScheduleDefinition, opts ...TargetOption) (*Schedule, error) {
	input, err := encodeValue(def.Input)
	if err != nil {
		return nil, err
	}
	spec := scheduleSpec{
		Cron:     def.Cron,
		TimeZone: def.TimeZone,
		EveryMs:  roundMs(def.Every),
		Target:   def.Target,
		Input:    input,
		LimitKey: def.LimitKey,
		Overlap:  def.Overlap,
		Paused:   def.Paused,
		JitterMs: roundMs(def.Jitter),
	}
	var v Schedule
	if err := s.c.doJSON(ctx, http.MethodPut, scheduleOne(name, targetConfigOf(opts).scope), spec, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Get reads a schedule; nil (and no error) when it does not exist.
func (s Schedules) Get(ctx context.Context, name string, opts ...TargetOption) (*Schedule, error) {
	res, err := s.c.do(ctx, http.MethodGet, scheduleOne(name, targetConfigOf(opts).scope), nil)
	if err != nil {
		return nil, err
	}
	if res.status == http.StatusNotFound {
		return nil, nil
	}
	var v Schedule
	if err := decodeJSON(res, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Delete deletes a schedule; false when it did not exist.
func (s Schedules) Delete(ctx context.Context, name string, opts ...TargetOption) (bool, error) {
	res, err := s.c.do(ctx, http.MethodDelete, scheduleOne(name, targetConfigOf(opts).scope), nil)
	if err != nil {
		return false, err
	}
	if res.status == http.StatusNotFound {
		return false, nil
	}
	var v struct{}
	if err := decodeJSON(res, &v); err != nil {
		return false, err
	}
	return true, nil
}

// Pause pauses a schedule.
func (s Schedules) Pause(ctx context.Context, name string, opts ...TargetOption) (*Schedule, error) {
	var v Schedule
	if err := s.c.doJSON(ctx, http.MethodPost, scheduleOne(name, targetConfigOf(opts).scope)+"/pause", nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Resume resumes a schedule. The next firing is computed from the current time: no
// catch-up firing.
func (s Schedules) Resume(ctx context.Context, name string, opts ...TargetOption) (*Schedule, error) {
	var v Schedule
	if err := s.c.doJSON(ctx, http.MethodPost, scheduleOne(name, targetConfigOf(opts).scope)+"/resume", nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Trigger fires a schedule once right away (ignoring Paused and Overlap, without moving
// NextAt) and returns the run. The same IdempotencyKey means the same run; without one the
// client generates a key, kept across every attempt.
func (s Schedules) Trigger(ctx context.Context, name string, opts ...TriggerOption) (*Invocation, error) {
	o := &triggerConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt.applyTrigger(o)
		}
	}
	key := o.key
	if !o.given {
		k, err := newKey()
		if err != nil {
			return nil, err
		}
		key = k
	}
	h := http.Header{}
	h.Set("Idempotency-Key", key)
	sent, err := s.c.send(ctx, http.MethodPost, scheduleOne(name, o.scope)+"/trigger", nil, h)
	if err != nil {
		return nil, err
	}
	var v struct {
		ID string `json:"id"`
	}
	if err := decodeJSON(sent.res, &v); err != nil {
		return nil, err
	}
	return s.c.Invocation(v.ID), nil
}

// List lists the schedules of the tenant (in a scope, with InScope).
func (s Schedules) List(ctx context.Context, opts ...TargetOption) (*ScheduleList, error) {
	var v ScheduleList
	if err := s.c.doJSON(ctx, http.MethodGet, schedulesBase(targetConfigOf(opts).scope), nil, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
