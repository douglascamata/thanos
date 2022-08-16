// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package limits

import (
	"context"
	"sync"
	"time"

	"github.com/efficientgo/tools/extkingpin"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// Limiter is responsible for managing the configuration and initializtion of
// different types that apply limits to the Receive instance.
type Limiter struct {
	sync.RWMutex
	requestLimiter requestLimiter
	writeGate      gate.Gate
	registerer     prometheus.Registerer
	// TODO: extract active series limiting logic into a self-contained type and
	// move it here.
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

// NewLimiter creates a new *Limiter given a configuration and prometheus
// registerer.
func NewLimiter(limitsConfig *RootLimitsConfig, reg prometheus.Registerer) *Limiter {
	limiter := &Limiter{
		writeGate:      gate.NewNoop(),
		requestLimiter: &noopRequestLimiter{},
		registerer:     reg,
	}

	if limitsConfig == nil {
		return limiter
	}
	limiter.LoadConfig(limitsConfig)

	return limiter
}

// StartConfigReloader starts the automatic configuration reloader based off of
// the file indicated by pathOrContent. It starts a Go routine in the given
// *run.Group.
func (l *Limiter) StartConfigReloader(g *run.Group, pathOrContent *extkingpin.PathOrContent) {
	if pathOrContent == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		return runutil.Repeat(15*time.Second, ctx.Done(), func() error {
			config, err := ParseLimitConfigContent(pathOrContent)
			if err != nil {
				return err
			}
			l.LoadConfig(config)
			return nil
		})
	}, func(err error) {
		cancel()
	})
}

func (l *Limiter) LoadConfig(config *RootLimitsConfig) {
	l.Lock()
	defer l.Unlock()
	maxWriteConcurrency := config.WriteLimits.GlobalLimits.MaxConcurrency
	if maxWriteConcurrency > 0 {
		l.writeGate = gate.New(
			extprom.WrapRegistererWithPrefix(
				"thanos_receive_write_request_concurrent_",
				l.registerer,
			),
			int(maxWriteConcurrency),
		)
	}
	l.requestLimiter = newConfigRequestLimiter(
		l.registerer,
		&config.WriteLimits,
	)
}

// RequestLimiter is a safe getter for the request limiter.
func (l *Limiter) RequestLimiter() requestLimiter {
	l.RLock()
	defer l.RUnlock()
	return l.requestLimiter
}

// WriteGate is a safe getter for the write gate.
func (l *Limiter) WriteGate() gate.Gate {
	l.RLock()
	defer l.RUnlock()
	return l.writeGate
}

// ParseLimitConfigContent parses the limit configuration from the path or
// content.
func ParseLimitConfigContent(limitsConfig *extkingpin.PathOrContent) (*RootLimitsConfig, error) {
	if limitsConfig == nil {
		return &RootLimitsConfig{}, nil
	}
	limitsContentYaml, err := limitsConfig.Content()
	if err != nil {
		return nil, errors.Wrap(err, "get content of limit configuration")
	}
	parsedConfig, err := ParseRootLimitConfig(limitsContentYaml)
	if err != nil {
		return nil, errors.Wrap(err, "parse limit configuration")
	}
	return parsedConfig, nil
}
