// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"context"
	"time"

	"github.com/efficientgo/tools/extkingpin"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type limiter struct {
	requestLimiter requestLimiter
	writeGate      gate.Gate
	config         *RootLimitsConfig
	// TODO: extract active series limiting logic into a self-contained type and
	// move it here.
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

func NewLimiter(limitsConfig *RootLimitsConfig, reg prometheus.Registerer) *limiter {
	limiter := &limiter{
		writeGate:      gate.NewNoop(),
		requestLimiter: &noopRequestLimiter{},
	}

	if limitsConfig == nil {
		return limiter
	}

	limiter.config = limitsConfig

	maxWriteConcurrency := limiter.config.WriteLimits.GlobalLimits.MaxConcurrency
	if maxWriteConcurrency > 0 {
		limiter.writeGate = gate.New(
			extprom.WrapRegistererWithPrefix(
				"thanos_receive_write_request_concurrent_",
				reg,
			),
			int(maxWriteConcurrency),
		)
	}
	limiter.requestLimiter = newConfigRequestLimiter(reg, &limiter.config.WriteLimits)

	return limiter
}

func (l *limiter) StartConfigReloader(g *run.Group, pathOrContent *extkingpin.PathOrContent) {
	if pathOrContent == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Add(func() error {
		return runutil.Repeat(15*time.Second, ctx.Done(), func() error {
			config, err := LoadLimitConfig(pathOrContent)
			if err != nil {
				return err
			}
			l.config = config
			return nil
		})
	}, func(err error) {
		cancel()
	})
}

func LoadLimitConfig(limitsConfig *extkingpin.PathOrContent) (*RootLimitsConfig, error) {
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
