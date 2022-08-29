// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package limits

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/gate"
	"gopkg.in/fsnotify.v1"
)

// Limiter is responsible for managing the configuration and initialization of
// different types that apply limits to the Receive instance.
type Limiter struct {
	sync.RWMutex
	requestLimiter      requestLimiter
	writeGate           gate.Gate
	registerer          prometheus.Registerer
	configPathOrContent fileContent
	logger              log.Logger
	// TODO: extract active series limiting logic into a self-contained type and
	// move it here.
}

type requestLimiter interface {
	AllowSizeBytes(tenant string, contentLengthBytes int64) bool
	AllowSeries(tenant string, amount int64) bool
	AllowSamples(tenant string, amount int64) bool
}

type fileContent interface {
	Content() ([]byte, error)
	Path() string
}

// NewLimiter creates a new *Limiter given a configuration and prometheus
// registerer.
func NewLimiter(configFile fileContent, reg prometheus.Registerer, logger log.Logger) (*Limiter, error) {
	limiter := &Limiter{
		writeGate:      gate.NewNoop(),
		requestLimiter: &noopRequestLimiter{},
		registerer:     reg,
		logger:         logger,
	}

	if configFile == nil {
		return limiter, nil
	}

	limiter.configPathOrContent = configFile
	if err := limiter.loadConfig(); err != nil {
		// TODO: wrap error
		return nil, err
	}

	return limiter, nil
}

// StartConfigReloader starts the automatic configuration reloader based off of
// the file indicated by pathOrContent. It starts a Go routine in the given
// *run.Group.
// TODO: add some metrics to the reloader
func (l *Limiter) StartConfigReloader(ctx context.Context, errChan chan<- error) error {
	if l.configPathOrContent == nil {
		return nil
	}
	if l.configPathOrContent.Path() == "" {
		return nil
	}

	// TODO: isolate this logic into a separate type for reusability
	path := l.configPathOrContent.Path()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrap(err, "creating file watcher")
	}
	if err := watcher.Add(path); err != nil {
		return errors.Wrapf(err, "adding path %s to file watcher", path)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-watcher.Events:
			// fsnotify sometimes sends a bunch of events without name or operation.
			// It's unclear what they are and why they are sent - filter them out.
			if event.Name == "" {
				break
			}
			// Everything but a CHMOD requires rereading.
			// If the file was removed, we can't read it, so skip.
			if event.Op^(fsnotify.Chmod|fsnotify.Remove) == 0 {
				break
			}
			if err := l.loadConfig(); err != nil {
				errChan <- err
				errMsg := fmt.Sprintf("error reloading tenant limits config from %s", l.configPathOrContent.Path())
				l.logger.Log("msg", errMsg, "err", err)
			}
			l.logger.Log("msg", "config reloaded")
		case err := <-watcher.Errors:
			if err != nil {
				errChan <- err
			}
		}
	}
}

func (l *Limiter) loadConfig() error {
	config, err := ParseLimitConfigContent(l.configPathOrContent)
	if err != nil {
		return err
	}
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
	return nil
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
func ParseLimitConfigContent(limitsConfig fileContent) (*RootLimitsConfig, error) {
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

type noopConfigContent struct{}

var _ fileContent = (*noopConfigContent)(nil)

func (n noopConfigContent) Content() ([]byte, error) {
	return nil, nil
}

func (n noopConfigContent) Path() string {
	return ""
}

// NewNoopConfig creates a no-op config content (no configuration).
func NewNoopConfig() noopConfigContent {
	return noopConfigContent{}
}

type staticPathContent struct {
	content []byte
	path    string
}

var _ fileContent = (*staticPathContent)(nil)

func (t *staticPathContent) Content() ([]byte, error) {
	return t.content, nil
}

func (t *staticPathContent) Path() string {
	return t.path
}

// NewStaticPathContent creates a new content that can be used to serve a static configuration. It copies the
// configuration from `fromPath` into `destPath` to avoid confusion with file watchers.
func NewStaticPathContent(fromPath string) (*staticPathContent, error) {
	content, err := os.ReadFile(fromPath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not load test content: %s", fromPath)
	}
	return &staticPathContent{content, fromPath}, nil
}

func (t *staticPathContent) rewriteConfig(newContent []byte) error {
	t.content = newContent
	return os.WriteFile(t.path, newContent, 0666)
}
