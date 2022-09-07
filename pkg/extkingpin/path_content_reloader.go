// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package extkingpin

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/go-kit/log/level"

	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

type fileContent interface {
	Content() ([]byte, error)
	Path() string
}

// PathContentReloader starts a file watcher that monitors the file indicated by fileContent.Path() and runs
// reloadFunc whenever a change is detected.
func PathContentReloader(ctx context.Context, fileContent fileContent, logger log.Logger, reloadFunc func()) error {
	filePath := fileContent.Path()
	watcher, err := fsnotify.NewWatcher()
	if filePath == "" {
		level.Debug(logger).Log("msg", "no path detected for config reload")
	}
	if err != nil {
		return errors.Wrap(err, "creating file watcher")
	}
	go func() {
		debounceTime := 100 * time.Millisecond
		lastReload := time.Now().Add(-debounceTime)
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-watcher.Events:
				// fsnotify sometimes sends a bunch of events without name or operation.
				// It's unclear what they are and why they are sent - filter them out.
				if event.Name == "" {
					break
				}
				if path.Base(event.Name) != path.Base(filePath) {
					break
				}
				// Everything but a CHMOD requires rereading.
				// If the file was removed, we can't read it, so skip.
				if event.Op^fsnotify.Chmod == 0 || event.Op^fsnotify.Remove == 0 {
					break
				}
				level.Debug(logger).Log("msg", fmt.Sprintf("change detected for %s", filePath), "eventName", event.Name, "eventOp", event.Op)
				if time.Now().After(lastReload.Add(debounceTime)) {
					reloadFunc()
				}
			case err := <-watcher.Errors:
				level.Error(logger).Log("msg", "watcher error", "error", err)
			}
		}
	}()
	if err := watcher.Add(path.Dir(filePath)); err != nil {
		return errors.Wrapf(err, "adding path %s to file watcher", filePath)
	}
	return nil
}

type staticPathContent struct {
	content []byte
	path    string
}

var _ fileContent = (*staticPathContent)(nil)

// Content returns the cached content.
func (t *staticPathContent) Content() ([]byte, error) {
	return t.content, nil
}

// Path returns the path to the file that contains the content.
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

// Rewrite rewrites the file backing this staticPathContent and swaps the local content cache. The file writing
// is needed to trigger the file system monitor.
func (t *staticPathContent) Rewrite(newContent []byte) error {
	t.content = newContent
	// Write the file to ensure possible file watcher reloaders get triggered.
	return os.WriteFile(t.path, newContent, 0666)
}