// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package limits

import (
	"context"
	"github.com/efficientgo/tools/core/pkg/testutil"
	"github.com/go-kit/log"
	"os"
	"path"
	"testing"
	"time"
)

func TestLimiter_StartConfigReloader(t *testing.T) {
	origLimitsFile, err := os.ReadFile(path.Join("testdata", "limits_config", "good_limits.yaml"))
	testutil.Ok(t, err)
	copyLimitsFile := path.Join(t.TempDir(), "limits.yaml")
	testutil.Ok(t, os.WriteFile(copyLimitsFile, origLimitsFile, 0666))

	goodLimits, err := NewStaticPathContent(copyLimitsFile)
	if err != nil {
		t.Fatalf("error trying to save static limit config: %s", err)
	}
	invalidLimitsPath := path.Join("./testdata", "limits_config", "invalid_limits.yaml")
	invalidLimits, err := os.ReadFile(invalidLimitsPath)
	if err != nil {
		t.Fatalf("could not load test content at %s: %s", invalidLimitsPath, err)
	}

	limiter, err := NewLimiter(goodLimits, nil, log.NewLogfmtLogger(os.Stdout))
	testutil.Ok(t, err)

	ctx := context.Background()
	defer ctx.Done()
	errChan := make(chan error)
	go func() {
		err := limiter.StartConfigReloader(ctx, errChan)
		testutil.Ok(t, err)
	}()
	time.Sleep(1 * time.Second)
	testutil.Ok(t, goodLimits.rewriteConfig(invalidLimits))
	testutil.NotOk(t, <-errChan)
}
