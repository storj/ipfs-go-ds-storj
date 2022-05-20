// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"storj.io/common/memory"
)

func TestNewChore_DefaultConfig(t *testing.T) {
	chore := NewChore(nil, nil)
	require.Equal(t, DefaultInterval, chore.interval)
	require.Equal(t, DefaultMinSize.Int(), chore.minSize)
	require.Equal(t, DefaultMaxSize.Int(), chore.maxSize)
	require.Equal(t, DefaultMaxBlocks, chore.maxBlocks)
}

func TestNewChore_WithInterval(t *testing.T) {
	for _, tt := range []struct {
		name             string
		interval         time.Duration
		expectedInterval time.Duration
	}{
		{
			name:             "zero interval",
			interval:         0,
			expectedInterval: DefaultInterval,
		},
		{
			// negative interval is valid - disables the packing job
			name:             "negative interval",
			interval:         -1 * time.Minute,
			expectedInterval: -1 * time.Minute,
		},
		{
			name:             "positive interval",
			interval:         1 * time.Hour,
			expectedInterval: 1 * time.Hour,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			chore := NewChore(nil, nil).WithInterval(tt.interval)
			require.Equal(t, tt.expectedInterval, chore.interval)
			require.Equal(t, DefaultMinSize.Int(), chore.minSize)
			require.Equal(t, DefaultMaxSize.Int(), chore.maxSize)
			require.Equal(t, DefaultMaxBlocks, chore.maxBlocks)
		})
	}
}

func TestNewChore_WithPackSize(t *testing.T) {
	for _, tt := range []struct {
		name              string
		minSize           memory.Size
		maxSize           memory.Size
		maxBlocks         int
		expectedMinSize   memory.Size
		expectedMaxSize   memory.Size
		expectedMaxBlocks int
	}{
		{
			name:              "all zeros",
			expectedMinSize:   DefaultMinSize,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "min size > max size",
			minSize:           3 * memory.MiB,
			maxSize:           2 * memory.MiB,
			expectedMinSize:   2 * memory.MiB,
			expectedMaxSize:   2 * memory.MiB,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "min size < max block size",
			minSize:           MaxBlockSize - 1,
			expectedMinSize:   MaxBlockSize,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "max size < max block size",
			maxSize:           MaxBlockSize - 1,
			expectedMinSize:   MaxBlockSize,
			expectedMaxSize:   MaxBlockSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "only min size < default",
			minSize:           DefaultMinSize - 1,
			expectedMinSize:   DefaultMinSize - 1,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "only min size < default min size > default max size",
			minSize:           DefaultMinSize + 1,
			expectedMinSize:   DefaultMinSize + 1,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "only min size > default max size",
			minSize:           2 * DefaultMaxSize,
			expectedMinSize:   DefaultMaxSize,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "only max size > default",
			maxSize:           2 * DefaultMaxSize,
			expectedMinSize:   DefaultMinSize,
			expectedMaxSize:   2 * DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "only max size < default max size > default min size",
			maxSize:           DefaultMaxSize - 1,
			expectedMinSize:   DefaultMinSize,
			expectedMaxSize:   DefaultMaxSize - 1,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "only max size < default min size",
			maxSize:           DefaultMinSize - 1,
			expectedMinSize:   DefaultMinSize - 1,
			expectedMaxSize:   DefaultMinSize - 1,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "zero max blocks",
			minSize:           1 * memory.MiB,
			maxSize:           2 * memory.MiB,
			expectedMinSize:   1 * memory.MiB,
			expectedMaxSize:   2 * memory.MiB,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "zero max blocks",
			minSize:           1 * memory.MiB,
			maxSize:           2 * memory.MiB,
			expectedMinSize:   1 * memory.MiB,
			expectedMaxSize:   2 * memory.MiB,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "negative max blocks",
			maxBlocks:         -1,
			expectedMinSize:   DefaultMinSize,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks,
		},
		{
			name:              "max blocks < default",
			maxBlocks:         1,
			expectedMinSize:   DefaultMinSize,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: 1,
		},
		{
			name:              "max blocks > default",
			maxBlocks:         DefaultMaxBlocks + 1,
			expectedMinSize:   DefaultMinSize,
			expectedMaxSize:   DefaultMaxSize,
			expectedMaxBlocks: DefaultMaxBlocks + 1,
		},
		{
			name:              "all valid values < defaults",
			minSize:           1 * memory.MiB,
			maxSize:           2 * memory.MiB,
			maxBlocks:         1000,
			expectedMinSize:   1 * memory.MiB,
			expectedMaxSize:   2 * memory.MiB,
			expectedMaxBlocks: 1000,
		},
		{
			name:              "all valid values > defaults",
			minSize:           2 * DefaultMinSize,
			maxSize:           2 * DefaultMaxSize,
			maxBlocks:         2 * DefaultMaxBlocks,
			expectedMinSize:   2 * DefaultMinSize,
			expectedMaxSize:   2 * DefaultMaxSize,
			expectedMaxBlocks: 2 * DefaultMaxBlocks,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			chore := NewChore(nil, nil).WithPackSize(tt.minSize.Int(), tt.maxSize.Int(), tt.maxBlocks)
			require.Equal(t, DefaultInterval, chore.interval)
			require.Equal(t, tt.expectedMinSize.Int(), chore.minSize)
			require.Equal(t, tt.expectedMaxSize.Int(), chore.maxSize)
			require.Equal(t, tt.expectedMaxBlocks, chore.maxBlocks)
		})
	}
}
