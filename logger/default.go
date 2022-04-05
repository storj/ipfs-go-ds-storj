// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package logger

import (
	"log"
	"os"
)

// Default is the default stdout logger.
var Default = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
