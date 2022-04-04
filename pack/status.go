// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

type Status int

const (
	Unpacked = Status(0)
	Packing  = Status(1)
	Packed   = Status(2)
)
