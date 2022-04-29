// Copyright (C) 2022 Storj Labs, Inc.
// See LICENSE for copying information.

package pack

import "storj.io/ipfs-go-ds-storj/db"

type Status int

const (
	Unpacked = Status(db.UnpackedStatus)
	Packing  = Status(db.PackingStatus)
	Packed   = Status(db.PackedStatus)
)
