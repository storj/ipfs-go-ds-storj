-- AUTOGENERATED BY storj.io/dbx
-- DO NOT EDIT
CREATE TABLE blocks (
	cid TEXT NOT NULL,
	size INTEGER NOT NULL,
	created TIMESTAMP NOT NULL,
	data BLOB,
	deleted INTEGER NOT NULL,
	pack_object TEXT NOT NULL,
	pack_offset INTEGER NOT NULL,
	pack_status INTEGER NOT NULL,
	PRIMARY KEY ( cid )
);