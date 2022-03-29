# Storj Datastore for IPFS

This is an implementation of the IPFS datastore interface backed by Storj.

**NOTE:** Plugins only work on Linux and MacOS at the moment. You can track the progress of this issue here: https://github.com/golang/go/issues/19282

## Bundling

The datastore plugin must be compiled and bundled together with go-ipfs. The plugin will be part of the go-ipfs binary after executing these steps:

```bash
# We use go modules for everything.
> export GO111MODULE=on

# Clone go-ipfs.
> git clone https://github.com/ipfs/go-ipfs
> cd go-ipfs

# Checkout the desired release tag of go-ipfs.
> git checkout v0.12.0

# Pull in the datastore plugin (you can specify a version other than latest if you'd like).
> go get github.com/kaloyan-raev/ipfs-go-ds-storj/plugin@latest

# Add the plugin to the preload list.
> echo -e "\nstorjds github.com/kaloyan-raev/ipfs-go-ds-storj/plugin 0" >> plugin/loader/preload_list

# Update the deptree
> go mod tidy

# Now rebuild go-ipfs with the plugin. The go-ipfs binary will be available at cmd/ipfs/ipfs.
> make build

# (Optionally) install go-ipfs. The go-ipfs binary will be copied to ~/go/bin/ipfs.
> make install
```

## Installation

For a brand new ipfs instance (no data stored yet):

1. Bundle the datastore plugin in go-ipfs as described above.
2. Delete `$IPFS_DIR` if it exists.
3. Run `ipfs init --empty-repo` (The `--empty-repo` flag prevents adding doc files to the local IPFS node, which may cause issues before configuring the datastore plugin). 
4. Edit `$IPFS_DIR/config` to include Storj details (see Configuration below).
5. Overwrite `$IPFS_DIR/datastore_spec` as specified below (*Don't do this on an instance with existing data - it will be lost*).

### Configuration

The `config` file should include the following:
```json
{
  "Datastore": {
  ...

    "Spec": {
      "mounts": [
        {
          "child": {
            "type": "storjds",
            "dbURI": "$postgresDBURI",
            "bucket": "$bucketname",
            "accessGrant": "$accessGrant",
            "logFile": "$pathToLogFile"
          },
          "mountpoint": "/blocks",
          "prefix": "storj.datastore",
          "type": "measure"
        },
```
`$postgresDBURI` is the URI to a PostgreSQL database installation. This database is used for local caching of blocks before they are packed and uploaded to the Storj bucket. The database must exists. 

`$bucketname` is a bucket on Storj DCS. It must be created.

`$accessGrant` is an access grant for Storj DCS with full permission on the entire `$bucketname` bucket.

The `logFile` config is optional. If set, it enables logging for this plugin.

If you are configuring a brand new ipfs instance without any data, you can overwrite the `datastore_spec` file with:

```json
{"mounts":[{"bucket":"$bucketname","mountpoint":"/blocks"},{"mountpoint":"/","path":"datastore","type":"levelds"}],"type":"mount"}
```

Otherwise, you need to do a datastore migration.

## Run With Docker

```
docker run --rm -d \
    --network host \
    -e STORJ_DATABASE_URL=<database_url> \
    -e STORJ_BUCKET=<storj_bucket> \
    -e STORJ_ACCESS=<storj_access_grant> \
    -e IPFS_GATEWAY_NO_FETCH=true \
    -e IPFS_GATEWAY_DOMAIN=<gateway_domain_name> \
    -e IPFS_GATEWAY_USE_SUBDOMAINS=false \
    -e IPFS_GATEWAY_PORT=8080 \
    -e IPFS_API_PORT=5001 \
    -e STORJ_LOG_FILE=/app/log/output.log \
    --mount type=bind,source=<log-dir>,destination=/app/log \
    kaloyanraev/ipfs-go-ds-storj
```

`STORJ_DATABASE_URL` can be set to a Postgres or CockroachDB database URL.

`STORJ_BUCKET` must be set to an existing bucket.

`STORJ_ACCESS` must be set to an access grant with full permission to `STORJ_BUCKET`.

`IPFS_GATEWAY_NO_FETCH` determines if the IPFS gateway is open (if set to false) or restricted (if set to true). Restricted gateways serve files only from the local IPFS node. Open gateways search the IPFS network if the file is not present on the local IPFS node.

`IPFS_GATEWAY_DOMAIN` can be set to the domain name assigned to the IPFS gateway. If set, `IPFS_GATEWAY_USE_SUBDOMAINS` determines if to use subdomains resolution style. See https://docs.ipfs.io/concepts/ipfs-gateway/#subdomain for details.

`IPFS_GATEWAY_PORT` can be set to change the IPFS Gateway port from the default 8080.

`IPFS_API_PORT` can be set to change the IPFS HTTP API port from the default 5001.

If `STORJ_LOG_FILE` is not set, the logs are printed to the standard output.

## License

MIT
