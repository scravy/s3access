# API


## S3Access: `s3 = S3Access()`

### `s3.select`

### `s3.ls`

### `s3.ls_path`


## S3Path: `p = S3Path("s3://bucket/key/part=value")`

### `p.with_params(foo=7, bar=29)`

👉 `s3://other/key/part=value/foo=7/bar=29`

Appends or replaces the key/value pairs in order as given.


### `p.with_params(foo=7, part='else', bar=29)`

👉 `s3://other/key/part=else/foo=7/bar=29`

If a partition is already mentioned in the path, it is
replaced with the value.


### `p.with_bucket('other')`

👉 `s3://other/key/part=value`

Replaces the bucket name of the S3 url.


### `p.with_key('path')`

👉 `s3://bucket/path`

Replaces the prefix/key/path component of this S3 url.
