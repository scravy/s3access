from __future__ import annotations

from typing import Optional, Dict


class S3Path:

    def __init__(self, bucket_or_url, key: Optional[str] = None):
        if isinstance(bucket_or_url, S3Path):
            self._bucket = bucket_or_url._bucket
            self._key = bucket_or_url._key
        elif bucket_or_url and key is not None:
            self._bucket = bucket_or_url
            self._key = key
        else:
            self._bucket = ''
            self._key = ''

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def key(self) -> str:
        return self._key

    @property
    def params(self) -> Dict[str, str]:
        return {}

    def with_key(self, new_key) -> S3Path:
        pass

    def with_bucket(self, new_bucket) -> S3Path:
        pass

    def with_params(self, **kwargs) -> S3Path:
        pass

    def __getitem__(self, item):
        return self.params[item]

    def __contains__(self, item):
        return item in self.params

    def __truediv__(self, key) -> S3Path:
        pass
