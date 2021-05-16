from __future__ import annotations

import re
from typing import Optional, Dict


class S3Path:
    _path_pattern = re.compile(r"([sS]3[aAnN]?:/*)*(?P<bucket>[^/:]+)(/(?P<key>.+)?)?")

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def key(self) -> str:
        return self._key

    @property
    def params(self) -> Dict[str, str]:
        return self._params

    def with_bucket(self, new_bucket) -> S3Path:
        return S3Path(new_bucket, self._key)

    def with_key(self, new_key) -> S3Path:
        return S3Path(self._bucket, new_key)

    def get_url(self, protocol: str = 's3'):
        return f"{protocol}://{self.bucket}/{self.key}"

    def with_params(self, **kwargs) -> S3Path:
        components = []
        applied = set()
        for component in self._key.split('/'):
            if not component:
                continue
            parts = component.split('=', 1)
            if len(parts) == 2:
                key, value = parts
                if key in kwargs:
                    applied.add(key)
                    components.append(f"{key}={kwargs[key]}")
                    continue
            components.append(component)
        for key, value in kwargs.items():
            if key not in applied:
                components.append(f"{key}={value}")
        return self.with_key('/'.join(components))

    def __init__(self, bucket_or_url, key: Optional[str] = None):
        if isinstance(bucket_or_url, S3Path):
            self._bucket = bucket_or_url._bucket
            self._key = bucket_or_url._key
        elif bucket_or_url and key is not None:
            self._bucket = bucket_or_url
            self._key = key
        else:
            result = self._path_pattern.fullmatch(bucket_or_url)
            if not result:
                raise ValueError
            self._bucket = result.group('bucket') or ''
            self._key = result.group('key') or ''
        self._key = self._key.rstrip('/')
        self._params = {}
        for component in self._key.split('/'):
            parts = component.split('=', 1)
            if len(parts) == 2:
                key, value = parts
                self._params[key] = value

    def __getitem__(self, item):
        return self.params[item]

    def __contains__(self, item):
        return item in self.params

    def __truediv__(self, key) -> S3Path:
        return self.with_key(self.key + '/' + key)

    def __eq__(self, other):
        if isinstance(other, S3Path):
            return self._bucket == other._bucket and self._key == other._key
        if isinstance(other, str):
            return self == S3Path(other)
        raise NotImplementedError

    def __hash__(self):
        return hash((self._bucket, self._key))

    def __str__(self):
        return f's3://{self._bucket}/{self._key}'

    def __repr__(self):
        return f'S3Path({str(self)})'
