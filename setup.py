import re

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

    start_result = re.search(r"<!-- START doctoc [^>]+-->", long_description)
    if start_result:
        pos = start_result.span(0)[0]
        before = long_description[:pos]
        remaining = long_description[pos:]
        end_result = re.search(r"<!-- END doctoc [^>]+-->", remaining)
        end_pos = end_result.span(0)[1]
        after = remaining[end_pos:]
        long_description = before + after

__pkginfo__ = {}
with open("s3access/__pkginfo__.py") as fh:
    exec(fh.read(), __pkginfo__)

install_requires = [
    'readstr>=0.5.0',
    'boto3>=1.17.0',  # first version supporting select_object_content (s3 select)
]

async_select_requires = [
    'aiobotocore>=1.3.0',
]

pandas_requires = [
    'pandas>=1.0.0',
    'pyarrow>=3.0.0',
]

tests_require = ['pytest'] + install_requires + async_select_requires + pandas_requires

setuptools.setup(
    name="s3access",
    version=__pkginfo__['__version__'],
    author="Julian Fleischer",
    author_email="tirednesscankill@warhog.net",
    description="Access Parquet files in S3 via S3 Select",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/scravy/s3access",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    extras_require={
        'async': async_select_requires,
        'pandas': pandas_requires,
        'dev': tests_require,
    },
    python_requires='>=3.8',
    tests_require=tests_require,
    install_requires=install_requires,
)
