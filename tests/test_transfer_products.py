import os
from datetime import datetime
from unittest.mock import patch

import hyp3_sdk
import pytest

import transfer_products


@patch.dict(os.environ, {}, clear=True)
def test_lambda_handler_missing_env_var():
    with pytest.raises(transfer_products.MissingEnvVar):
        transfer_products.lambda_handler(None, None)


def test_get_objects_to_copy():
    jobs = hyp3_sdk.Batch([
        hyp3_sdk.Job(
            job_type='job-type',
            job_id='job-0',
            request_time=datetime(1, 1, 1),
            status_code='SUCCEEDED',
            user_id='user-id',
            name='name-foo',
            files=[{'s3': {'bucket': 'source-bucket', 'key': 'job-0/filename-5A87.zip'}}],
        ),
        hyp3_sdk.Job(
            job_type='job-type',
            job_id='job-1',
            request_time=datetime(1, 1, 1),
            status_code='SUCCEEDED',
            user_id='user-id',
            name='name-bar',
            files=[{'s3': {'bucket': 'source-bucket', 'key': 'job-1/filename-C054.zip'}}],
        ),
    ])
    existing_objects = frozenset({
        'target-prefix/name-foo/job-0/filename-5A87.ext2',
        'target-prefix/name-bar/job-1/filename-C054.ext1',
    })
    expected_objects_to_copy = [
        transfer_products.ObjectToCopy(
            'source-bucket',
            'job-0/filename-5A87.ext1',
            'target-prefix/name-foo/job-0/filename-5A87.ext1',
        ),
        transfer_products.ObjectToCopy(
            'source-bucket',
            'job-0/filename-5A87.ext3',
            'target-prefix/name-foo/job-0/filename-5A87.ext3',
        ),
        transfer_products.ObjectToCopy(
            'source-bucket',
            'job-1/filename-C054.ext2',
            'target-prefix/name-bar/job-1/filename-C054.ext2',
        ),
        transfer_products.ObjectToCopy(
            'source-bucket',
            'job-1/filename-C054.ext3',
            'target-prefix/name-bar/job-1/filename-C054.ext3',
        ),
    ]
    assert transfer_products.get_objects_to_copy(
        jobs, existing_objects, 'target-prefix', ['.ext1', '.ext2', '.ext3']
    ) == expected_objects_to_copy


def test_get_source_key():
    assert transfer_products.get_source_key('filename.zip', '_foo.ext') == 'filename_foo.ext'


def test_get_target_key():
    assert transfer_products.get_target_key(
        'job-id/filename.ext', 'job-name', 'job-id', 'target-prefix'
    ) == 'target-prefix/job-name/job-id/filename.ext'
