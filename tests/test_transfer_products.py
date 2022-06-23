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
            job_type='test-job-type',
            job_id='test-job-0',
            request_time=datetime(1, 1, 1),
            status_code='SUCCEEDED',
            user_id='test-user-id',
            name='test-name-foo',
            files=[{'s3': {'bucket': 'test-source-bucket', 'key': 'test-job-0/test-filename-5A87.zip'}}],
        ),
        hyp3_sdk.Job(
            job_type='test-job-type',
            job_id='test-job-1',
            request_time=datetime(1, 1, 1),
            status_code='SUCCEEDED',
            user_id='test-user-id',
            name='test-name-bar',
            files=[{'s3': {'bucket': 'test-source-bucket', 'key': 'test-job-1/test-filename-C054.zip'}}],
        ),
    ])
    existing_objects = frozenset({
        'test-target-prefix/test-name-foo/test-job-0/test-filename-5A87.ext2',
        'test-target-prefix/test-name-bar/test-job-1/test-filename-C054.ext1',
    })
    expected_objects_to_copy = [
        transfer_products.ObjectToCopy(
            'test-source-bucket',
            'test-job-0/test-filename-5A87.ext1',
            'test-target-prefix/test-name-foo/test-job-0/test-filename-5A87.ext1',
        ),
        transfer_products.ObjectToCopy(
            'test-source-bucket',
            'test-job-0/test-filename-5A87.ext3',
            'test-target-prefix/test-name-foo/test-job-0/test-filename-5A87.ext3',
        ),
        transfer_products.ObjectToCopy(
            'test-source-bucket',
            'test-job-1/test-filename-C054.ext2',
            'test-target-prefix/test-name-bar/test-job-1/test-filename-C054.ext2',
        ),
        transfer_products.ObjectToCopy(
            'test-source-bucket',
            'test-job-1/test-filename-C054.ext3',
            'test-target-prefix/test-name-bar/test-job-1/test-filename-C054.ext3',
        ),
    ]
    assert transfer_products.get_objects_to_copy(
        jobs, existing_objects, 'test-target-prefix', ['.ext1', '.ext2', '.ext3']
    ) == expected_objects_to_copy
