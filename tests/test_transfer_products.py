import os
from datetime import datetime
from unittest.mock import patch, MagicMock, NonCallableMock, call, Mock

import hyp3_sdk
import pytest

import transfer_products


MOCK_ENV = {
    'HYP3_URL': 'test-url',
    'EARTHDATA_USERNAME': 'test-user',
    'EARTHDATA_PASSWORD': 'test-pass',
    'S3_TARGET_BUCKET': 'target-bucket',
    'S3_TARGET_PREFIX': 'target-prefix',
}

JOBS = hyp3_sdk.Batch([
    hyp3_sdk.Job(
        job_type='job-type',
        job_id='job-0',
        request_time=datetime(1, 1, 1),
        status_code='SUCCEEDED',
        user_id='user-id',
        name='name-foo',
        files=[{
            'filename': 'filename-5A87.zip',
            'url': 'url-base/path/to/filename-5A87.zip',
        }],
    ),
    hyp3_sdk.Job(
        job_type='job-type',
        job_id='job-1',
        request_time=datetime(1, 1, 1),
        status_code='SUCCEEDED',
        user_id='user-id',
        name='name-bar',
        files=[{
            'filename': 'filename-C054.zip',
            'url': 'url-base/path/to/filename-C054.zip',
        }],
    ),
])

EXISTING_OBJECTS = frozenset({
    'target-prefix/name-foo/job-0/filename-5A87.ext2',
    'target-prefix/name-bar/job-1/filename-C054.ext1',
})

EXTENSIONS = ['.ext1', '.ext2', '.ext3']

EXPECTED_OBJECTS_TO_COPY = [
    transfer_products.ObjectToCopy(
        url='url-base/path/to/filename-5A87.ext1',
        target_key='target-prefix/filename-5A87.ext1',
    ),
    transfer_products.ObjectToCopy(
        url='url-base/path/to/filename-5A87.ext2',
        target_key='target-prefix/filename-5A87.ext2'
    ),
    transfer_products.ObjectToCopy(
        url='url-base/path/to/filename-5A87.ext3',
        target_key='target-prefix/filename-5A87.ext3',
    ),
    transfer_products.ObjectToCopy(
        url='url-base/path/to/filename-C054.ext1',
        target_key='target-prefix/filename-C054.ext1'
    ),
    transfer_products.ObjectToCopy(
        url='url-base/path/to/filename-C054.ext2',
        target_key='target-prefix/filename-C054.ext2',
    ),
    transfer_products.ObjectToCopy(
        url='url-base/path/to/filename-C054.ext3',
        target_key='target-prefix/filename-C054.ext3',
    ),
]


@patch('transfer_products.copy_object')
@patch('transfer_products.get_existing_objects')
@patch('transfer_products.EXTENSIONS', EXTENSIONS)
@patch.dict(os.environ, MOCK_ENV, clear=True)
def test_lambda_handler(mock_get_existing_objects: MagicMock, mock_copy_object: MagicMock):
    mock_hyp3 = NonCallableMock(hyp3_sdk.HyP3)
    mock_hyp3.find_jobs.return_value = JOBS

    mock_hyp3_class = Mock()
    mock_hyp3_class.return_value = mock_hyp3

    mock_get_existing_objects.return_value = EXISTING_OBJECTS

    with patch('hyp3_sdk.HyP3', mock_hyp3_class):
        transfer_products.lambda_handler(None, None)

    mock_hyp3_class.assert_called_once_with(api_url='test-url', username='test-user', password='test-pass')
    mock_hyp3.find_jobs.assert_called_once_with(status_code='SUCCEEDED')
    mock_get_existing_objects.assert_called_once_with('target-bucket', 'target-prefix')

    assert mock_copy_object.mock_calls == [call(obj, 'target-bucket') for obj in EXPECTED_OBJECTS_TO_COPY]


@patch.dict(os.environ, {}, clear=True)
def test_lambda_handler_missing_env_var():
    with pytest.raises(transfer_products.MissingEnvVar):
        transfer_products.lambda_handler(None, None)


def test_get_objects_to_copy():
    assert transfer_products.get_objects_to_copy(
        JOBS, EXISTING_OBJECTS, 'target-prefix', EXTENSIONS
    ) == EXPECTED_OBJECTS_TO_COPY
