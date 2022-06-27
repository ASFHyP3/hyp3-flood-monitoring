import argparse
import os
from dataclasses import dataclass

import boto3
import boto3.s3.transfer
import botocore.exceptions
import hyp3_sdk
import requests
from dotenv import load_dotenv

S3 = boto3.resource('s3')

TARGET_BUCKET = 'hyp3-nasa-disasters'

# TODO decide on appropriate extensions
EXTENSIONS = ['_VV.tif', '_VH.tif', '_rgb.tif', '_dem.tif', '_WM.tif', '.README.md.txt']


@dataclass(frozen=True)
class ObjectToCopy:
    source_bucket: str
    source_key: str
    target_key: str

    # TODO if we migrate to an enterprise test deployment: remove this field
    url: str


class MissingEnvVar(Exception):
    pass


def get_existing_objects(target_prefix: str) -> frozenset[str]:
    return frozenset(obj.key for obj in S3.Bucket(TARGET_BUCKET).objects.filter(Prefix=f'{target_prefix}/'))


def get_objects_to_copy(
        jobs: hyp3_sdk.Batch,
        existing_objects: frozenset[str],
        target_prefix: str,
        extensions: list[str]) -> list[ObjectToCopy]:

    objects_to_copy = []
    for job in jobs:
        assert job.succeeded()
        source_bucket: str = job.files[0]['s3']['bucket']

        zip_key: str = job.files[0]['s3']['key']
        assert zip_key.endswith('.zip')

        zip_url: str = job.files[0]['url']
        assert zip_url.endswith('.zip')

        for ext in extensions:
            source_key = get_source_key(zip_key, ext)
            target_key = get_target_key(source_key, job.name, job.job_id, target_prefix)

            url = get_source_key(zip_url, ext)
            assert url.endswith(source_key)

            if target_key not in existing_objects:
                objects_to_copy.append(
                    ObjectToCopy(source_bucket=source_bucket, source_key=source_key, target_key=target_key, url=url)
                )

    return objects_to_copy


def get_source_key(zip_key: str, ext: str) -> str:
    return zip_key.removesuffix('.zip') + ext


def get_target_key(source_key: str, job_name: str, job_id: str, target_prefix: str) -> str:
    source_prefix, source_basename = source_key.split('/')
    assert source_prefix == job_id
    return '/'.join([target_prefix, job_name, job_id, source_basename])


def copy_objects(objects_to_copy: list[ObjectToCopy], dry_run: bool) -> None:
    for count, obj in enumerate(objects_to_copy, start=1):
        print(
            f'({count}/{len(objects_to_copy)}) '
            f'Copying {obj.source_bucket}/{obj.source_key} to {TARGET_BUCKET}/{obj.target_key}'
        )
        if not dry_run:
            # TODO if we migrate to an enterprise test deployment:
            #  - call copy_object instead of transfer_object
            #  - don't except requests.HTTPError
            try:
                transfer_object(obj)
            except (botocore.exceptions.ClientError, requests.HTTPError) as e:
                print(f'Error copying object: {e}')


def copy_object(obj: ObjectToCopy) -> None:
    chunk_size = 104857600  # 100 MB
    S3.Bucket(TARGET_BUCKET).copy(
        CopySource={'Bucket': obj.source_bucket, 'Key': obj.source_key},
        Key=obj.target_key,
        Config=boto3.s3.transfer.TransferConfig(multipart_threshold=chunk_size, multipart_chunksize=chunk_size)
    )


def transfer_object(obj: ObjectToCopy) -> None:
    path = download_object(obj.url)
    upload_object(path, obj.target_key)
    os.remove(path)


def download_object(url: str) -> str:
    chunk_size = 104857600  # 100 MB
    path = f'/tmp/{url.split("/")[-1]}'
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
    return path


def upload_object(path: str, key: str) -> None:
    chunk_size = 104857600  # 100 MB
    S3.Bucket(TARGET_BUCKET).upload_file(
        Filename=path,
        Key=key,
        Config=boto3.s3.transfer.TransferConfig(multipart_threshold=chunk_size, multipart_chunksize=chunk_size)
    )


def get_env_var(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise MissingEnvVar(name)
    return val


def lambda_handler(event, context) -> None:
    main(dry_run=False)


def main(dry_run: bool) -> None:
    if dry_run:
        print('(DRY RUN)')

    hyp3_url = get_env_var('HYP3_URL')
    earthdata_username = get_env_var('EARTHDATA_USERNAME')
    earthdata_password = get_env_var('EARTHDATA_PASSWORD')
    target_prefix = get_env_var('S3_TARGET_PREFIX')

    print(f'HyP3 API URL: {hyp3_url}')
    print(f'Earthdata user: {earthdata_username}')

    hyp3 = hyp3_sdk.HyP3(api_url=hyp3_url, username=earthdata_username, password=earthdata_password)

    jobs = hyp3.find_jobs(status_code='SUCCEEDED')
    print(f'Jobs: {len(jobs)}')

    existing_objects = get_existing_objects(target_prefix)
    print(f'Existing objects: {len(existing_objects)}')

    objects_to_copy = get_objects_to_copy(jobs, existing_objects, target_prefix, EXTENSIONS)
    print(f'Objects to copy: {len(objects_to_copy)}')

    copy_objects(objects_to_copy, dry_run)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    parser.add_argument('--no-dry-run', action='store_true')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main(dry_run=(not args.no_dry_run))
