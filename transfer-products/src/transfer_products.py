import argparse
import os
from dataclasses import dataclass

import boto3
import boto3.s3.transfer
import botocore.exceptions
import hyp3_sdk
import requests

S3 = boto3.resource('s3')

# TODO decide on appropriate extensions
EXTENSIONS = ['_VV.tif', '_VH.tif', '_rgb.tif', '_dem.tif', '_WM.tif', '.README.md.txt']


@dataclass(frozen=True)
class ObjectToCopy:
    url: str
    target_key: str


class MissingEnvVar(Exception):
    pass


def get_existing_objects(target_bucket: str, target_prefix: str) -> frozenset[str]:
    return frozenset(obj.key for obj in S3.Bucket(target_bucket).objects.filter(Prefix=f'{target_prefix}/'))


def get_objects_to_copy(
        jobs: hyp3_sdk.Batch,
        existing_objects: frozenset[str],
        target_prefix: str,
        extensions: list[str]) -> list[ObjectToCopy]:

    objects_to_copy = []
    for job in jobs:
        if job.expired():
            continue

        assert job.succeeded()

        zip_filename: str = job.files[0]['filename']
        assert zip_filename.endswith('.zip')

        zip_url: str = job.files[0]['url']
        assert zip_url.endswith('.zip')

        for ext in extensions:
            filename = zip_filename.removesuffix('.zip') + ext
            url = zip_url.removesuffix('.zip') + ext
            assert url.endswith(filename)

            target_key = f'{target_prefix}/{filename}'

            if target_key not in existing_objects:
                objects_to_copy.append(ObjectToCopy(url=url, target_key=target_key))

    return objects_to_copy


def copy_objects(objects_to_copy: list[ObjectToCopy], target_bucket: str, dry_run: bool) -> None:
    for count, obj in enumerate(objects_to_copy, start=1):
        print(
            f'({count}/{len(objects_to_copy)}) '
            f'Copying {obj.url} to {target_bucket}/{obj.target_key}'
        )
        if not dry_run:
            try:
                copy_object(obj, target_bucket)
            except (botocore.exceptions.ClientError, requests.HTTPError) as e:
                print(f'Error copying object: {e}')


def copy_object(obj: ObjectToCopy, target_bucket: str) -> None:
    path = download_object(obj.url)
    upload_object(path, target_bucket, obj.target_key)
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


def upload_object(path: str, target_bucket: str, target_key: str) -> None:
    chunk_size = 104857600  # 100 MB
    S3.Bucket(target_bucket).upload_file(
        Filename=path,
        Key=target_key,
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
    target_bucket = get_env_var('S3_TARGET_BUCKET')
    target_prefix = get_env_var('S3_TARGET_PREFIX')

    print(f'HyP3 API URL: {hyp3_url}')
    print(f'Earthdata user: {earthdata_username}')

    hyp3 = hyp3_sdk.HyP3(api_url=hyp3_url, username=earthdata_username, password=earthdata_password)

    jobs = hyp3.find_jobs(status_code='SUCCEEDED')
    print(f'Jobs: {len(jobs)}')

    existing_objects = get_existing_objects(target_bucket, target_prefix)
    print(f'Existing objects: {len(existing_objects)}')

    objects_to_copy = get_objects_to_copy(jobs, existing_objects, target_prefix, EXTENSIONS)
    print(f'Objects to copy: {len(objects_to_copy)}')

    copy_objects(objects_to_copy, target_bucket, dry_run=dry_run)


if __name__ == '__main__':
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    parser.add_argument('--no-dry-run', action='store_true')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main(dry_run=(not args.no_dry_run))
