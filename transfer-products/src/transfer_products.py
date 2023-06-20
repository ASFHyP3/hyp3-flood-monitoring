import argparse
import os
from dataclasses import dataclass

import boto3
import botocore.exceptions
import hyp3_sdk
from boto3.s3.transfer import TransferConfig

S3 = boto3.resource('s3')

# TODO decide on appropriate extensions
EXTENSIONS = ['_VV.tif', '_VH.tif', '_rgb.tif', '_dem.tif', '_WM.tif', '.README.md.txt']


@dataclass(frozen=True)
class ObjectToCopy:
    source_bucket: str
    source_key: str
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

        zip_key: str = job.files[0]['s3']['key']
        assert zip_key.endswith('.zip')

        for ext in extensions:
            source_key = zip_key.removesuffix('.zip') + ext
            target_key = f'{target_prefix}/{source_key.split("/")[-1]}'

            if target_key not in existing_objects:
                objects_to_copy.append(
                    ObjectToCopy(
                        source_bucket=job.files[0]['s3']['bucket'],
                        source_key=source_key,
                        target_key=target_key,
                    )
                )

    return objects_to_copy


def copy_objects(objects_to_copy: list[ObjectToCopy], target_bucket: str, dry_run: bool) -> None:
    for count, obj in enumerate(objects_to_copy, start=1):
        print(
            f'({count}/{len(objects_to_copy)}) '
            f'Copying {obj.source_bucket}/{obj.source_key} to {target_bucket}/{obj.target_key}'
        )
        if not dry_run:
            try:
                copy_object(obj.source_bucket, obj.source_key, target_bucket, obj.target_key)
            except botocore.exceptions.ClientError as e:
                print(f'Error copying object: {e}')


def copy_object(source_bucket, source_key, target_bucket, target_key, chunk_size=104857600):
    bucket = S3.Bucket(target_bucket)
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    transfer_config = TransferConfig(multipart_threshold=chunk_size, multipart_chunksize=chunk_size)
    bucket.copy(CopySource=copy_source, Key=target_key, Config=transfer_config)


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
