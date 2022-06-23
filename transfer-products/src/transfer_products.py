import argparse
import os
from dataclasses import dataclass

import boto3
import hyp3_sdk
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

S3 = boto3.resource('s3')

TARGET_BUCKET = 'hyp3-nasa-disasters'

TARGET_PREFIX = 'PDC-test'

# TODO decide on appropriate extensions
EXTENSIONS = ['_VV.tif', '_VH.tif', '_rgb.tif', '_dem.tif', '_WM.tif', '.README.md.txt']

# TODO handle when source key doesn't exist?

# TODO tests


@dataclass(frozen=True)
class ObjectToCopy:
    source_bucket: str
    source_key: str
    target_key: str


class MissingEnvVar(Exception):
    pass


def get_jobs(hyp3: hyp3_sdk.HyP3) -> hyp3_sdk.Batch:
    return hyp3.find_jobs(status_code='SUCCEEDED')


def get_existing_objects() -> frozenset[str]:
    return frozenset(obj.key for obj in S3.Bucket(TARGET_BUCKET).objects.filter(Prefix=f'{TARGET_PREFIX}/'))


def get_objects_to_copy(jobs: hyp3_sdk.Batch, existing_objects: frozenset[str]) -> list[ObjectToCopy]:
    objects_to_copy = []
    for job in jobs:
        assert job.succeeded()
        source_bucket: str = job.files[0]['s3']['bucket']
        zip_key: str = job.files[0]['s3']['key']
        assert zip_key.endswith('.zip')
        for ext in EXTENSIONS:
            source_key = get_source_key(zip_key, ext)
            target_key = get_target_key(source_key, job.name, job.job_id)
            if target_key not in existing_objects:
                objects_to_copy.append(
                    ObjectToCopy(source_bucket=source_bucket, source_key=source_key, target_key=target_key)
                )
    return objects_to_copy


def get_source_key(zip_key: str, ext: str) -> str:
    return zip_key.removesuffix('.zip') + ext


def get_target_key(source_key: str, job_name: str, job_id: str) -> str:
    source_prefix, source_basename = source_key.split('/')
    assert source_prefix == job_id
    return '/'.join([TARGET_PREFIX, job_name, job_id, source_basename])


def copy_objects(objects_to_copy: list[ObjectToCopy], dry_run: bool) -> None:
    for count, obj in enumerate(objects_to_copy, start=1):
        print(
            f'({count}/{len(objects_to_copy)}) '
            f'Copying {obj.source_bucket}/{obj.source_key} to {TARGET_BUCKET}/{obj.target_key}'
        )
        if not dry_run:
            copy_object(obj)


def copy_object(obj: ObjectToCopy) -> None:
    chunk_size = 104857600  # 100 MB
    S3.Bucket(TARGET_BUCKET).copy(
        CopySource={'Bucket': obj.source_bucket, 'Key': obj.source_key},
        Key=obj.target_key,
        Config=TransferConfig(multipart_threshold=chunk_size, multipart_chunksize=chunk_size)
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

    print(f'HyP3 API URL: {hyp3_url}')
    print(f'Earthdata user: {earthdata_username}')

    hyp3 = hyp3_sdk.HyP3(api_url=hyp3_url, username=earthdata_username, password=earthdata_password)

    jobs = get_jobs(hyp3)
    print(f'Jobs: {len(jobs)}')

    existing_objects = get_existing_objects()
    print(f'Existing objects: {len(existing_objects)}')

    objects_to_copy = get_objects_to_copy(jobs, existing_objects)
    print(f'Objects to copy: {len(objects_to_copy)}')

    copy_objects(objects_to_copy, dry_run)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    parser.add_argument('--no-dry-run', action='store_true')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main(dry_run=(not args.no_dry_run))
