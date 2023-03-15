# hyp3-flood-monitoring

⚠️Important: As of March 14, 2023, the test and prod flood monitoring systems have been
temporarily disabled until key stakeholders are ready to make use of the data. We have
set the HyP3 job quota for the `hyp3_flood_monitoring` user to `0` so that no new jobs
will be run for flood monitoring subscriptions (but the flood monitoring system will
continue to check for active hazards and create subscriptions).

To re-enable the test and prod flood monitoring systems, follow these steps:

1. Identify the HyP3 deployment used by the flood monitoring system. For test,
   this is the HyP3 deployment that corresponds to the URL given by the `HYP3_URL` parameter in the
   [deploy-test.yml](./.github/workflows/deploy-test.yml) workflow. For prod, see the same parameter in the
   [deploy-prod.yml](./.github/workflows/deploy-prod.yml) workflow.
2. Log in to the AWS account corresponding to that HyP3 deployment, navigate to the DynamoDB console,
   select the HyP3 Users Table, and confirm that there is an item for the `hyp3_flood_monitoring` user
   with `max_jobs_per_month` set to `0`.
3. Edit the `max_jobs_per_month` field and set it to an appropriate value, depending on how much data we want
   the flood monitoring system to produce.

## Architecture overview

The [Pacific Disaster Center](https://www.pdc.org/about) (PDC)
provides a Hazard API (see [PDC Hazard API](#pdc-hazard-api)).
The purpose of this project is to provide RTC products for areas of interest affected by active flood hazards.
This is accomplished by maintaining a [HyP3 subscription](https://hyp3-docs.asf.alaska.edu/using/subscriptions/)
for each active flood hazard.

An AWS Lambda function runs periodically and executes the following steps:

<ol>
<li>
Query the PDC Hazard API for a list of active flood hazards.
</li>
<li>
For each hazard:
  <ol type="a">
  <li>
    If there is no existing HyP3 subscription for the hazard, create one, and set its end datetime for a few hours
    into the future.
  </li>
  <li>
    Otherwise, update the existing HyP3 subscription with any parameters that have changed (e.g. AOI),
    and set its end datetime for a few hours into the future.
  </li>
  </ol>
</li>
</ol>

Note that when a hazard expires (becomes inactive), the following steps occur automatically:

1. The hazard disappears from the list of active hazards returned by the PDC Hazard API.
2. As a result, our system does not update the end datetime for the hazard's HyP3 subscription,
   and the subscription is soon disabled.

Note that a HyP3 subscription remains enabled for a few days beyond its end datetime,
in case any new data becomes available that was acquired within the subscription's
start and end datetime range. No jobs will be submitted for data acquired after the subscription's end datetime.

Additionally, a second AWS Lambda function runs periodically and copies any new RTC products
that have been created by flood monitoring subscriptions into an S3 bucket for permanent
archival.

## Important constants

There are some important global constants defined in [`hyp3_floods.py`](./hyp3-floods/src/hyp3_floods.py):

* `HAZARD_START_DATE_DELTA` allows us to set a HyP3 subscription start date for slightly
  before the hazard start date, in case the hazard start date has an error margin.
* `HAZARD_START_DATE_MINIMUM` prevents setting a HyP3 subscription start date before the
  minimum date, so that we don't back-process a ton of data.

We attempted to choose reasonable values for these constants, but we are open to changing them on request from
the PDC team or other key stakeholders.

## Developer setup

```
conda env create -f environment.yml
conda activate hyp3-flood-monitoring
```

## Environment variables

You'll need to create a `.env` file to specify environment variables required for local development. You can create
a single file named `.env` or multiple `*.env` files in the `env/` directory, depending on whether you need to
specify multiple different environments. Each line of the file should be of the form `KEY='value'`.

Below is a non-exhaustive list of some environment variables that you may want to set in your `.env` file.
(Of course, you can leave your `.env` file blank for now, and simply add variables as needed.
For example, you can try to run a particular script and allow it to complain about missing
environment variables, and then add those variables to your `.env` file.)

* `PDC_HAZARDS_AUTH_TOKEN`: Authorization token for the PDC Hazard API (production);
   see [PDC Hazard API](#pdc-hazard-api).
* `HYP3_URL`: URL for the HyP3 API that you want to query, e.g. <https://hyp3-test-api.asf.alaska.edu>.
* `EARTHDATA_USERNAME`: Available in the `tools_user_accounts` secret in AWS Secrets Manager (in the HyP3 AWS account),
   via the secret key `hyp3-flood-monitoring-edl-username`.
* `EARTHDATA_PASSWORD`: Available in the `tools_user_accounts` secret in AWS Secrets Manager (in the HyP3 AWS account),
   via the secret key `hyp3-flood-monitoring-edl-password`.

## PDC Hazard API

PDC provides a Hazard API:

* PDC Hazard API (production): <https://sentry.pdc.org/hp_srv/>
* PDC Hazard API (test): <https://testsentry.pdc.org/hp_srv/>

The automated flood monitoring system (including our test deployment) only interacts with the production Hazard API.
When running queries manually, we as developers also interact only with the production Hazard API, unless the PDC
team asks us to query their test API for a specific reason.

The Hazard API authorization tokens are available in the `tools_user_accounts` secret in AWS Secrets Manager
(in the HyP3 AWS account), via the secret keys `PDC Hazard API auth token (prod)`
and `PDC Hazard API auth token (test)`.

## Running the Lambda functions locally

You can locally execute the source code for the AWS Lambda functions, e.g. for debugging purposes. Each Lambda
function is provided as a Python script with a command-line interface.

To show the help text for the `hyp3-floods` Lambda function
(for creating and updating HyP3 subscriptions):

```
python hyp3-floods/src/hyp3_floods.py -h
```

To show the help text for the `transfer-products` Lambda function
(for archiving products by copying them to an S3 bucket):

```
python transfer-products/src/transfer_products.py -h
```

Each Lambda function takes a `.env` file as a command-line argument
(see [Environment variables](#environment-variables)).

## Additional scripts

Additional scripts are provided on the
[`scripts`](https://github.com/ASFHyP3/hyp3-flood-monitoring/tree/scripts) branch.

Before running these scripts, create a `.env` file as described in
[Environment variables](#environment-variables).

Also, make sure you have an AWS config profile for the HyP3 account.

You may want to run `check_subscriptions.py` periodically in order to verify that
the flood monitoring system is working as expected:

```
AWS_PROFILE=hyp3 PYTHONPATH=${PWD}/hyp3-floods/src python scripts/check_subscriptions.py <dotenv_path>
```

The `get_stats.py` script is intended to run as a GitHub Actions workflow, though you can run it locally
if you wish. Its purpose is to generate certain statistics describing the operation of the flood
monitoring system, which are made available to key stakeholders.

To run the script via GitHub Actions, navigate to the Actions console for this repo,
select the "Generate Stats" workflow for either test or prod (depending on whether
you wish to generate stats for the test or prod flood monitoring system), and then
manually run the workflow.

The script will make the stats available by publishing a message to an AWS SNS topic, which
has been created manually (not via CloudFormation) in the HyP3 AWS account.
