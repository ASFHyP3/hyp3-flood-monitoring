# hyp3-flood-monitoring

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
start and end datetime range. But no jobs will be submitted for data that was
acquired after the subscription's end datetime.

Additionally, a second AWS Lambda function runs periodically and copies any new RTC products
that have been created by flood monitoring subscriptions into an S3 bucket for permanent
archival.

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

## TODO

* Document how to run the two lambda functions and the purpose of the `scripts` branch.
* Explain the output of the `check_subscriptions` script.
* Explain why there are always more enabled subscriptions than active hazards.
* Perhaps clarify wording in the output of `generate_stats` to refer to *enabled* subscriptions and *active* hazards.
