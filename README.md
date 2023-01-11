# hyp3-flood-monitoring

## Architecture overview

TODO

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

The Pacific Disaster Center (PDC) provides a Hazard API:

* PDC Hazard API (production): <https://sentry.pdc.org/hp_srv/>
* PDC Hazard API (test): <https://testsentry.pdc.org/hp_srv/>

The automated flood monitoring system (including our test deployment) only interacts with the production Hazard API.
When running queries manually, we as developers also interact only with the production Hazard API, unless the PDC
team asks us to query their test API for a specific reason.

The Hazard API authorization tokens are available in the `tools_user_accounts` secret in AWS Secrets Manager
(in the HyP3 AWS account), via the secret keys `PDC Hazard API auth token (prod)`
and `PDC Hazard API auth token (test)`.

## TODO

Document how to run the two lambda functions and the purpose of the `scripts` branch.
