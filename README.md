Branch Strategy
feat/{development} , fix/{fix} 
e.g., feat/add_weather_feature 
development or bug fix branch. Branch by work.

dev
 integration branch. Merge tested in Google Cloud stg environment.

mainBranch
 for release. Manage the code that is eventually deployed to production.

Managed Resources
Cloud Run Jobs

short-term-stage1

short-term-stage2-weekly-test

short-term-stage2-monthly-test

short-term-stage2-weekly-test-wk6sales2  

Cloud Run Service (Functions)  

vertex-pipeline-shortterm1-weekly2

vertex-pipeline-shortterm1-monthly2

vertex-pipeline-shortterm1-monthly3

vertex-pipeline-shortterm1-midium 

Resources for this subsequent Vertex Pipeline will be added in the future

Process Flow
(See separate document)

Deployment Method (STG)
Cloud Run Jobs

Cloud Run Jobs resources always refer to the dev-latest tag.

Build in GA when PR to dev branch, push to Artifact Registory. Give dev-latest tag.
 3A. Move dev-origin tag to an image with dev-latest tag in GA when merging into dev branch.
 3B. Move dev-latest to an image with dev-origin tag when rejecting a PR to dev branch.

Cloud Run Service (Functions)

Cloud Run Service resources always refer to the dev-latest tag.

Build in GA when PR to dev branch, and push to Artifact Registry. Give dev-latest tag.
 3A. Move the dev-origin tag to an image with the dev-latest tag in GA when merging into a dev branch, and redeploy Cloud Run Service with
 that dev-origin image. 
3B. When rejecting a PR to the dev branch, move dev-latest to an image tagged with the dev-origin tag, and redeploy the Cloud Run Service with
 that dev-latest image.

In Cloud Run, Jobs and Services reflect Docker images differently.

Cloud Run Jobs always works by fetching the latest image of the specified tag at job execution.
 Therefore, you only need to replace tags such as , such as , on the Artifact Registry side, so you don't need to redeploy.dev-latestdev-origin

Cloud Run Services, on the other hand, refers to the digest (fixed hash) of the image at the time of deployment.
 Therefore, re-tagging on the Artifact Registry side will not automatically reflect and will require redeploying Cloud Run Service with a new image.

This means that Jobs only need to be replaced with tags and will be reflected immediately,
 while Services will not reflect changes unless you redeploy them.

このレポジトリの処理を参考にしてください(https://github.com/cainz-technology/cainz-demandforecast-statistical-model/tree/dev/.github/workflowsConnect your Github account )

In Cloud Run, Jobs and Services handle Docker image updates differently.

Cloud Run Jobs always pull the latest image for the specified tag each time a job is executed. 
Therefore, simply updating or switching tags in Artifact Registry (e.g.,  or ) will immediately take effect, and no redeployment is required.dev-latestdev-origin

On the other hand, Cloud Run Services  lock the image digest (a fixed hash) at the time of deployment. 
This means that even if you switch or update tags in Artifact Registry, the changes will not take effect automatically. 
To apply the updated image, you must redeploy the Cloud Run Service.

In short, Jobs immediately reflect image tag changes, while Services require a redeployment to pick up the latest image.

Please refer to the processing in this repository for reference:
https://github.com/cainz-technology/cainz-demandforecast-statistical-model/tree/dev/.github/workflowsConnect your Github account 

Test Specifications
The service name when deployed to the STG environment is the same as the production version.

Tests are performed in parallel with multiple tasks, which are performed by specifying a single store.

The reference table is done to the prd environment, the output is to the stg environment, and the output is to consider idempotency (in the case of BigQuery, if there is no output of the week, delete it before output).

Test specifications are defined by the administrator for each change. (OK for testing a single resource or pipeline execution required, etc.)

restriction
PRs to the dev branch are done in Draft. (forced in GA)

If the Reviewer has completed the previous PR, run "Ready for review", promote it to a PR, and deploy it to the stg environment. (forced in GA) 
Example: 



name: Enforce PR policy for dev

on:
  pull_request:
    branches:
      - dev
    types: [opened, reopened, ready_for_review]

permissions:
  contents: read
  pull-requests: write

jobs:
  pr-policy:
    runs-on: ubuntu-latest
    steps:
      - name: Check PR rules
        env:
          ACTOR: ${{ github.actor }}
          REPO:  ${{ github.repository }}
          PRNUM: ${{ github.event.pull_request.number }}
          ALLOWED_REVIEWER: ${{ vars.ALLOWED_REVIEWER }}
        run: |
          # --- Draft強制 ---
          if [ "${{ github.event.action }}" = "opened" ] || [ "${{ github.event.action }}" = "reopened" ]; then
            if [ "${{ github.event.pull_request.draft }}" != "true" ]; then
              echo "PRs to dev must be created as Draft."
              exit 1
            fi
          fi

          # --- Readyは管理者だけ ---
          if [ "${{ github.event.action }}" = "ready_for_review" ]; then
            if [ "$ACTOR" != "$ALLOWED_REVIEWER" ]; then
              echo "Only $ALLOWED_REVIEWER can mark Ready for review. Reverting to Draft."
              gh api -X POST repos/$REPO/pulls/$PRNUM/convert-to-draft
              gh pr comment $PRNUM -R $REPO --body "Ready for review is restricted. Only $ALLOWED_REVIEWER can promote PRs."
              exit 1
            fi
          fi

          echo "PR policy check passed."
Apply the following rules to the dev, main branch 

Require branches to be up to date before merging

Branches need to be updated before merging

Require status checks to pass before merging 

You can't merge unless all status checks succeed

authentication
Deploy with authentication using Direct Workload Identity Federation

以下の内容を参照(https://github.com/cainz-technology/cainz-search-similar-products/blob/main/.github/workflows/deploy-cloud-functions.yml)Connect your Github account 

Please refer to the following repository for a similar configuration. 
https://github.com/cainz-technology/cainz-demandforecast-statistical-modelConnect your Github account 



###############This is cicd.yml file###########
name: CI & Build/Push

on:
  push:
    branches:
      - 'feat/**'
      - 'fix/**'
  pull_request:
    branches:
      - main
      - dev

env:
  REGION: asia-northeast1
  REPOSITORY: longterm2-artifacts
  IMAGE_NAME: statistical_model_prediction

jobs:
  ci:
    if: ${{ github.event_name == 'push' }}
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python 3.10
        id: setup-python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install Poetry
        uses: snok/install-poetry@v1
        with:
          version: latest
          virtualenvs-create: true
          virtualenvs-in-project: true

      - name: Load cached venv
        id: cached-poetry-dependencies
        uses: actions/cache@v3
        with:
          path: .venv
          key: venv-${{ runner.os }}-${{ steps.setup-python.outputs.python-version }}-${{ hashFiles('**/poetry.lock') }}

      - name: Install dependencies
        if: steps.cached-poetry-dependencies.outputs.cache-hit != 'true'
        run: poetry install --no-interaction --no-root --with dev

      - name: Make format_and_lint.sh executable
        run: chmod +x format_and_lint.sh

      - name: Run format and lint
        run: |
          source .venv/bin/activate
          ./format_and_lint.sh

      - name: Check for changes
        run: |
          if [ -n "$(git status --porcelain)" ]; then
            echo "Files were modified by formatters:"
            git status --porcelain
            echo "Please run 'format_and_lint.sh' locally and commit the changes."
            exit 1
          else
            echo "All files are properly formatted."
          fi

      - name: Run tests
        run: |
          source .venv/bin/activate
          pytest

  build-push:
    if: ${{ github.event_name == 'pull_request' }}   # ← PR のときだけ CD 実行#
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set environment variables by PR base
        run: |
          if [[ "${{ github.base_ref }}" == "main" ]]; then
            echo "PROJECT_ID=prd-cainz-demandforecast" >> $GITHUB_ENV
            echo "GCP_WIF_PROVIDER=${{ secrets.PRD_GCP_WIF_PROVIDER }}" >> $GITHUB_ENV
            echo "GCP_SA_EMAIL=${{ secrets.PRD_GCP_SA_EMAIL }}" >> $GITHUB_ENV
          else
            echo "PROJECT_ID=stg-cainz-demandforecast" >> $GITHUB_ENV
            echo "GCP_WIF_PROVIDER=${{ secrets.STG_GCP_WIF_PROVIDER }}" >> $GITHUB_ENV
            echo "GCP_SA_EMAIL=${{ secrets.STG_GCP_SA_EMAIL }}" >> $GITHUB_ENV
          fi

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v1
        with:
          workload_identity_provider: ${{ env.GCP_WIF_PROVIDER }}
          service_account: ${{ env.GCP_SA_EMAIL }}
          project_id: ${{ env.PROJECT_ID }}

      - name: Set up gcloud CLI
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ env.PROJECT_ID }}

      - name: Configure Docker credential helper
        run: gcloud auth configure-docker ${{ env.REGION }}-docker.pkg.dev

      - name: Build Docker image
        run: >
          docker build -t ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-latest .

      - name: Push Docker image
        run: >
          docker push ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-latest





###################This is dev_merge.yml file############# 
name: Dev Branch Merge/Reject Handler

on:
  pull_request:
    types: [closed]
    branches:
      - dev

env:
  REGION: asia-northeast1
  REPOSITORY: longterm2-artifacts
  IMAGE_NAME: statistical_model_prediction

jobs:
  handle-merge:
    if: github.event.pull_request.merged == true
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set environment variables
        run: |
          echo "PROJECT_ID=stg-cainz-demandforecast" >> $GITHUB_ENV
          echo "GCP_WIF_PROVIDER=${{ secrets.STG_GCP_WIF_PROVIDER }}" >> $GITHUB_ENV
          echo "GCP_SA_EMAIL=${{ secrets.STG_GCP_SA_EMAIL }}" >> $GITHUB_ENV

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v1
        with:
          workload_identity_provider: ${{ env.GCP_WIF_PROVIDER }}
          service_account: ${{ env.GCP_SA_EMAIL }}
          project_id: ${{ env.PROJECT_ID }}

      - name: Set up gcloud CLI
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ env.PROJECT_ID }}

      - name: Move dev-origin tag to dev-latest image
        run: |
          # Get the image digest for dev-latest tag
          DEV_LATEST_DIGEST=$(gcloud artifacts docker images describe \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-latest \
            --format='value(image_summary.digest)')
          
          # Remove existing dev-origin tag if it exists
          gcloud artifacts docker tags delete \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-origin \
            --quiet || true
          
          # Add dev-origin tag to the image that has dev-latest tag
          gcloud artifacts docker tags add \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}@${DEV_LATEST_DIGEST} \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-origin

  handle-reject:
    if: github.event.pull_request.merged == false
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set environment variables
        run: |
          echo "PROJECT_ID=stg-cainz-demandforecast" >> $GITHUB_ENV
          echo "GCP_WIF_PROVIDER=${{ secrets.STG_GCP_WIF_PROVIDER }}" >> $GITHUB_ENV
          echo "GCP_SA_EMAIL=${{ secrets.STG_GCP_SA_EMAIL }}" >> $GITHUB_ENV

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v1
        with:
          workload_identity_provider: ${{ env.GCP_WIF_PROVIDER }}
          service_account: ${{ env.GCP_SA_EMAIL }}
          project_id: ${{ env.PROJECT_ID }}

      - name: Set up gcloud CLI
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ env.PROJECT_ID }}

      - name: Move dev-latest tag to dev-origin image
        run: |
          # Get the image digest for dev-origin tag
          DEV_ORIGIN_DIGEST=$(gcloud artifacts docker images describe \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-origin \
            --format='value(image_summary.digest)')
          
          # Remove existing dev-latest tag if it exists
          gcloud artifacts docker tags delete \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-latest \
            --quiet || true
          
          # Add dev-latest tag to the image that has dev-origin tag
          gcloud artifacts docker tags add \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}@${DEV_ORIGIN_DIGEST} \
            ${{ env.REGION }}-docker.pkg.dev/${{ env.PROJECT_ID }}/${{ env.REPOSITORY }}/${{ env.IMAGE_NAME }}:dev-latest
          
          echo "Reverted dev-latest tag to stable dev-origin image after PR rejection"
