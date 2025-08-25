#################deploy-cloud-functions.yml
name: Deploy Cloud Functions

on:
  push:
    branches:
      - dev
      - main
    paths:
      - 'pipeline/weekly_update/**'
      - '.github/workflows/deploy-cloud-functions.yml'
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set function name based on branch
        id: set-function-name
        run: |
          if [[ "${{ github.ref }}" == "refs/heads/main" ]]; then
            echo "FUNCTION_NAME=update-similar-products-search" >> $GITHUB_ENV
            echo "Using production function name: update-similar-products-search"
          else
            echo "FUNCTION_NAME=update-similar-products-search-stg" >> $GITHUB_ENV
            echo "Using staging function name: update-similar-products-search-stg"
          fi

      - name: Authenticate to Google Cloud
        uses: google-github-actions/auth@v1
        with:
          workload_identity_provider: ${{ secrets.GCP_WIF_PROVIDER }}
          service_account: ${{ secrets.GCP_SA_EMAIL }}
          project_id: dev-cainz-demandforecast

      - name: Set up gcloud CLI
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: dev-cainz-demandforecast

      - name: Set Python version
        id: python-version
        run: |
          echo "PYTHON_VERSION=3.10" >> $GITHUB_ENV
          echo "Using Python version: 3.10"

      - name: Deploy Cloud Function
        id: deploy
        run: |
          # YAMLファイルから環境変数を読み取り
          ENV_VARS=$(python -c '
          import yaml, sys
          with open("pipeline/weekly_update/config.yaml", "r") as f:
              config = yaml.safe_load(f)
          env_vars = []
          # 最上位の変数
          for key, value in config.items():
              env_vars.append(f"{key.upper()}={value}")
          print(",".join(env_vars))
          ')
          
          gcloud functions deploy ${{ env.FUNCTION_NAME }} \
            --region=asia-northeast1 \
            --runtime=python310 \
            --source=pipeline/weekly_update \
            --entry-point=main \
            --trigger-http \
            --no-allow-unauthenticated \
            --service-account=read-czdp-receive-data-from-df@dev-cainz-demandforecast.iam.gserviceaccount.com \
            --memory=256MiB \
            --timeout=3600s \
            --set-env-vars=${ENV_VARS} \
            --project=dev-cainz-demandforecast

      - name: Print deployment info
        if: steps.deploy.outcome == 'success'
        run: |
          echo "Cloud Function deployed successfully!"
          echo "Function name: ${{ env.FUNCTION_NAME }}"
          echo "Region: asia-northeast1"
          echo "Runtime: python310"
          echo "Entry point: main"
          echo "Service account: read-czdp-receive-data-from-df@dev-cainz-demandforecast.iam.gserviceaccount.com"
          echo "Authentication: Required"
          echo "Memory: 256MiB"
          echo "Timeout: 3600s"


###########################dev-merge.yml
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



###################################cicd.yml
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
