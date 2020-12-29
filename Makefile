SHELL=/bin/sh

export COMPOSE_DOCKER_CLI_BUILD=1
export BUILDKIT_PROGRESS=tty
export DOCKER_BUILDKIT=1

.PHONY: gcf local

gcf:
	gcloud functions deploy alicorp-pe-is-qlik-gcf \
		--project alicorp-datalake \
		--region us-east1 \
		--ingress-settings internal-only \
		--source . \
		--env-vars-file gcf/.env.yml \
		--ignore-file gcf/.gcloudignore \
		--runtime python37 \
		--trigger-resource alicorp-pe-is-qlik-gcs \
		--trigger-event google.storage.object.finalize \
		--entry-point main \
		--max-instances 1 \
		--memory 2g \
		--timeout 540s \
		--no-retry \
		--service-account alicorp-pe-is-qlik-gsa@alicorp-datalake.iam.gserviceaccount.com

local:
	docker build \
		-f local/Dockerfile \
		-t alicorp-pe-is-qlik-gcf/local \
		--no-cache=false \
		.
	docker run \
		-it --rm \
		--cpus 1 --cpu-shares 1024 --memory 1g --memory-swap 1g \
		-v "$(PWD)":/app:rw \
		alicorp-pe-is-qlik-gcf/local