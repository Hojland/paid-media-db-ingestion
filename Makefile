.PHONY: build start_dev push_image

aws_login:
	$(eval export AWS_PROFILE=$(shell bash -c 'read -p "Enter AWS_PROFILE: " pass; echo $$pass'))
	$(eval export AWS_DEFAULT_REGION=$(shell bash -c 'read -p "Enter AWS_DEFAULT_REGION: " pass; echo $$pass'))


build:
	@-docker build -f Dockerfile . \
		-t paid-media-db-ingestion:latest

push_image:
	make aws_login
	aws ecr get-login-password | docker login --username AWS --password-stdin 059692690036.dkr.ecr.eu-central-1.amazonaws.com/dev-ecr/paid-media-db-ingestion
	docker tag paid-media-db-ingestion:latest 059692690036.dkr.ecr.eu-central-1.amazonaws.com/dev-ecr/paid-media-db-ingestion
	docker push 059692690036.dkr.ecr.eu-central-1.amazonaws.com/dev-ecr/paid-media-db-ingestion


start_dev:
	@-PWD=$(shell pwd)
	@-docker stop paid-media-db-ingestion_dev > /dev/null 2>&1 ||:
	@-docker container prune --force > /dev/null

	@-docker build -f dev.Dockerfile . \
		--no-cache -t paid-media-db-ingestion_dev:latest

	@-docker run \
		-p 10000:8888 \
		--rm \
		--env-file .env \
		--name paid-media-db-ingestion_dev \
		--cpus=1 \
		-v $(PWD)/src/:/app/src/ \
		-d \
		paid-media-db-ingestion_dev:latest > /dev/null

	@echo "Container started"
	@echo "Jupyter is running at http://localhost:10000/?token=paid"