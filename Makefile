check-env:
    @: $(or $(AWS_ACCESS_KEY_ID),$(error AWS_ACCESS_KEY_ID is not set))
    @: $(or $(AWS_SECRET_ACCESS_KEY),$(error AWS_SECRET_ACCESS_KEY is not set))
    @: $(or $(AWS_SESSION_TOKEN),$(error AWS_SESSION_TOKEN is not set))
	@: $(or $(DB_USER),$(error DB_USER is not set))
    @: $(or $(DB_PASSWORD),$(error DB_PASSWORD is not set))
	@: $(or $(DB_HOST),$(error DB_HOST is not set))
	@: $(or $(DJANGO_SECRET_KEY),$(error DJANGO_SECRET_KEY is not set))


get_deps: check-env
	@echo "Pulling a hydrofabric data archive from S3"
	aws s3 cp --no-progress s3://ngwpc-hydrofabric/hydrofabric_data.tgz /tmp/hydro_api/
	@echo "Extracting the hydrofabric data to Hydrofabric in your current working directory"
	tar -xzf /tmp/hydro_api/hydrofabric_data.tgz -C $(shell pwd)/

build:
	sudo docker build --no-cache -t hydrofabric_api .

run: check-env
	@echo "Starting the docker container running the hydrofabric_api on port 8000 \n"
	@sudo docker run --network host -it -e DB_USER=$(DB_USER) -e DB_PASSWORD=$(DB_PASSWORD) -e DB_HOST=$(DB_HOST) -e AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) -e AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -e AWS_SESSION_TOKEN=$(AWS_SESSION_TOKEN) -e DJANGO_SECRET_KEY=$(DJANGO_SECRET_KEY) -v $(shell pwd):/workspace -v $(shell pwd)/Hydrofabric:/Hydrofabric -p 8000:8000 hydrofabric_api
