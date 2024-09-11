get_deps:
	@echo "Pulling a tar archive of a Hydrofabric Base image compatible with the Hydrofabric API"
	aws s3 cp s3://ngwpc-dev/DanielCumpton/hydrofabric_service_v1.tar /tmp/hydro_api/
	@echo "Running docker load to add the image to your local docker registry"
	sudo docker load -i /tmp/hydro_api/hydrofabric_service_v1.tar

	@echo "Pulling a hydrofabric data archive from S3"
	aws s3 cp s3://ngwpc-hydrofabric/hydrofabric_data.tgz /tmp/hydro_api/
	@echo "Extracting the hydrofabric data to Hydrofabric in your current working directory"
	tar -xzf /tmp/hydro_api/hydrofabric_data.tgz -C $(shell pwd)/

build:
	sudo docker build -t hydrofabric_api .

run:
	sudo docker run --network host -it -e DB_PASSWORD=$(DB_PASSWORD) -e AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) -e AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) -e AWS_SESSION_TOKEN=$(AWS_SESSION_TOKEN) -v $(shell pwd):/workspace -v $(shell pwd)/Hydrofabric:/Hydrofabric -p 8000:8000 hydrofabric_api
