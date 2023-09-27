.PHONY: clean start restart_app

clean:
	- docker kill $(docker ps -q) 
	- docker rmi $(docker images -a -q) -f 
	- echo 'y' | docker container prune

start: start_fdb start_minio

start_fdb: start_minio
	docker compose up -d
	./start.bash

start_minio:
	docker run -d \
   		-p 9000:9000 \
   		-p 9090:9090 \
   		--name minio \
   		-v /media/ashu/seagate/minio/data:/data \
   		-e "MINIO_ROOT_USER=admin" \
   		-e "MINIO_ROOT_PASSWORD=test_admin" \
   		quay.io/minio/minio server /data --console-address ":9090"


restart_app:
	docker compose down
	docker compose up --no-deps --build