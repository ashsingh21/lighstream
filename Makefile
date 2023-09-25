.PHONY: clean start restart_app

clean:
	- docker kill $(docker ps -q) 
	- docker rmi $(docker images -a -q) -f 
	- echo 'y' | docker container prune

start_fdb:
	docker compose up -d
	./start.bash

restart_app:
	docker compose down
	docker compose up --no-deps --build