.PHONY: clean start restart_app

clean:
	- docker kill $(docker ps -q) 
	- docker rmi $(docker images -a -q) -f 
	- echo 'y' | docker container prune

start:
	docker compose up

restart_app:
	docker compose down
	docker compose up --no-deps --build