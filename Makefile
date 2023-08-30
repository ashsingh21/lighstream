.PHONY: clean

clean:
	- docker kill $(docker ps -q) 
	- docker rmi $(docker images -a -q) -f 
	- echo 'y' | docker container prune