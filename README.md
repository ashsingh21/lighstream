# Milestones
* Implement basic metadata cluster using Syclladb
* Implement metadata client and syclla client
* Implement lightstream client that uses metadata client internally 
* implement basic producer and consumer -> every message gets commited blocking and consumer can stream that data


When I comeback I need to setup MinIO on my ssd then find a library that will allow me to stream data to it, with retry and multipart

TODO:
The design of the Agent could be Agent {Router;  Service (service is currently agent, need to chnage it to service then wrap it in agent)}