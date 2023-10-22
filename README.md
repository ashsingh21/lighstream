# Milestones
* Implement basic metadata cluster using Syclladb
* Implement metadata client and syclla client
* Implement lightstream client that uses metadata client internally 
* implement basic producer and consumer -> every message gets commited blocking and consumer can stream that data


When I comeback I need to setup MinIO on my ssd then find a library that will allow me to stream data to it, with retry and multipart

TODO:
The design of the Agent could be Agent {Router;  Service (service is currently agent, need to chnage it to service then wrap it in agent)}

Note: ActorFactory uses unbounded queue so it can cause exhaustion of memory if workers dont process  queue fast enough

Note: you will need to install the fdbclient on the host to connect to the cluster

TODO: Build table abstraction over topic subspaces

TODO" add multipart https://github.com/apache/incubator-opendal/blob/c7bfe23dd1a0ec796e6751ead8a59d98fb13140c/core/src/docs/rfcs/1420_object_writer.md?plain=1#L21


Good Read on AWS s3 polic7y stuff
https://www.chrisfarris.com/bucket-policy-examples/#:~:text=Principal%20is%20used%20by%20Resource,IAM)%20to%20users%20or%20roles.

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement1",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::251752769356:user/naditest"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::nadi",
                "arn:aws:s3:::nadi/*"
            ]
        }
    ]
}