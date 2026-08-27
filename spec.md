# Technical specification for an asynchronous gateway

## General service architecture

I am creating a service that will receive OpenAI compatible requests for LLM inference, either regular online requests or batch requests.  The gateway will use a queue to handle requests asynchronously and send then them to backend services (such as the OpenAI endpoint itself, Gemini, etc.) according to different availability criteria.

Some considerations about the backend services:

- They will be responsible for processing the requests and returning the responses to the gateway. The gateway will then be responsible for making available the responses to the user.

- They may themselves expose OpenAI compatible interfaces with or without batch capabilities.

- They are assumed to expose an endpoint for health checking and availability monitoring purposes so that the gateway knows when to send queued requests to them.

- The main target backend service will be GCP Provisioned Throughput, so that queued requests can be processed in bulk when the capacity of Provisioned Throughput endpoints is not fully used.

- Other backends to be considered are regular Gemini endpoints on different consumption models (FLEX, etc), as well as other LLM providers.

The gateway must be able to handle the following scenarios.

- The gateway administrator should configure backend services in a yaml file, each one indicating its API endpoint, authentication and authorization configs, its capabilities (suports online inference and/or batch inference), the endpoint to check its health and availability.

- Users should be able to specify in their requests what is the maximum wait time they are willing to wait for a response. If the request is not processed within that time, the gateway should return an error to the user.

- The gateway administrator should also configure policies through which user requests are routed to different backend services. This should include an order of preference in which backend services are tried if they are not available and/or a failover strategy for when a request fails during processing.

- These policies must also account for requests being routed to a specific backend service based on the type of content, size of the request, or other factors.

- The user may submit a batch request (with a reference to a json file with the requests), but the backend service may just support individual requests. In this case, the gateway will be responsible for breaking the batch request into individual requests and sending them to the backend service.

- In all cases, upon submitting any kind of request he will get a request ID and the asynchronous response will be delivered as if it always was a batch request, in some shared storage system.

- The user always poll the gateway for the status of a request and retrieve the response when it is ready.

## Implementation details

The system must implemented in Google Cloud Platform and have the following components:

- The entry point will be an OpenAPI compatible HTTP interface exposed through GCP Apigee. Apigee will handle authentication and authorization of requests. It will also be responsible for request validation and transformation.

- A BigQuery table will be used to store and track requests with a corresponding status.

- Upon receiving a request in apigee, it will be (1) sent to a Pub/Sub queue which will store it until it can be processed, (2) registered in the BigQuery table with a status of PENDING.

- A fleet of workers (as Cloud Run jobs) will poll the Pub/Sub queue for pending requests and process them according to the configured policies.

- Responses will be stored in Google Cloud Storage (GCS) in json format and referenced in the BigQuery table. They will be persisted for a default of 7 days.

- The BigQuery table should be partitioned by date and include columns for request metadata, status, and response information, including response status, content length, timestamp, backend service that served the request and elapsed time.

- The job will update the status of the request in the BigQuery table as it progresses through the different stages of processing.

Handling batch requests broken down into individual calls to backend services:

- A second pub/sub queue will be used when user batch requests have to be broken into individual requests and sent to the backend service.

- Individual queries resulting from breaking down a batch request will share the same request ID and will be tagged with the sequence number of the query within the batch request.

- Workers will also poll this second queue for individual queries and process them according to the configured policies.

- The BigQuery table will also contain entries for individual queries resulting from breaking down a batch request, with the same request ID and the sequence number of the query within the batch request.

- Workers handling this individual queries resulting from breaking down a batch request must ensure that the responses are reassembled in the correct order and with the correct request ID before being stored in GCS. In case of timeout or failure of one of the workers, the response from that worker should be marked as failed in the BigQuery table.


## Your task

You must 

- design and implement the asynchronous gateway as described in this document. 

- use Google Cloud Platform services for all components of the system. 

- use Python for all code you write. 

- create all necessary configuration artifacts to deploy all the components of the system, including Dockerfiles for the Cloud Run workers and Terradorm Infrastructure as Code (IaC) artifacts.

- create a deployment script that automates the deployment of all the components of the system.

- create a infrastructure check script that can be used to verify the health of all components of the system and if there is any configuration drift compared to the terradorm artifacts.

- create a set of unit tests and integration tests for the system.

- create a set of tests to simulate the different failure scenarios described in the problem description. Specially the ones involving breaking up a batch request into individual ones.

- create documentation for the system including a README file, architecture diagrams, deployment instructions, operational runbooks, and API documentation.

- create a simple UI allowing administrators and users to see the status of the requests and the responses when available. It should also allow administrators to configure backend services and policies.









