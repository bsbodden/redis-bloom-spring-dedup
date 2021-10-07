## DeDuplication with Redis Bloom in SpringBoot

## Running Locally

### Use docker-compose to start all containers

```
docker-compose up
```

### Launch the Spring Boot client

```
./mvnw spring-boot:run
```
[Use the Web UI](http://localhost:8000)

## Using the Demo

[Navigate to the job submission page](http://localhost:8000)

- Set the number of messages to stream
- Set the percentage of duplicate messages
- Set the time to sleep between messages (stay over 5 ms)

The job will be submitted and the Graphs view will update every 10 seconds to show the Filtered (Deduplicated) message count and the Unfiltered(total) message count
