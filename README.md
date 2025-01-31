# The Kafka Letter Box

The Letter Box pattern enables organisations to exchange data asynchronously. Instead of performing queries on each other's APIs, they can listen to each other's changes. The letter box is a simple endpoint that can be registered somewhere to receive messages. It is the only endpoint organisations have to publish in integration scenarios.

Behind the letter box, the routing of messages is done internally. This can evolve without impacting the senders of messages. The contract are only the messages themselves.

The letter box in this project only accepts mTLS connections with configured peer domains. It doesn't terminate mTLS itself, but relies on an proxy to do that. The proxy is supposed to communicate the domain information through an HTTP header.

The incoming JSON messages are all put in the same configured Kafka topic. Applications can consume the topic and filter out what they need.

The fields `_id` and `_corr` are added with a random UUID if they are not yet present in the message. The field `_domain` is added with the domain that is found in the client certificate. If there is more than one domain in the client certificate, the first one is used.

## Configuration

The configuration is managed by the [Lightbend Config package](https://github.com/lightbend/config). By default it will try to load `conf/application.conf`. An alternative configuration may be loaded by adding `-Dconfig.resource=myconfig.conf`, where the file is also supposed to be in the `conf` directory, or `-Dconfig.file=/conf/myconfig.conf`. If no configuration file is available it will load a default one from the resources. The following entries are available:

|Entry|Mandatory|Description|
|---|---|---|
|asString|No|When set to `true`, the JSON messages are serialised to Kafka as strings. Otherwise, they are serialised as compressed CBOR. The default value is `false`.|
|domains|No|The list of domains that are allowed to connect. Leading wildcards can be used. The default value is the empty list, in which case no connections are allowed.|
|cnPattern|No|The pattern to extract common names from the subject assignments in the client certificate. The default pattern is `^.*CN=([\p{IsAlphabetic}\d\-.*]+).*$`.|
|header|No|This is the HTTP header that carries the subject information from the client certificate. There can be multiple comma-separated subject assignments in the value. The default header is `X-Forwarded-Tls-Client-Cert-Info`.|
|kafka|Yes|All Kafka settings come below this entry. So for example, the setting `bootstrap.servers` would go to the entry `kafka.bootstrap.servers`.|
|namespace|No|A name to distinguish several deployments in the same environment. The default value is `sse`.|
|otlp.grpc|No|The OpenTelemetry endpoint for logs and metrics. It should be a URL like `http://localhost:4317`.|
|topic|Yes|The Kafka topic in which the messages are published.|
|tracesTopic|No|The Kafka topic to which the event traces are sent.|

## Telemetry

A few OpenTelemetry observable counters are emitted every minute. The following table shows the counters.

|Counter|Description|
|---|---|
|http.server.average_duration_millis|The average request duration in the measured interval.|
|http.server.average_request_bytes|The average request body size in bytes in the measured interval.|
|http.server.average_response_bytes|The average response body size in bytes in the measured interval.|
|http.server.requests|The number of requests during the measured interval.|

The following attributes are added to the counters.

|Attribute|Description|
|---|---|
|http.request.method|The request method.|
|http.response.status_code|The status code of the response.|
|instance|The UUID of the JES HTTP instance.|

The logs are also sent to the OpenTelemetry endpoint.

The event traces are JSON messages, as described in [JSON Streams Telemetry](https://jsonstreams.io/docs/logging.html). They are sent to the Kafka topic set in the `tracesTopic` configuration field.

## Building and Running

You can build the tool with `mvn clean package`. This will produce a self-contained JAR-file in the `target` directory with the form `pincette-letter-box-<version>-jar-with-dependencies.jar`. You can launch this JAR with `java -jar`.

## Docker

Docker images can be found at [https://hub.docker.com/repository/docker/wdonne/pincette-letter-box](https://hub.docker.com/repository/docker/wdonne/pincette-letter-box).

## Kubernetes

You can mount the configuration in a `ConfigMap` and `Secret` combination. The `ConfigMap` should be mounted at `/conf/application.conf`. You then include the secret in the configuration from where you have mounted it. See also [https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax](https://github.com/lightbend/config/blob/main/HOCON.md#include-syntax).