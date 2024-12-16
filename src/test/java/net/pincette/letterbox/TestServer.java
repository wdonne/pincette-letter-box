package net.pincette.letterbox;

import static com.typesafe.config.ConfigFactory.defaultApplication;
import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static com.typesafe.config.ConfigValueFactory.fromIterable;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.net.http.HttpClient.newBuilder;
import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.util.Kafka.topicPartitions;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.letterbox.Server.DEFAULT_HEADER;
import static net.pincette.letterbox.Server.DOMAIN;
import static net.pincette.rs.Box.box;
import static net.pincette.rs.Cancel.cancel;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Util.asValueAsync;
import static net.pincette.rs.kafka.ConsumerEvent.STARTED;
import static net.pincette.rs.kafka.Util.createTopics;
import static net.pincette.rs.kafka.Util.deleteTopics;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.ScheduledCompletionStage.composeAsyncAfter;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.admin.Admin.create;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.typesafe.config.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import javax.json.JsonObject;
import net.pincette.kafka.json.JsonDeserializer;
import net.pincette.rs.kafka.KafkaPublisher;
import net.pincette.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

// @Execution(ExecutionMode.CONCURRENT)
class TestServer {
  private static final String BOOTSTRAP_SERVER = "localhost:9092";
  private static final Map<String, Object> COMMON_CONFIG =
      map(pair(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER));
  private static final String TEST_CASE = "testCase";

  private static final Admin admin = create(COMMON_CONFIG);
  private static final HttpClient client = newBuilder().version(HTTP_1_1).build();
  private static final String topic = randomUUID().toString();
  private static final URI uri = URI.create("http://localhost:9000");

  @AfterAll
  static void afterAll() {
    deleteTopics(set(topic), admin).toCompletableFuture().join();
  }

  @BeforeAll
  static void beforeAll() {
    createTopics(set(newTopic(topic)), admin).toCompletableFuture().join();
  }

  private static Config config(final List<String> domains) {
    return defaultApplication()
        .withValue("topic", fromAnyRef(topic))
        .withValue("kafka.bootstrap.servers", fromAnyRef(BOOTSTRAP_SERVER))
        .withValue("domains", fromIterable(domains));
  }

  private static CompletionStage<JsonObject> consume(final String testCase) {
    final KafkaPublisher<String, JsonObject> kafkaPublisher = kafkaPublisher(testCase);

    new Thread(kafkaPublisher::start).start();

    return asValueAsync(consumer(kafkaPublisher, testCase));
  }

  private static Map<String, Object> consumerConfig(final String groupId) {
    return merge(
        COMMON_CONFIG,
        map(
            pair(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
            pair(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class),
            pair(GROUP_ID_CONFIG, groupId),
            pair(ENABLE_AUTO_COMMIT_CONFIG, false)));
  }

  private static Publisher<JsonObject> consumer(
      final KafkaPublisher<String, JsonObject> kafkaPublisher, final String testCase) {
    return with(kafkaPublisher.publishers().get(topic))
        .map(ConsumerRecord::value)
        .map(testCaseProcessor(testCase))
        .get();
  }

  private static KafkaPublisher<String, JsonObject> kafkaPublisher(final String testCase) {
    final Collection<TopicPartition> partitions =
        topicPartitions(topic, admin).toCompletableFuture().join();

    return new KafkaPublisher<String, JsonObject>()
        .withConsumer(() -> new KafkaConsumer<>(consumerConfig(testCase)))
        .withTopics(set(topic))
        .withEventHandler(
            (event, consumer) -> {
              if (event == STARTED) {
                consumer.seekToBeginning(partitions);
              }
            });
  }

  private static NewTopic newTopic(final String name) {
    return new NewTopic(name, 1, (short) 1);
  }

  private static HttpRequest request(
      final JsonObject message, final String cnHeader, final String cnHeaderValue) {
    return HttpRequest.newBuilder()
        .uri(uri)
        .header(cnHeader, cnHeaderValue)
        .header(CONTENT_TYPE.toString(), "application/json")
        .POST(ofString(string(message, false)))
        .build();
  }

  private static CompletionStage<Integer> sendMessage(
      final JsonObject message, final String cnHeader, final String cnHeaderValue) {
    return client
        .sendAsync(request(message, cnHeader, cnHeaderValue), discarding())
        .thenApply(HttpResponse::statusCode);
  }

  private static Server startServer(final Config config) {
    final Server server = new Server().withPort(9000).withConfig(config);

    new Thread(server::start).start();

    return server;
  }

  private static JsonObject stripTechnical(final JsonObject json) {
    return createObjectBuilder(json).remove(CORR).remove(ID).remove(DOMAIN).build();
  }

  private static Pair<JsonObject, Integer> test(
      final Config config,
      final JsonObject message,
      final String cnHeader,
      final String cnHeaderValue,
      final boolean consume) {
    return tryToGetWithRethrow(
            () -> startServer(config),
            server -> {
              final CompletionStage<Integer> statusCode =
                  composeAsyncAfter(
                      () -> sendMessage(message, cnHeader, cnHeaderValue), ofSeconds(1));

              return (consume
                      ? consume(message.getString(TEST_CASE))
                      : completedFuture((JsonObject) null))
                  .thenComposeAsync(response -> statusCode.thenApply(s -> pair(response, s)))
                  .toCompletableFuture()
                  .join();
            })
        .orElse(null);
  }

  private static Processor<JsonObject, JsonObject> testCaseProcessor(final String testCase) {
    return box(filter(json -> testCase.equals(json.getString(TEST_CASE, null))), cancel(v -> true));
  }

  @Test
  @DisplayName("test1")
  void test1() {
    final JsonObject message = o(f(TEST_CASE, v("test1")));
    final Pair<JsonObject, Integer> result =
        test(
            config(list("lemonade.be")),
            message,
            DEFAULT_HEADER,
            "Subject=\"CN=lemonade.be, L=Leuven\"",
            true);

    assertEquals(message, stripTechnical(result.first));
    assertEquals(202, result.second);
  }

  @Test
  @DisplayName("test2")
  void test2() {
    final JsonObject message = o(f(TEST_CASE, v("test2")));
    final Pair<JsonObject, Integer> result =
        test(
            config(list("lemonade.be")),
            message,
            DEFAULT_HEADER,
            "Subject=\"CN=lemo.be, L=Leuven\"",
            false);

    assertNull(result.first);
    assertEquals(403, result.second);
  }

  @Test
  @DisplayName("test3")
  void test3() {
    final JsonObject message = o(f(TEST_CASE, v("test3")), f(CORR, v("corr")), f(ID, v("id")));
    final Pair<JsonObject, Integer> result =
        test(
            config(list("lemonade.be")),
            message,
            DEFAULT_HEADER,
            "Subject=\"CN=lemonade.be, L=Leuven\"",
            true);

    assertEquals(createObjectBuilder(message).add(DOMAIN, "lemonade.be").build(), result.first);
    assertEquals(202, result.second);
  }
}
