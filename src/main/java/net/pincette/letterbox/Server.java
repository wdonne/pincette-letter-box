package net.pincette.letterbox;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static java.lang.Boolean.FALSE;
import static java.lang.System.getenv;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.regex.Pattern.compile;
import static net.pincette.config.Util.configValue;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.Util.getUsername;
import static net.pincette.jes.tel.OtelUtil.metrics;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.letterbox.Common.LETTER_BOX;
import static net.pincette.letterbox.Common.LOGGER;
import static net.pincette.letterbox.Common.VERSION;
import static net.pincette.letterbox.Common.namespace;
import static net.pincette.netty.http.Util.simpleResponse;
import static net.pincette.netty.http.Util.wrapMetrics;
import static net.pincette.netty.http.Util.wrapTracing;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.rs.kafka.KafkaSubscriber.subscriber;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.put;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getStackTrace;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.jes.tel.EventTrace;
import net.pincette.jes.tel.HttpMetrics;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.Metrics;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.DequePublisher;
import net.pincette.rs.Source;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Server implements AutoCloseable {
  private static final String ANONYMOUS = "anonymous";
  private static final String AS_STRING = "asString";
  private static final String CN_PATTERN = "cnPattern";
  static final String DEFAULT_HEADER = "X-Forwarded-Tls-Client-Cert-Info";
  private static final Pattern DEFAULT_CN_PATTERN =
      compile("^.*CN=([\\p{IsAlphabetic}\\d\\-.*]+).*$");
  static final String DOMAIN = "domain";
  static final String DOMAIN_FIELD = "_domain";
  private static final String DOMAINS = "domains";
  private static final String HEADER = "header";
  private static final String INSTANCE_ATTRIBUTE = "instance";
  private static final String INSTANCE_ENV = "INSTANCE";
  private static final String KAFKA = "kafka";
  private static final String MESSAGE_TOPIC = "topic";
  private static final String TRACES_TOPIC = "tracesTopic";

  private final Config config;
  private final EventTrace eventTrace;
  private final HttpServer httpServer;
  private final String instance = ofNullable(getenv(INSTANCE_ENV)).orElse(randomUUID().toString());
  private final Map<String, String> attributes = map(pair(INSTANCE_ATTRIBUTE, instance));
  private final int port;
  private final String topic;
  private final String tracesTopic;

  private Server(final int port, final Config config) {
    this.port = port;
    this.config = config;
    topic = config != null ? config.getString(MESSAGE_TOPIC) : null;
    tracesTopic = config != null ? tracesTopic(config) : null;
    eventTrace =
        config != null && tracesTopic != null
            ? new EventTrace()
                .withServiceNamespace(namespace(config))
                .withServiceName(LETTER_BOX)
                .withServiceVersion(VERSION)
                .withName(LETTER_BOX)
            : null;
    httpServer =
        port != -1 && config != null
            ? new HttpServer(
                port,
                wrapTracing(
                    metrics(namespace(config), LETTER_BOX, VERSION, config)
                        .map(m -> wrapMetrics(handler(), metricsSubscriber(m, instance)))
                        .orElseGet(this::handler),
                    LOGGER))
            : null;
  }

  public Server() {
    this(-1, null);
  }

  private static Pattern cnPattern(final Config config) {
    return configValue(config::getString, CN_PATTERN)
        .map(Pattern::compile)
        .orElse(DEFAULT_CN_PATTERN);
  }

  private static List<String> getDomains(
      final HttpRequest request, final String header, final Pattern cnPattern) {
    return ofNullable(request.headers().get(header))
        .map(h -> decode(h, UTF_8).split(","))
        .map(subjects -> getCns(subjects, cnPattern))
        .orElseGet(Collections::emptyList);
  }

  private static List<String> getCns(final String[] subjects, final Pattern cnPattern) {
    return stream(subjects)
        .map(cnPattern::matcher)
        .filter(Matcher::matches)
        .map(m -> m.group(1))
        .toList();
  }

  private static List<String> getSupportedDomains(
      final List<String> domains, final List<String> found) {
    final List<String> result =
        domains.stream().flatMap(d -> getSupportedDomains(d, found)).toList();

    if (result.isEmpty()) {
      LOGGER.warning(() -> "None of the domains " + found + " are allowed");
    }

    return result;
  }

  private static Stream<String> getSupportedDomains(final String domain, final List<String> found) {
    return found.stream().filter(d -> isSupportedDomain(domain, d));
  }

  private static String idValue(final JsonObject json, final String field) {
    return ofNullable(json.getString(field, null)).orElseGet(() -> randomUUID().toString());
  }

  private static boolean isSupportedDomain(final String configured, final String given) {
    return (configured.startsWith("*") && given.endsWith(configured.substring(1)))
        || (!configured.startsWith("*") && !given.startsWith("*") && given.equals(configured));
  }

  private static Subscriber<Metrics> metricsSubscriber(
      final OpenTelemetry metrics, final String instance) {
    return HttpMetrics.subscriber(metrics.getMeter(LETTER_BOX), path -> null, instance);
  }

  private static KafkaProducer<String, JsonObject> producer(final Config config) {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new JsonSerializer());
  }

  private static KafkaProducer<String, String> producerString(final Config config) {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new StringSerializer());
  }

  private static CompletionStage<Throwable> readMessage(
      final Publisher<ByteBuf> requestBody,
      final Deque<JsonObject> publisher,
      final String domain) {
    final CompletableFuture<Throwable> future = new CompletableFuture<>();

    with(requestBody)
        .map(ByteBuf::nioBuffer)
        .map(parseJson())
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(v -> updateMessage(v, domain))
        .get()
        .subscribe(
            lambdaSubscriber(publisher::addFirst, () -> future.complete(null), future::complete));

    return future;
  }

  private static Publisher<ByteBuf> reportException(
      final HttpResponse response, final Throwable t) {
    LOGGER.log(SEVERE, t, t::getMessage);
    response.setStatus(BAD_REQUEST);

    return Source.of(wrappedBuffer(getStackTrace(t).getBytes(UTF_8)));
  }

  private static CompletionStage<Publisher<ByteBuf>> response(
      final HttpResponse response, final HttpResponseStatus status) {
    return simpleResponse(response, status, empty());
  }

  private static String tracesTopic(final Config config) {
    return configValue(config::getString, TRACES_TOPIC).orElse(null);
  }

  private static JsonObject updateMessage(final JsonObject json, final String domain) {
    return createObjectBuilder(json)
        .add(ID, idValue(json, ID))
        .add(CORR, idValue(json, CORR))
        .add(DOMAIN_FIELD, domain)
        .build();
  }

  public void close() {
    httpServer.close();
  }

  private RequestHandler handler() {
    final Pattern cnPattern = cnPattern(config);
    final List<String> domains =
        configValue(config::getStringList, DOMAINS).orElseGet(Collections::emptyList);
    final String header = configValue(config::getString, HEADER).orElse(DEFAULT_HEADER);
    final Deque<JsonObject> publisher = publisher();

    if (domains.isEmpty()) {
      LOGGER.warning("No configured domains found in config");
    }

    return (request, requestBody, response) ->
        Optional.of(request.method())
            .filter(m -> m.equals(POST))
            .map(m -> getDomains(request, header, cnPattern))
            .map(found -> getSupportedDomains(domains, found))
            .filter(doms -> !doms.isEmpty())
            .map(
                doms ->
                    readMessage(requestBody, publisher, doms.get(0))
                        .thenComposeAsync(
                            t ->
                                t == null
                                    ? response(response, ACCEPTED)
                                    : completedFuture(reportException(response, t))))
            .orElseGet(
                () ->
                    response(
                        response, !request.method().equals(POST) ? NOT_IMPLEMENTED : FORBIDDEN));
  }

  private <T> List<ProducerRecord<String, T>> publishMessage(
      final JsonObject json, final Function<JsonObject, T> mapper) {
    final ProducerRecord<String, T> message =
        new ProducerRecord<>(topic, json.getString(ID), mapper.apply(json));
    final ProducerRecord<String, T> trace =
        tracesTopic != null
            ? new ProducerRecord<>(
                tracesTopic, json.getString(CORR), mapper.apply(traceMessage(json)))
            : null;

    return trace != null ? list(message, trace) : list(message);
  }

  private Deque<JsonObject> publisher() {
    final boolean asString = configValue(config::getBoolean, AS_STRING).orElse(FALSE);

    return asString
        ? publisher(v -> string(v, false), () -> producerString(config))
        : publisher(v -> v, () -> producer(config));
  }

  private <T> Deque<JsonObject> publisher(
      final Function<JsonObject, T> mapper, final Supplier<KafkaProducer<String, T>> producer) {
    final DequePublisher<JsonObject> dequePublisher = new DequePublisher<>();

    with(dequePublisher)
        .map(v -> publishMessage(v, mapper))
        .map(flattenList())
        .get()
        .subscribe(subscriber(producer));

    return dequePublisher.getDeque();
  }

  public CompletionStage<Boolean> run() {
    return httpServer.run();
  }

  public void start() {
    httpServer.start();
  }

  private JsonObject traceMessage(final JsonObject json) {
    return eventTrace
        .withTraceId(json.getString(CORR))
        .withTimestamp(now())
        .withAttributes(put(attributes, DOMAIN, json.getString(DOMAIN_FIELD)))
        .withUsername(getUsername(json).orElse(ANONYMOUS))
        .toJson()
        .build();
  }

  public Server withConfig(final Config config) {
    return new Server(port, config);
  }

  public Server withPort(final int port) {
    return new Server(port, config);
  }
}
