package net.pincette.letterbox;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.logging.Level.SEVERE;
import static java.util.regex.Pattern.compile;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.fromConfig;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.letterbox.Application.LOGGER;
import static net.pincette.netty.http.Util.simpleResponse;
import static net.pincette.netty.http.Util.wrapTracing;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.asValueAsync;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.tryToGetSilent;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.HttpServer;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.Source;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Server implements AutoCloseable {
  private static final String AS_STRING = "asString";
  private static final String CN_PATTERN = "cnPattern";
  static final String DEFAULT_HEADER = "X-Forwarded-Tls-Client-Cert-Info";
  private static final Pattern DEFAULT_CN_PATTERN =
      compile("^.*CN=([\\p{IsAlphabetic}\\d\\-.*]+).*$");
  static final String DOMAIN = "_domain";
  private static final String DOMAINS = "domains";
  private static final String HEADER = "header";
  private static final String KAFKA = "kafka";
  private static final String TOPIC = "topic";

  private final Config config;
  private final HttpServer httpServer;
  private final int port;

  private Server(final int port, final Config config) {
    this.port = port;
    this.config = config;
    httpServer =
        port != -1 && config != null
            ? new HttpServer(port, wrapTracing(handler(config), LOGGER))
            : null;
  }

  public Server() {
    this(-1, null);
  }

  private static Pattern cnPattern(final Config config) {
    return tryToGetSilent(() -> config.getString(CN_PATTERN))
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

  private static KafkaProducer<String, JsonObject> producer(final Config config) {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new JsonSerializer());
  }

  private static KafkaProducer<String, String> producerString(final Config config) {
    return createReliableProducer(
        fromConfig(config, KAFKA), new StringSerializer(), new StringSerializer());
  }

  private static CompletionStage<JsonObject> readMessage(final Publisher<ByteBuf> requestBody) {
    return asValueAsync(
            with(requestBody)
                .map(ByteBuf::nioBuffer)
                .map(parseJson())
                .filter(JsonUtil::isObject)
                .map(JsonValue::asJsonObject)
                .get())
        .exceptionally(t -> null);
  }

  private static Publisher<ByteBuf> reportException(
      final HttpResponse response, final Throwable t) {
    LOGGER.log(SEVERE, t, t::getMessage);
    response.setStatus(INTERNAL_SERVER_ERROR);

    return Source.of(wrappedBuffer(getStackTrace(t).getBytes(UTF_8)));
  }

  private static CompletionStage<Publisher<ByteBuf>> response(
      final HttpResponse response, final HttpResponseStatus status) {
    return simpleResponse(response, status, empty());
  }

  private static Function<JsonObject, CompletionStage<Boolean>> sendMessage(final Config config) {
    final boolean asString = tryToGetSilent(() -> config.getBoolean(AS_STRING)).orElse(FALSE);
    final KafkaProducer<String, JsonObject> producer = asString ? null : producer(config);
    final KafkaProducer<String, String> producerString = asString ? producerString(config) : null;
    final String topic = config.getString(TOPIC);

    return asString
        ? (message ->
            send(
                producerString,
                new ProducerRecord<>(topic, randomUUID().toString(), string(message, false))))
        : (message ->
            send(producer, new ProducerRecord<>(topic, randomUUID().toString(), message)));
  }

  private static JsonObject updateMessage(final JsonObject json, final String domain) {
    return createObjectBuilder(json)
        .add(ID, idValue(json, ID))
        .add(CORR, idValue(json, CORR))
        .add(DOMAIN, domain)
        .build();
  }

  public void close() {
    httpServer.close();
  }

  private RequestHandler handler(final Config config) {
    final Pattern cnPattern = cnPattern(config);
    final List<String> domains =
        tryToGetSilent(() -> config.getStringList(DOMAINS)).orElseGet(Collections::emptyList);
    final String header = tryToGetSilent(() -> config.getString(HEADER)).orElse(DEFAULT_HEADER);
    final Function<Boolean, HttpResponseStatus> result =
        r -> TRUE.equals(r) ? ACCEPTED : INTERNAL_SERVER_ERROR;
    final Function<JsonObject, CompletionStage<Boolean>> sendMessage = sendMessage(config);

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
                    readMessage(requestBody)
                        .thenComposeAsync(
                            json ->
                                json != null
                                    ? sendMessage
                                        .apply(updateMessage(json, doms.get(0)))
                                        .thenComposeAsync(
                                            v ->
                                                response(response, result.apply(v))
                                                    .exceptionally(
                                                        t -> reportException(response, t)))
                                    : response(response, BAD_REQUEST)))
            .orElseGet(
                () ->
                    response(
                        response, !request.method().equals(POST) ? NOT_IMPLEMENTED : FORBIDDEN));
  }

  public CompletionStage<Boolean> run() {
    return httpServer.run();
  }

  public void start() {
    httpServer.start();
  }

  public Server withConfig(final Config config) {
    return new Server(port, config);
  }

  public Server withPort(final int port) {
    return new Server(port, config);
  }
}
