package net.pincette.letterbox;

import static java.lang.System.getenv;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.regex.Pattern.compile;
import static net.pincette.config.Util.configValue;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.letterbox.Common.LETTER_BOX;
import static net.pincette.letterbox.Common.LOGGER;
import static net.pincette.letterbox.Common.VERSION;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;

import com.typesafe.config.Config;
import io.netty.handler.codec.http.HttpRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.json.JsonObject;
import net.pincette.letterbox.kafka.Publisher;
import net.pincette.letterbox.kafka.Publisher.Context;
import net.pincette.netty.http.HttpServer;

public class Server implements AutoCloseable {
  private static final String CN_PATTERN = "cnPattern";
  static final String DEFAULT_HEADER = "X-Forwarded-Tls-Client-Cert-Info";
  private static final Pattern DEFAULT_CN_PATTERN =
      compile("^.*CN=([\\p{IsAlphabetic}\\d\\-.*]+).*$");
  static final String DOMAIN = "domain";
  static final String DOMAIN_FIELD = "_domain";
  private static final String DOMAINS = "domains";
  private static final String HEADER = "header";
  private static final String INSTANCE_ENV = "INSTANCE";

  private final Config config;
  private final HttpServer httpServer;
  private final int port;
  private final Publisher publisher;

  @SuppressWarnings("java:S2095") // It is closed in the close method.
  private Server(final int port, final Config config) {
    this.port = port;
    this.config = config;
    publisher =
        config != null
            ? new Publisher()
                .withConfig(config)
                .withServiceName(LETTER_BOX)
                .withServiceVersion(VERSION)
                .withInstance(
                    ofNullable(getenv(INSTANCE_ENV)).orElseGet(() -> randomUUID().toString()))
                .withVerify(verify())
                .withTransform(transform())
                .withTelemetryAttributes(telemetryAttributes())
            : null;
    httpServer =
        port != -1 && publisher != null ? new HttpServer(port, publisher.requestHandler()) : null;
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

  private static Optional<String> getSupportedDomain(
      final List<String> domains, final List<String> found) {
    final List<String> result =
        domains.stream().flatMap(d -> getSupportedDomains(d, found)).toList();

    if (result.isEmpty()) {
      LOGGER.warning(() -> "None of the domains " + found + " are allowed");
    }

    return Optional.of(result).filter(r -> !r.isEmpty()).map(r -> r.get(0));
  }

  private static Stream<String> getSupportedDomains(final String domain, final List<String> found) {
    return found.stream().filter(d -> isSupportedDomain(domain, d));
  }

  private static boolean isSupportedDomain(final String configured, final String given) {
    return (configured.startsWith("*") && given.endsWith(configured.substring(1)))
        || (!configured.startsWith("*") && !given.startsWith("*") && given.equals(configured));
  }

  public void close() {
    httpServer.close();
    publisher.close();
  }

  private Function<HttpRequest, Optional<String>> getSupportedDomain() {
    final Pattern cnPattern = cnPattern(config);
    final List<String> domains =
        configValue(config::getStringList, DOMAINS).orElseGet(Collections::emptyList);
    final String header = configValue(config::getString, HEADER).orElse(DEFAULT_HEADER);

    return request -> getSupportedDomain(domains, getDomains(request, header, cnPattern));
  }

  public CompletionStage<Boolean> run() {
    return httpServer.run();
  }

  public void start() {
    httpServer.start();
  }

  private Function<Context, Map<String, String>> telemetryAttributes() {
    final Function<HttpRequest, Optional<String>> domain = getSupportedDomain();

    return context -> map(pair(DOMAIN, domain.apply(context.request()).orElse(null)));
  }

  private Function<Context, List<JsonObject>> transform() {
    final Function<HttpRequest, Optional<String>> domain = getSupportedDomain();

    return context ->
        list(
            createObjectBuilder(context.message())
                .add(DOMAIN_FIELD, domain.apply(context.request()).orElse(null))
                .build());
  }

  private Predicate<Context> verify() {
    final Function<HttpRequest, Optional<String>> domain = getSupportedDomain();

    return context -> domain.apply(context.request()).isPresent();
  }

  public Server withConfig(final Config config) {
    return new Server(port, config);
  }

  public Server withPort(final int port) {
    return new Server(port, config);
  }
}
