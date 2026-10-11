package dk.trustworks.essentials.interop;

import com.acme.billing.InvoiceIssued;
import com.acme.orders.CustomerId;
import com.acme.orders.OrderEvent;
import com.acme.orders.OrderId;
import com.acme.orders.OrderPlaced;
import dk.trustworks.essentials.components.foundation.json.EssentialsObjectMappers;
import dk.trustworks.essentials.components.foundation.messaging.RedeliveryPolicy;
import dk.trustworks.essentials.components.foundation.messaging.queue.*;
import dk.trustworks.essentials.components.foundation.transaction.jdbi.JdbiUnitOfWorkFactory;
import dk.trustworks.essentials.components.queue.postgresql.PostgresqlDurableQueues;
import org.jdbi.v3.core.Jdbi;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Java node of the Phase 6 interop tests: Essentials 0.60.0 {@link PostgresqlDurableQueues} with default settings,
 * driven from the command line. Every event the tests assert on is one JSON object per line on stdout; logging goes
 * to stderr.
 *
 * <pre>
 * harness produce                 --count N --prefix P [--ordered-keys K] [--delay-ms D]
 * harness produce-fixture
 * harness consume                 [--parallel P] [--fail-attempts F] [--redelivery-delay-ms D] [--max-redeliveries R]
 *                                 [--handle-ms H] [--timeout-ms T]
 * harness consume-and-crash-after K  (consume options)
 * harness resurrect               --id ID [--delay-ms D]
 * common: --jdbc-url URL [--user U] [--password P] [--queue Q] [--node NAME]
 * </pre>
 */
public final class Harness {
    private static final ObjectMapper MAPPER = EssentialsObjectMappers.createJackson3ObjectMapper();

    private final Map<String, String> options;
    private final String              node;
    private final QueueName           queueName;

    private Harness(Map<String, String> options) {
        this.options = options;
        this.node = option("node", "java");
        this.queueName = QueueName.of(option("queue", "Orders"));
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: harness <command> [--option value]...");
        }
        var command   = args[0];
        var options   = new HashMap<String, String>();
        var remaining = new ArrayDeque<>(Arrays.asList(args).subList(1, args.length));
        if (command.equals("consume-and-crash-after")) {
            options.put("hang-after", remaining.removeFirst());
        }
        while (!remaining.isEmpty()) {
            var name = remaining.removeFirst();
            if (!name.startsWith("--") || remaining.isEmpty()) {
                throw new IllegalArgumentException("expected --option value, got " + name);
            }
            options.put(name.substring(2), remaining.removeFirst());
        }
        var harness = new Harness(options);
        switch (command) {
            case "produce" -> harness.produce();
            case "produce-fixture" -> harness.produceFixture();
            case "consume", "consume-and-crash-after" -> harness.consume();
            case "resurrect" -> harness.resurrect();
            default -> throw new IllegalArgumentException("unknown command " + command);
        }
    }

    private PostgresqlDurableQueues durableQueues() {
        var jdbi = Jdbi.create(option("jdbc-url", System.getenv("JDBC_URL")),
                               option("user", "postgres"),
                               option("password", "postgres"));
        var builder = PostgresqlDurableQueues.builder()
                                             .setUnitOfWorkFactory(new JdbiUnitOfWorkFactory(jdbi))
                                             .setJsonSerializer(EssentialsObjectMappers.createJSONSerializer());
        if (options.containsKey("timeout-ms")) {
            builder.setMessageHandlingTimeout(Duration.ofMillis(longOption("timeout-ms", 0)));
        }
        var durableQueues = builder.build();
        durableQueues.start();
        return durableQueues;
    }

    private void produce() {
        var durableQueues = durableQueues();
        var count         = (int) longOption("count", 1);
        var prefix        = option("prefix", "java");
        var orderedKeys   = (int) longOption("ordered-keys", 0);
        var delay         = Duration.ofMillis(longOption("delay-ms", 0));
        var messages      = new ArrayList<Message>();
        if (orderedKeys == 0) {
            for (int i = 0; i < count; i++) {
                var payload = new OrderPlaced(OrderId.of(prefix + "-" + i),
                                              CustomerId.of("customer-" + i),
                                              new BigDecimal(i + ".5"),
                                              Instant.parse("2026-10-11T12:00:00.123456Z").plusSeconds(i));
                messages.add(Message.of(payload, metaData(prefix + "-" + i)));
            }
        } else {
            // Orders interleaved across keys, so a consumer that ignores the key order would get them wrong
            for (int order = 0; order < count; order++) {
                for (int k = 0; k < orderedKeys; k++) {
                    var key = prefix + "-key-" + k;
                    messages.add(OrderedMessage.of(new OrderEvent.OrderAdded(OrderId.of(key), order),
                                                   key,
                                                   order,
                                                   metaData(key + "-" + order)));
                }
            }
        }
        for (var message : messages) {
            var id    = durableQueues.queueMessage(queueName, message, delay);
            var event = event("queued");
            event.put("id", id.toString());
            emit(event);
        }
        emit(event("done"));
        durableQueues.stop();
    }

    private MessageMetaData metaData(String correlationId) {
        return MessageMetaData.of("correlationId", correlationId, "producer", node);
    }

    /**
     * The rows of {@code docs/fixtures/java_rows.sql}, queued through the real API so the two can be compared.
     * The fixture's row 7 (a payload that does not match its type) cannot be produced by Java's serializer.
     */
    private void produceFixture() {
        var durableQueues = durableQueues();
        var orders        = QueueName.of("Orders");
        durableQueues.queueMessage(orders, Message.of(
                new OrderPlaced(OrderId.of("order-1"), CustomerId.of("customer-1"), new BigDecimal("125.5"),
                                Instant.parse("2026-10-11T12:00:00Z")),
                MessageMetaData.of("correlationId", "corr-1", "essentials.causedByEventId", "evt-42")));
        durableQueues.queueMessage(orders, Message.of(
                new OrderPlaced(OrderId.of("order-2"), CustomerId.of("customer-2"), new BigDecimal("10"),
                                Instant.parse("2026-10-11T12:00:01Z"))));
        durableQueues.queueMessage(orders,
                                   Message.of(new OrderEvent.OrderCancelled(OrderId.of("order-3"), "customer request")),
                                   Duration.ofHours(1));
        durableQueues.queueMessage(orders, OrderedMessage.of(new OrderEvent.OrderAdded(OrderId.of("order-4"), 0), "order-4", 0));
        durableQueues.queueMessage(orders, OrderedMessage.of(new OrderEvent.OrderAdded(OrderId.of("order-4"), 1), "order-4", 1));
        durableQueues.queueMessageAsDeadLetterMessage(orders, Message.of(
                new OrderPlaced(OrderId.of("order-5"), CustomerId.of("customer-5"), new BigDecimal("1"),
                                Instant.parse("2026-10-11T12:00:02Z"))), new IllegalStateException("boom"));
        durableQueues.queueMessage(QueueName.of("Invoices"), Message.of(new InvoiceIssued("inv-1")));
        emit(event("done"));
        durableQueues.stop();
    }

    private void consume() throws InterruptedException {
        var durableQueues  = durableQueues();
        var failAttempts   = longOption("fail-attempts", 0);
        var handleMillis   = longOption("handle-ms", 0);
        var hangAfter      = longOption("hang-after", -1);
        var handled        = new AtomicInteger();
        var policy = RedeliveryPolicy.fixedBackoff(Duration.ofMillis(longOption("redelivery-delay-ms", 100)),
                                                   (int) longOption("max-redeliveries", 5));
        QueuedMessageHandler handler = message -> {
            if (hangAfter >= 0 && handled.get() >= hangAfter) {
                var event = event("hung");
                event.put("id", message.getId().toString());
                emit(event);
                sleep(Long.MAX_VALUE);
            }
            if (message.getTotalDeliveryAttempts() <= failAttempts) {
                var event = describe("failed", message);
                emit(event);
                throw new IllegalStateException("interop: failing delivery attempt " + message.getTotalDeliveryAttempts());
            }
            sleep(handleMillis);
            handled.incrementAndGet();
            emit(describe("handled", message));
        };
        var consumer = durableQueues.consumeFromQueue(queueName, policy, (int) longOption("parallel", 1), handler);
        var stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.stop();
            durableQueues.stop();
            stopped.countDown();
        }));
        emit(event("ready"));
        stopped.await();
    }

    private void resurrect() {
        var durableQueues = durableQueues();
        var resurrected = durableQueues.resurrectDeadLetterMessage(QueueEntryId.of(option("id", null)),
                                                                   Duration.ofMillis(longOption("delay-ms", 0)));
        var event = event("resurrected");
        event.put("found", resurrected.isPresent());
        emit(event);
        durableQueues.stop();
    }

    private ObjectNode describe(String eventName, QueuedMessage message) {
        var event = event(eventName);
        event.put("id", message.getId().toString());
        event.put("type", message.getPayload().getClass().getName());
        event.set("payload", MAPPER.valueToTree(message.getPayload()));
        event.set("metadata", MAPPER.valueToTree(new TreeMap<>(message.getMetaData())));
        event.put("totalAttempts", message.getTotalDeliveryAttempts());
        event.put("redeliveryAttempts", message.getRedeliveryAttempts());
        event.put("deliveryMode", message.getDeliveryMode().name());
        if (message.getMessage() instanceof OrderedMessage ordered) {
            event.put("key", ordered.getKey());
            event.put("order", ordered.getOrder());
        }
        return event;
    }

    private ObjectNode event(String name) {
        var event = MAPPER.createObjectNode();
        event.put("event", name);
        event.put("node", node);
        return event;
    }

    private static synchronized void emit(ObjectNode event) {
        System.out.println(MAPPER.writeValueAsString(event));
        System.out.flush();
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private String option(String name, String defaultValue) {
        var value = options.getOrDefault(name, defaultValue);
        if (value == null) {
            throw new IllegalArgumentException("missing --" + name);
        }
        return value;
    }

    private long longOption(String name, long defaultValue) {
        return options.containsKey(name) ? Long.parseLong(options.get(name)) : defaultValue;
    }
}
