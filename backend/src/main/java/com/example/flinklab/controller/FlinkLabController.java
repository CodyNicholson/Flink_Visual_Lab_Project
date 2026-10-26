package com.example.flinklab.controller;

import com.example.flinklab.model.StreamRecordPayload;
import com.example.flinklab.service.TelemetryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@RestController
@RequestMapping("/api/flink")
public class FlinkLabController {

    private static final String ORDERS_TOPIC = "flink-orders-topic";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final TelemetryService telemetryService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${flink.shared-dir}")
    private String sharedDir;

    public FlinkLabController(
            KafkaTemplate<String, String> kafkaTemplate,
            JdbcTemplate jdbcTemplate,
            TelemetryService telemetryService
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.telemetryService = telemetryService;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamTelemetry() {
        return telemetryService.registerClient();
    }

    /**
     * Button 1: Send event into Apache Kafka Topic (Consumed by Flink KafkaSource)
     */
    @PostMapping("/trigger/kafka")
    public Map<String, Object> triggerKafkaEvent(
            @RequestParam(defaultValue = "CUST-101") String customerId,
            @RequestParam(defaultValue = "250.00") double amount
    ) throws Exception {
        String eventId = "KFK-" + shortId();
        publish(new StreamRecordPayload(eventId, "KAFKA", customerId, amount, System.currentTimeMillis()));
        return Map.of("status", "sent_to_kafka", "eventId", eventId);
    }

    /**
     * Button 2: Execute SQL UPDATE/INSERT in Postgres to trigger Flink WAL CDC Connector
     */
    @PostMapping("/trigger/postgres-cdc")
    public Map<String, Object> triggerPostgresCdc(
            @RequestParam(defaultValue = "CUST-101") String customerId,
            @RequestParam(defaultValue = "VIP_DIAMOND") String newTier
    ) {
        int updated = jdbcTemplate.update(
                "UPDATE customers_cdc_source SET tier = ?, credit_limit = credit_limit + 5000, updated_at = NOW() WHERE customer_id = ?",
                newTier, customerId
        );
        if (updated == 0) {
            jdbcTemplate.update(
                    "INSERT INTO customers_cdc_source (customer_id, full_name, tier, credit_limit) VALUES (?, ?, ?, ?)",
                    customerId, "New CDC Customer", newTier, 20000.00
            );
        }
        return Map.of("status", "postgres_wal_mutated", "operation", updated == 0 ? "INSERT" : "UPDATE",
                "customerId", customerId, "newTier", newTier);
    }

    /**
     * Button 3: Write a JSONL file into /data/flink-fs/inbox for Flink FileSource to ingest
     */
    @PostMapping("/trigger/filesystem")
    public Map<String, Object> triggerFileSystemDrop(
            @RequestParam(defaultValue = "CUST-202") String customerId,
            @RequestParam(defaultValue = "480.50") double amount
    ) throws Exception {
        String eventId = "FILE-" + shortId();
        StreamRecordPayload payload = new StreamRecordPayload(
                eventId, "FILESYSTEM", customerId, amount, System.currentTimeMillis()
        );
        File inbox = new File(sharedDir, "inbox");
        inbox.mkdirs();
        // Write under a hidden name first (FileSource skips files starting with '.') and then
        // rename, so the directory monitor never picks up a half-written file
        File tmpFile = new File(inbox, ".batch-" + eventId + ".jsonl.tmp");
        File dropFile = new File(inbox, "batch-" + eventId + ".jsonl");
        Files.writeString(tmpFile.toPath(), objectMapper.writeValueAsString(payload) + "\n");
        Files.move(tmpFile.toPath(), dropFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return Map.of("status", "file_dropped", "file", dropFile.getName(), "eventId", eventId);
    }

    /**
     * Button 4: Trigger Stateful Postgres Lookup Enrichment & JDBC Sink
     */
    @PostMapping("/trigger/postgres-jdbc")
    public Map<String, Object> triggerPostgresJdbcJoin(
            @RequestParam(defaultValue = "CUST-101") String customerId,
            @RequestParam(defaultValue = "920.00") double amount
    ) throws Exception {
        String eventId = "PGJOIN-" + shortId();
        publish(new StreamRecordPayload(eventId, "JDBC_ENRICH", customerId, amount, System.currentTimeMillis()));
        return Map.of("status", "jdbc_join_dispatched", "eventId", eventId);
    }

    /**
     * Button 5: Send Out-of-Order Backdated Event to test Flink Watermarks & Checkpointing
     */
    @PostMapping("/trigger/late-watermark")
    public Map<String, Object> triggerLateEvent(
            @RequestParam(defaultValue = "CUST-202") String customerId,
            @RequestParam(defaultValue = "99.99") double amount
    ) throws Exception {
        String eventId = "LATE-" + shortId();
        // Backdate the event time by 60 seconds, well behind the current stream watermark
        long backdatedTimestamp = System.currentTimeMillis() - 60_000L;
        publish(new StreamRecordPayload(eventId, "WATERMARK_CHECKPOINT", customerId, amount, backdatedTimestamp));
        return Map.of("status", "late_event_sent", "eventId", eventId);
    }

    /**
     * Returns live rows from Postgres CDC source table and Enriched Sink table for UI verification
     */
    @GetMapping("/state")
    public Map<String, Object> getDatabaseAndFileState() throws Exception {
        List<Map<String, Object>> customers = jdbcTemplate.queryForList(
                "SELECT customer_id, full_name, tier, credit_limit FROM customers_cdc_source ORDER BY customer_id"
        );
        List<Map<String, Object>> sinkOrders = jdbcTemplate.queryForList(
                "SELECT order_id, customer_id, source_type, amount, customer_tier, running_customer_total, watermark_status " +
                "FROM enriched_orders_sink ORDER BY processed_at DESC LIMIT 8"
        );
        File inbox = new File(sharedDir, "inbox");
        String[] files = inbox.list((dir, name) -> !name.startsWith("."));
        File outboxFile = new File(new File(sharedDir, "outbox"), "flink-processed-sink.jsonl");
        long outboxLines = 0;
        if (outboxFile.exists()) {
            try (Stream<String> lines = Files.lines(outboxFile.toPath())) {
                outboxLines = lines.count();
            }
        }
        return Map.of(
                "customersCdcTable", customers,
                "enrichedSinkTable", sinkOrders,
                "inboxFileCount", files != null ? files.length : 0,
                "outboxLineCount", outboxLines
        );
    }

    private void publish(StreamRecordPayload payload) throws Exception {
        // Keyed by customer so all of a customer's orders land on the same partition, in order
        kafkaTemplate.send(ORDERS_TOPIC, payload.getCustomerId(), objectMapper.writeValueAsString(payload)).get();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    }
}
