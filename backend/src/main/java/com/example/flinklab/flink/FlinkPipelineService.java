package com.example.flinklab.flink;

import com.example.flinklab.model.FlinkTraceEvent;
import com.example.flinklab.model.StreamRecordPayload;
import com.example.flinklab.service.TelemetryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

@Service
public class FlinkPipelineService {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Allowed event-time disorder before the watermark passes an event (BoundedOutOfOrderness). */
    static final long OUT_OF_ORDERNESS_MS = 2_000L;
    /** How far behind the wall clock the watermark trails while a source is idle. */
    static final long IDLE_WATERMARK_LAG_MS = 10_000L;

    @Value("${spring.datasource.url}")
    private String jdbcUrl;

    @Value("${spring.datasource.username}")
    private String dbUser;

    @Value("${spring.datasource.password}")
    private String dbPass;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrap;

    @Value("${flink.shared-dir}")
    private String sharedDir;

    @Value("${flink.web-ui-port}")
    private int webUiPort;

    @EventListener(ApplicationReadyEvent.class)
    public void startFlinkTopology() {
        Thread flinkRunner = new Thread(() -> {
            try {
                runPipeline();
            } catch (Exception e) {
                LOG.error("Embedded Flink job terminated", e);
            }
        }, "flink-embedded-job-runner");
        flinkRunner.setDaemon(true);
        flinkRunner.start();
    }

    private void runPipeline() throws Exception {
        File inboxDir = new File(sharedDir, "inbox");
        File outboxDir = new File(sharedDir, "outbox");
        File checkpointDir = new File(sharedDir, "checkpoints");
        inboxDir.mkdirs();
        outboxDir.mkdirs();
        checkpointDir.mkdirs();

        // Embedded MiniCluster with its own Web Dashboard, file-backed checkpoints and
        // unlimited restarts (so a dropped Postgres/Kafka connection only restarts the job)
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, webUiPort);
        conf.set(RestOptions.BIND_ADDRESS, "0.0.0.0");
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.enableCheckpointing(10000L); // Checkpoint every 10 seconds

        // 1. KAFKA SOURCE
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("flink-orders-topic")
                .setGroupId("flink-lab-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawKafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source"
        );

        // 2. LOCAL FILE SYSTEM SOURCE (monitors /data/flink-fs/inbox continuously every 1s)
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inboxDir.getAbsolutePath()))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        DataStream<String> rawFileStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "FileSystem-Directory-Source"
        );

        // Unify Kafka and FileSystem JSON streams and assign Event-Time Watermarks
        DataStream<StreamRecordPayload> unifiedEvents = rawKafkaStream
                .union(rawFileStream)
                .flatMap((String json, Collector<StreamRecordPayload> out) -> {
                    try {
                        out.collect(MAPPER.readValue(json, StreamRecordPayload.class));
                    } catch (Exception e) {
                        LOG.warn("Skipping malformed record: {}", json);
                    }
                })
                .returns(StreamRecordPayload.class)
                .name("JSON-Deserializer")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StreamRecordPayload>forGenerator(ctx -> new LabWatermarkGenerator<>())
                                .withTimestampAssigner((event, ts) -> event.getEventTimestamp())
                )
                .name("Event-Time-Watermarks");

        // 3. POSTGRESQL WAL CHANGE DATA CAPTURE (CDC) SOURCE
        // CDC changes carry no business timestamp, so they are stamped with ingestion time. They
        // still need watermarks: a two-input operator's watermark is the minimum of both inputs.
        DataStream<String> postgresCdcStream = env
                .addSource(new PostgresWalCdcSource(jdbcUrl, dbUser, dbPass))
                .name("Postgres-WAL-CDC-Source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forGenerator(ctx -> new LabWatermarkGenerator<>())
                                .withTimestampAssigner((cdc, ts) -> System.currentTimeMillis())
                )
                .name("CDC-Ingestion-Time-Watermarks");

        // 4. STATEFUL CO-PROCESS OPERATOR (Joins CDC Changelog State + Stream Events + Watermarks)
        DataStream<FlinkTraceEvent> processedStream = unifiedEvents
                .keyBy(StreamRecordPayload::getCustomerId, Types.STRING)
                .connect(postgresCdcStream.keyBy(cdc -> cdc.split("\\|")[1], Types.STRING))
                .process(new StatefulStreamCdcProcessor(jdbcUrl, dbUser, dbPass))
                .name("Stateful-CDC-Join");

        // 5. DUAL SINKS: Postgres Upsert Sink + Local FileSystem Outbox Sink
        processedStream
                .addSink(new MultiTargetSink(jdbcUrl, dbUser, dbPass, outboxDir.getAbsolutePath()))
                .name("Postgres-and-FileSystem-Sink");

        LOG.info("Submitting embedded Flink job (dashboard on port {})", webUiPort);
        env.execute("Flink-Interactive-Visual-Lab-Job");
    }

    /**
     * BoundedOutOfOrderness watermarks (max event time - 2s) that also follow the wall clock at a
     * 10s lag while no events arrive. A plain bounded-out-of-orderness watermark stays at
     * Long.MIN_VALUE until the first event, and an idle CDC input would hold the joined operator's
     * watermark there forever; with the clock floor a backdated event is reliably detected as late.
     */
    static class LabWatermarkGenerator<T> implements WatermarkGenerator<T> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            long clockWatermark = System.currentTimeMillis() - IDLE_WATERMARK_LAG_MS;
            long eventWatermark = maxTimestamp == Long.MIN_VALUE
                    ? Long.MIN_VALUE
                    : maxTimestamp - OUT_OF_ORDERNESS_MS - 1;
            output.emitWatermark(new Watermark(Math.max(clockWatermark, eventWatermark)));
        }
    }

    /**
     * Stateful KeyedCoProcessFunction demonstrating:
     * - ValueState for cumulative spend per key
     * - ValueState for live Postgres CDC Customer Tier
     * - Event-Time Watermark inspection (On-Time vs Out-of-Order Late Event)
     * - Checkpoint completion notifications (state snapshots)
     */
    public static class StatefulStreamCdcProcessor
            extends KeyedCoProcessFunction<String, StreamRecordPayload, String, FlinkTraceEvent>
            implements CheckpointListener {

        private final String jdbcUrl;
        private final String dbUser;
        private final String dbPass;

        private transient ValueState<Double> runningTotalState;
        private transient ValueState<String> customerTierState;

        public StatefulStreamCdcProcessor(String jdbcUrl, String dbUser, String dbPass) {
            this.jdbcUrl = jdbcUrl;
            this.dbUser = dbUser;
            this.dbPass = dbPass;
        }

        @Override
        public void open(Configuration parameters) {
            runningTotalState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("running-customer-total", Double.class));
            customerTierState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("cdc-customer-tier", String.class));
        }

        @Override
        public void processElement1(
                StreamRecordPayload event,
                Context ctx,
                Collector<FlinkTraceEvent> out
        ) throws Exception {
            // The sleeps between stages only pace the telemetry so the UI can animate each hop

            // Stage 1: Emit Source Ingestion Telemetry
            TelemetryService.broadcast(new FlinkTraceEvent(
                    event.getEventId(),
                    event.getSourceType(),
                    "SOURCE_INGEST",
                    event.getCustomerId(),
                    "+I",
                    event.getAmount(),
                    0.0,
                    "PENDING",
                    "EventTS=" + event.getEventTimestamp(),
                    String.format("Flink Source ingested %s record for %s ($%.2f).",
                            event.getSourceType(), event.getCustomerId(), event.getAmount()),
                    System.currentTimeMillis()
            ));
            Thread.sleep(450);

            // Stage 2: Evaluate Event-Time Watermark (an element is late once ts <= watermark)
            long currentWatermark = ctx.timerService().currentWatermark();
            boolean isLate = currentWatermark > Long.MIN_VALUE && event.getEventTimestamp() <= currentWatermark;
            String watermarkStatus = isLate ? "LATE_OUT_OF_ORDER" : "ON_TIME_WATERMARK";
            long skewSeconds = currentWatermark > Long.MIN_VALUE
                    ? (currentWatermark - event.getEventTimestamp()) / 1000
                    : 0;

            TelemetryService.broadcast(new FlinkTraceEvent(
                    event.getEventId(),
                    event.getSourceType(),
                    "WATERMARK_ASSIGN",
                    event.getCustomerId(),
                    "+I",
                    event.getAmount(),
                    0.0,
                    "EVALUATING",
                    watermarkStatus,
                    isLate
                            ? String.format("Late event! Its timestamp is %ds behind the operator watermark. An event-time "
                                    + "window would drop it; this ProcessFunction keeps it and tags it LATE_OUT_OF_ORDER.",
                                    skewSeconds)
                            : "Event timestamp is ahead of the operator watermark: on time.",
                    System.currentTimeMillis()
            ));
            Thread.sleep(450);

            // Stage 3: Keyed ValueState Update & Postgres CDC State Enrichment
            Double prevTotal = runningTotalState.value();
            double newTotal = (prevTotal == null ? 0.0 : prevTotal) + event.getAmount();
            runningTotalState.update(newTotal);

            String tier = customerTierState.value();
            if (tier == null) {
                // Bootstrap state for keys whose rows existed before the CDC slot was created
                tier = lookupInitialTierFromPostgres(event.getCustomerId());
                customerTierState.update(tier);
            }

            FlinkTraceEvent enriched = new FlinkTraceEvent(
                    event.getEventId(),
                    event.getSourceType(),
                    "KEYED_STATE_JOIN",
                    event.getCustomerId(),
                    "+I",
                    event.getAmount(),
                    newTotal,
                    tier,
                    watermarkStatus,
                    String.format("Keyed ValueState updated for %s: Running Spend = $%.2f | Joined Tier = %s.",
                            event.getCustomerId(), newTotal, tier),
                    System.currentTimeMillis()
            );
            TelemetryService.broadcast(enriched);
            Thread.sleep(450);

            out.collect(enriched);
        }

        @Override
        public void processElement2(
                String cdcPayload,
                Context ctx,
                Collector<FlinkTraceEvent> out
        ) throws Exception {
            // Format: op|customerId|tier|creditLimit|rawWal
            String[] parts = cdcPayload.split("\\|", 5);
            String op = parts[0];
            String custId = parts[1];
            String tier = parts[2];
            String creditLimit = parts[3];

            String description;
            switch (op) {
                case "-U" -> description = String.format(
                        "Postgres WAL captured UPDATE on customers_cdc_source. -U retracts the old row image for %s (Tier=%s, Limit $%s).",
                        custId, tier, creditLimit);
                case "-D" -> {
                    customerTierState.clear();
                    description = String.format(
                            "Postgres WAL captured DELETE for %s. -D cleared the cached tier from Keyed State.", custId);
                }
                default -> {
                    customerTierState.update(tier);
                    description = String.format(
                            "Postgres WAL captured %s on customers_cdc_source. Keyed State for %s updated to Tier=%s (Limit $%s).",
                            op, custId, tier, creditLimit);
                }
            }

            Double currentSpend = runningTotalState.value();
            TelemetryService.broadcast(new FlinkTraceEvent(
                    "CDC-" + System.currentTimeMillis() % 100000,
                    "POSTGRES_CDC",
                    "CDC_SYNC",
                    custId,
                    op,
                    Double.parseDouble(creditLimit),
                    currentSpend == null ? 0.0 : currentSpend,
                    tier,
                    "WAL_LSN_STREAM",
                    description,
                    System.currentTimeMillis()
            ));
            Thread.sleep(300);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            TelemetryService.broadcast(new FlinkTraceEvent(
                    "CHK-" + checkpointId,
                    "WATERMARK_CHECKPOINT",
                    "CHECKPOINT_COMPLETE",
                    null,
                    "",
                    0.0,
                    0.0,
                    null,
                    "",
                    String.format("Checkpoint #%d complete: barriers flowed through every operator and the keyed "
                            + "ValueState snapshot was written to /data/flink-fs/checkpoints.", checkpointId),
                    System.currentTimeMillis()
            ));
        }

        private String lookupInitialTierFromPostgres(String customerId) {
            try (Connection c = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
                 PreparedStatement ps = c.prepareStatement(
                         "SELECT tier FROM customers_cdc_source WHERE customer_id = ?")) {
                ps.setString(1, customerId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Tier lookup for {} failed: {}", customerId, e.getMessage());
            }
            return "STANDARD";
        }
    }

    /**
     * Sink Function that persists enriched Flink stream records to PostgreSQL and the Local File System Outbox
     */
    public static class MultiTargetSink extends RichSinkFunction<FlinkTraceEvent> {
        private final String jdbcUrl;
        private final String dbUser;
        private final String dbPass;
        private final String outboxPath;

        private transient Connection conn;
        private transient PreparedStatement upsert;

        public MultiTargetSink(String jdbcUrl, String dbUser, String dbPass, String outboxPath) {
            this.jdbcUrl = jdbcUrl;
            this.dbUser = dbUser;
            this.dbPass = dbPass;
            this.outboxPath = outboxPath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
            upsert = conn.prepareStatement(
                    "INSERT INTO enriched_orders_sink " +
                    "(order_id, customer_id, source_type, amount, customer_tier, running_customer_total, watermark_status) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (order_id) DO UPDATE SET " +
                    "running_customer_total = EXCLUDED.running_customer_total, customer_tier = EXCLUDED.customer_tier, " +
                    "watermark_status = EXCLUDED.watermark_status, processed_at = CURRENT_TIMESTAMP");
        }

        @Override
        public void invoke(FlinkTraceEvent value, Context context) throws Exception {
            // 1. Upsert into PostgreSQL Sink Table
            upsert.setString(1, value.eventId());
            upsert.setString(2, value.customerId());
            upsert.setString(3, value.sourceType());
            upsert.setDouble(4, value.amount());
            upsert.setString(5, value.customerTier());
            upsert.setDouble(6, value.runningTotal());
            upsert.setString(7, value.watermarkInfo());
            upsert.executeUpdate();

            // 2. Append to Local FileSystem Outbox Log
            File outFile = new File(outboxPath, "flink-processed-sink.jsonl");
            String line = MAPPER.writeValueAsString(value) + "\n";
            Files.writeString(outFile.toPath(), line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            // 3. Notify React UI of Sink Commit
            TelemetryService.broadcast(new FlinkTraceEvent(
                    value.eventId(),
                    value.sourceType(),
                    "SINK_COMMIT",
                    value.customerId(),
                    "+I",
                    value.amount(),
                    value.runningTotal(),
                    value.customerTier(),
                    value.watermarkInfo(),
                    String.format("Flink Sink upserted Order %s into Postgres 'enriched_orders_sink' & appended it to '/data/flink-fs/outbox'.",
                            value.eventId()),
                    System.currentTimeMillis()
            ));
        }

        @Override
        public void close() throws Exception {
            if (upsert != null) {
                upsert.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
