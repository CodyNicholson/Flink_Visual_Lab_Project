package com.example.flinklab.flink;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PostgreSQL logical replication (WAL CDC) source. Reads decoded WAL changes for
 * customers_cdc_source from a replication slot using the built-in test_decoding plugin, without
 * ever querying the table, and emits changelog records: op|customerId|tier|creditLimit|rawWal
 * where op is +I, -U (before image), +U (after image) or -D.
 *
 * The slot's confirmed LSN is advanced as soon as a change is emitted, so this source is
 * at-least-once friendly rather than exactly-once; good enough for a teaching lab.
 */
public class PostgresWalCdcSource extends RichSourceFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresWalCdcSource.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private volatile boolean running = true;

    static final String SLOT_NAME = "flink_cdc_lab_slot";
    private static final String TABLE_MARKER = "table public.customers_cdc_source:";
    private static final Pattern ID_PATTERN = Pattern.compile("customer_id\\[character varying\\]:'([^']+)'");
    private static final Pattern TIER_PATTERN = Pattern.compile("tier\\[character varying\\]:'([^']+)'");
    private static final Pattern LIMIT_PATTERN = Pattern.compile("credit_limit\\[numeric\\]:([0-9.]+)");

    public PostgresWalCdcSource(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (running) {
            try {
                ensureReplicationSlot();
                streamWal(ctx);
            } catch (Exception e) {
                if (!running) {
                    break;
                }
                // e.g. the slot is still held by a previous (crashed) walsender; retry shortly
                LOG.warn("WAL CDC stream interrupted ({}); reconnecting in 3s", e.getMessage());
                Thread.sleep(3000);
            }
        }
    }

    private void ensureReplicationSlot() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement check = conn.prepareStatement(
                     "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            check.setString(1, SLOT_NAME);
            try (ResultSet rs = check.executeQuery()) {
                if (rs.next()) {
                    return;
                }
            }
            try (PreparedStatement create = conn.prepareStatement(
                    "SELECT pg_create_logical_replication_slot(?, 'test_decoding')")) {
                create.setString(1, SLOT_NAME);
                create.execute();
                LOG.info("Created logical replication slot {}", SLOT_NAME);
            }
        }
    }

    private void streamWal(SourceContext<String> ctx) throws Exception {
        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        try (Connection replConn = DriverManager.getConnection(jdbcUrl, props)) {
            PGConnection pgConnection = replConn.unwrap(PGConnection.class);
            PGReplicationStream stream = pgConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(SLOT_NAME)
                    .withSlotOption("include-xids", false)
                    .withSlotOption("skip-empty-xacts", true)
                    .withStatusInterval(10, TimeUnit.SECONDS)
                    .start();
            LOG.info("Streaming WAL changes from slot {}", SLOT_NAME);

            try {
                while (running) {
                    ByteBuffer buffer = stream.readPending();
                    if (buffer == null) {
                        Thread.sleep(100);
                        continue;
                    }

                    String walRecord = StandardCharsets.UTF_8.decode(buffer).toString();
                    // BEGIN/COMMIT and other tables arrive on the same slot; keep only our table
                    if (walRecord.startsWith(TABLE_MARKER)) {
                        synchronized (ctx.getCheckpointLock()) {
                            emitChangelog(walRecord, ctx);
                        }
                    }

                    stream.setAppliedLSN(stream.getLastReceiveLSN());
                    stream.setFlushedLSN(stream.getLastReceiveLSN());
                }
            } finally {
                stream.close();
            }
        }
    }

    private void emitChangelog(String walRecord, SourceContext<String> ctx) {
        if (walRecord.contains(": INSERT:")) {
            ctx.collect(toRecord("+I", walRecord, walRecord));
        } else if (walRecord.contains(": UPDATE:")) {
            // REPLICA IDENTITY FULL: "UPDATE: old-key: <before row> new-tuple: <after row>"
            int split = walRecord.indexOf("new-tuple:");
            if (split > 0) {
                ctx.collect(toRecord("-U", walRecord.substring(0, split), walRecord));
                ctx.collect(toRecord("+U", walRecord.substring(split), walRecord));
            } else {
                ctx.collect(toRecord("+U", walRecord, walRecord));
            }
        } else if (walRecord.contains(": DELETE:")) {
            ctx.collect(toRecord("-D", walRecord, walRecord));
        }
    }

    private static String toRecord(String op, String rowImage, String rawWal) {
        return op + "|" + extract(ID_PATTERN, rowImage, "UNKNOWN")
                + "|" + extract(TIER_PATTERN, rowImage, "UNKNOWN")
                + "|" + extract(LIMIT_PATTERN, rowImage, "0")
                + "|" + rawWal;
    }

    private static String extract(Pattern pattern, String text, String fallback) {
        Matcher m = pattern.matcher(text);
        return m.find() ? m.group(1) : fallback;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
