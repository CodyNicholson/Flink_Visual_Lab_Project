package com.example.flinklab.model;

import java.io.Serializable;

public record FlinkTraceEvent(
        String eventId,
        String sourceType,     // KAFKA, POSTGRES_CDC, FILESYSTEM, JDBC_ENRICH, WATERMARK_CHECKPOINT
        String operatorStage,  // SOURCE_INGEST, WATERMARK_ASSIGN, KEYED_STATE_JOIN, SINK_COMMIT, CDC_SYNC, CHECKPOINT_COMPLETE
        String customerId,
        String changelogOp,    // +I (Insert), -U/+U (Update), -D (Delete)
        double amount,
        double runningTotal,
        String customerTier,
        String watermarkInfo,
        String description,
        long timestamp
) implements Serializable {}
