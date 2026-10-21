package com.example.flinklab.model;

import java.io.Serializable;

public class StreamRecordPayload implements Serializable {
    private String eventId;
    private String sourceType;   // KAFKA, FILESYSTEM, JDBC_ENRICH, WATERMARK_CHECKPOINT
    private String customerId;
    private double amount;
    private long eventTimestamp;

    public StreamRecordPayload() {}

    public StreamRecordPayload(String eventId, String sourceType, String customerId, double amount, long eventTimestamp) {
        this.eventId = eventId;
        this.sourceType = sourceType;
        this.customerId = customerId;
        this.amount = amount;
        this.eventTimestamp = eventTimestamp;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getSourceType() { return sourceType; }
    public void setSourceType(String sourceType) { this.sourceType = sourceType; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }
}
