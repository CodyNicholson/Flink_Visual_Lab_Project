import React, { useEffect, useRef, useState } from 'react';

const API_BASE = '/api/flink';
const FLINK_JOB_UI = import.meta.env.VITE_FLINK_EMBEDDED_UI_URL || 'http://localhost:8082';
const FLINK_CLUSTER_UI = import.meta.env.VITE_FLINK_CLUSTER_UI_URL || 'http://localhost:8081';

// Scenarios 1, 4 and 5 all travel through the Kafka topic
const KAFKA_SOURCE_TYPES = ['KAFKA', 'JDBC_ENRICH', 'WATERMARK_CHECKPOINT'];

const LESSONS = {
  KAFKA: {
    title: '1. Apache Kafka Source Connector (Event-Time Stream Ingestion)',
    howItWorks: 'Clicking this button publishes an order event into the Kafka topic "flink-orders-topic". Flink\'s KafkaSource consumes the partition split, assigns an Event-Time Watermark, routes the event by keyBy(customerId) into Keyed ValueState, and writes the enriched aggregate to the Postgres & FileSystem Sinks.'
  },
  POSTGRES_CDC: {
    title: '2. PostgreSQL Change Data Capture (WAL Logical Replication Stream)',
    howItWorks: 'Clicking this button executes a direct SQL UPDATE on PostgreSQL\'s "customers_cdc_source" table. Flink does NOT poll the database: its CDC source reads the PostgreSQL Write-Ahead Log (wal_level=logical) through a replication slot in real time. Because the table uses REPLICA IDENTITY FULL, the UPDATE arrives as a "-U" (retract old row) followed by a "+U" (new row) changelog pair, and the "+U" immediately updates Flink\'s Customer Tier ValueState.'
  },
  FILESYSTEM: {
    title: '3. Local File System Source & Sink (Continuous Split Monitor)',
    howItWorks: 'Clicking this button writes a new ".jsonl" micro-batch file directly into the shared Docker volume "/data/flink-fs/inbox". Flink\'s FileSource (configured with monitorContinuously every 1s) discovers the new file as a split, streams the record through the stateful pipeline, and appends the output to "/data/flink-fs/outbox".'
  },
  JDBC_ENRICH: {
    title: '4. Stateful Stream-Table Join & PostgreSQL Upsert Sink',
    howItWorks: 'Demonstrates joining high-velocity stream events against relational database state inside Flink\'s KeyedCoProcessFunction. Flink enriches the incoming payment with the customer\'s live CDC tier and cumulative spend state, then executes an idempotent INSERT ... ON CONFLICT DO UPDATE upsert into PostgreSQL\'s "enriched_orders_sink" table.'
  },
  WATERMARK_CHECKPOINT: {
    title: '5. Out-of-Order Event-Time Watermarks & Chandy-Lamport Checkpointing',
    howItWorks: 'Injects an event with a timestamp backdated by 60 seconds. The operator\'s watermark (max event time - 2s, and never more than 10s behind the wall clock) is already past that timestamp, so Flink flags the record LATE_OUT_OF_ORDER. Every 10s a checkpoint barrier flows through the DAG and the keyed ValueState is snapshotted to /data/flink-fs/checkpoints; watch the checkpoint badge.'
  }
};

const SCENARIOS = [
  { key: 'KAFKA', cls: 'btn-kafka', label: '1. Emit Kafka Stream Event (CUST-101 +$250)', endpoint: '/trigger/kafka?customerId=CUST-101&amount=250.00' },
  { key: 'POSTGRES_CDC', cls: 'btn-cdc', label: '2A. Postgres CDC: Upgrade CUST-101 to VIP_DIAMOND', endpoint: '/trigger/postgres-cdc?customerId=CUST-101&newTier=VIP_DIAMOND' },
  { key: 'POSTGRES_CDC', cls: 'btn-cdc-alt', label: '2B. Postgres CDC: Upgrade CUST-202 to PLATINUM', endpoint: '/trigger/postgres-cdc?customerId=CUST-202&newTier=PLATINUM' },
  { key: 'FILESYSTEM', cls: 'btn-file', label: '3. Drop File into Local FileSystem (/inbox)', endpoint: '/trigger/filesystem?customerId=CUST-202&amount=480.50' },
  { key: 'JDBC_ENRICH', cls: 'btn-jdbc', label: '4. Stateful Join & Postgres JDBC Upsert Sink', endpoint: '/trigger/postgres-jdbc?customerId=CUST-101&amount=920.00' },
  { key: 'WATERMARK_CHECKPOINT', cls: 'btn-late', label: '5. Send Out-of-Order Late Event (Watermark Test)', endpoint: '/trigger/late-watermark?customerId=CUST-202&amount=99.99' }
];

export default function App() {
  const [selectedSource, setSelectedSource] = useState('KAFKA');
  const [activeStage, setActiveStage] = useState(null);
  const [activeEvent, setActiveEvent] = useState(null);
  const [logs, setLogs] = useState([]);
  const [connected, setConnected] = useState(false);
  const [lastCheckpoint, setLastCheckpoint] = useState(null);
  const [checkpointFlash, setCheckpointFlash] = useState(false);
  const [error, setError] = useState(null);
  const [dbState, setDbState] = useState({
    customersCdcTable: [],
    enrichedSinkTable: [],
    inboxFileCount: 0,
    outboxLineCount: 0
  });
  const flashTimer = useRef(null);

  const refreshDbState = async () => {
    try {
      const res = await fetch(`${API_BASE}/state`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setDbState(await res.json());
    } catch (err) {
      console.error(err);
    }
  };

  useEffect(() => {
    refreshDbState();
    const es = new EventSource(`${API_BASE}/stream`);
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);

    es.addEventListener('flink-trace', (e) => {
      const trace = JSON.parse(e.data);
      if (trace.operatorStage === 'CHECKPOINT_COMPLETE') {
        // Periodic, so it updates the checkpoint badge instead of hijacking the DAG animation
        setLastCheckpoint(trace);
        setCheckpointFlash(true);
        clearTimeout(flashTimer.current);
        flashTimer.current = setTimeout(() => setCheckpointFlash(false), 1200);
        return;
      }
      setActiveStage(trace.operatorStage);
      setActiveEvent(trace);
      setLogs((prev) => [trace, ...prev.slice(0, 34)]);
      refreshDbState();
    });

    return () => {
      es.close();
      clearTimeout(flashTimer.current);
    };
  }, []);

  const triggerScenario = async (sourceKey, endpoint) => {
    setSelectedSource(sourceKey);
    setError(null);
    try {
      const res = await fetch(`${API_BASE}${endpoint}`, { method: 'POST' });
      if (!res.ok) throw new Error(`Backend returned HTTP ${res.status}`);
    } catch (err) {
      setError(`Trigger failed: ${err.message}`);
    }
    refreshDbState();
  };

  const lesson = LESSONS[selectedSource];
  const isLate = activeEvent?.watermarkInfo === 'LATE_OUT_OF_ORDER';
  const ingesting = (types) => activeStage === 'SOURCE_INGEST' && types.includes(activeEvent?.sourceType);

  return (
    <div className="flink-lab">
      <header className="top-bar">
        <div>
          <h1>Apache Flink Stateful Stream & Multi-Connector Visual Lab</h1>
          <p>Spring Boot 3 + Flink 1.19 + Kafka + Postgres WAL CDC + Local FileSystem</p>
        </div>
        <div className="header-badges">
          <span className={`badge ${connected ? 'badge-green' : 'badge-red'}`}>
            {connected ? 'SSE telemetry: live' : 'SSE telemetry: connecting…'}
          </span>
          <span className={`badge badge-green ${checkpointFlash ? 'badge-flash' : ''}`}>
            Checkpointing: 10s (Exactly-Once){lastCheckpoint ? ` · last ${lastCheckpoint.eventId}` : ''}
          </span>
          <a href={FLINK_JOB_UI} target="_blank" rel="noreferrer" className="badge badge-link">
            Lab Job Dashboard
          </a>
          <a href={FLINK_CLUSTER_UI} target="_blank" rel="noreferrer" className="badge badge-link">
            Standalone Cluster UI
          </a>
        </div>
      </header>

      {/* Interactive Connector Trigger Buttons */}
      <section className="button-bar">
        {SCENARIOS.map((s) => (
          <button key={s.label} className={`action-btn ${s.cls}`} onClick={() => triggerScenario(s.key, s.endpoint)}>
            {s.label}
          </button>
        ))}
      </section>

      {error && <div className="error-banner">{error}</div>}

      {/* Educational Explanation Banner */}
      <section className="explanation-card">
        <div className="explanation-header">
          <h3>{lesson.title}</h3>
          {activeEvent && (
            <span className="live-pill">
              Active Packet: <strong>{activeEvent.eventId}</strong> | Changelog: <code>{activeEvent.changelogOp}</code> | Stage: <code>{activeEvent.operatorStage}</code>
            </span>
          )}
        </div>
        <p>{lesson.howItWorks}</p>
      </section>

      {/* Animated Flink Operator DAG Topology */}
      <section className="dag-canvas">
        {/* Column 1: Data Sources */}
        <div className="dag-column">
          <div className="column-label">1. External Data Sources</div>

          <div className={`dag-node ${ingesting(KAFKA_SOURCE_TYPES) ? 'node-pulse-blue' : ''}`}>
            <div className="node-title">Apache Kafka Source</div>
            <div className="node-meta">Topic: <code>flink-orders-topic</code> (3 partitions)</div>
          </div>

          <div className={`dag-node ${activeStage === 'CDC_SYNC' ? 'node-pulse-purple' : ''}`}>
            <div className="node-title">Postgres WAL CDC Connector</div>
            <div className="node-meta">Slot: <code>flink_cdc_lab_slot (test_decoding)</code></div>
          </div>

          <div className={`dag-node ${ingesting(['FILESYSTEM']) ? 'node-pulse-amber' : ''}`}>
            <div className="node-title">Local FileSystem Source</div>
            <div className="node-meta">Dir: <code>/data/flink-fs/inbox</code> ({dbState.inboxFileCount} files)</div>
          </div>
        </div>

        <div className={`dag-connector ${activeStage === 'SOURCE_INGEST' || activeStage === 'CDC_SYNC' ? 'connector-active' : ''}`}>
          <span>Source Split</span>
          <div className="connector-line" />
        </div>

        {/* Column 2: Event-Time Watermarks & Changelog Stream */}
        <div className="dag-column">
          <div className="column-label">2. Event-Time & Changelog</div>
          <div className={`dag-node tall-node ${
            activeStage === 'WATERMARK_ASSIGN' ? (isLate ? 'node-pulse-red' : 'node-pulse-cyan')
              : activeStage === 'CDC_SYNC' ? 'node-pulse-cyan' : ''
          }`}>
            <div className="node-title">Watermark & Changelog Operator</div>
            <div className="node-meta">BoundedOutOfOrderness (2s)</div>
            <div className="operator-detail">
              <div>Changelog Mode: <code>+I, -U, +U, -D</code></div>
              <div>Status: <code className={isLate ? 'late-text' : ''}>{activeEvent?.watermarkInfo || 'IDLE'}</code></div>
            </div>
          </div>
        </div>

        <div className={`dag-connector ${activeStage === 'WATERMARK_ASSIGN' || activeStage === 'KEYED_STATE_JOIN' ? 'connector-active' : ''}`}>
          <span>keyBy(custId)</span>
          <div className="connector-line" />
        </div>

        {/* Column 3: Stateful CoProcess Operator */}
        <div className="dag-column">
          <div className="column-label">3. Flink Managed Keyed State</div>
          <div className={`dag-node tall-node ${
            activeStage === 'KEYED_STATE_JOIN' || activeStage === 'CDC_SYNC' ? 'node-pulse-green'
              : checkpointFlash ? 'node-pulse-checkpoint' : ''
          }`}>
            <div className="node-title">KeyedCoProcessFunction</div>
            <div className="node-meta">Fault-Tolerant ValueState Backend</div>
            <div className="operator-detail">
              <div>Customer Key: <code>{activeEvent?.customerId || '—'}</code></div>
              <div>Joined Tier: <code>{activeEvent?.customerTier || '—'}</code></div>
              <div>Running Total: <code>${activeEvent ? Number(activeEvent.runningTotal).toFixed(2) : '0.00'}</code></div>
              <div>Last Snapshot: <code>{lastCheckpoint ? lastCheckpoint.eventId : 'pending'}</code></div>
            </div>
          </div>
        </div>

        <div className={`dag-connector ${activeStage === 'SINK_COMMIT' ? 'connector-active' : ''}`}>
          <span>Sink Writer</span>
          <div className="connector-line" />
        </div>

        {/* Column 4: Multi-Target Sinks */}
        <div className="dag-column">
          <div className="column-label">4. Flink Sinks</div>
          <div className={`dag-node ${activeStage === 'SINK_COMMIT' ? 'node-pulse-emerald' : ''}`}>
            <div className="node-title">PostgreSQL Upsert Sink</div>
            <div className="node-meta">Table: <code>enriched_orders_sink</code></div>
          </div>
          <div className={`dag-node ${activeStage === 'SINK_COMMIT' ? 'node-pulse-emerald' : ''}`}>
            <div className="node-title">FileSystem Outbox Sink</div>
            <div className="node-meta">File: <code>/outbox/flink-processed-sink.jsonl</code> ({dbState.outboxLineCount} lines)</div>
          </div>
        </div>
      </section>

      {/* Live Database & Sink Inspector Panels */}
      <section className="tables-grid">
        <div className="panel-card">
          <h4>PostgreSQL Source Table: <code>customers_cdc_source</code> (Monitored by Flink WAL CDC)</h4>
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Customer ID</th>
                  <th>Name</th>
                  <th>Live Tier (CDC)</th>
                  <th>Credit Limit</th>
                </tr>
              </thead>
              <tbody>
                {dbState.customersCdcTable.map((row) => (
                  <tr key={row.customer_id}>
                    <td><code>{row.customer_id}</code></td>
                    <td>{row.full_name}</td>
                    <td><span className="tier-badge">{row.tier}</span></td>
                    <td>${Number(row.credit_limit).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="panel-card">
          <h4>PostgreSQL Sink Table: <code>enriched_orders_sink</code> (Written by Flink Sink)</h4>
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Order ID</th>
                  <th>Source</th>
                  <th>Customer</th>
                  <th>Amount</th>
                  <th>Joined Tier</th>
                  <th>Running State Total</th>
                  <th>Watermark</th>
                </tr>
              </thead>
              <tbody>
                {dbState.enrichedSinkTable.length === 0 ? (
                  <tr>
                    <td colSpan="7" className="empty-cell">Click any button above to stream events through Flink...</td>
                  </tr>
                ) : (
                  dbState.enrichedSinkTable.map((row) => (
                    <tr key={row.order_id}>
                      <td><code>{row.order_id}</code></td>
                      <td>{row.source_type}</td>
                      <td><code>{row.customer_id}</code></td>
                      <td>${Number(row.amount).toFixed(2)}</td>
                      <td><span className="tier-badge">{row.customer_tier}</span></td>
                      <td><strong>${Number(row.running_customer_total).toFixed(2)}</strong></td>
                      <td><code className={row.watermark_status === 'LATE_OUT_OF_ORDER' ? 'late-text' : ''}>{row.watermark_status}</code></td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* Real-time Flink Operator Telemetry Stream */}
      <section className="panel-card">
        <h4>Real-Time Apache Flink Operator Telemetry Log (SSE)</h4>
        <div className="telemetry-feed">
          {logs.length === 0 ? (
            <div className="empty-cell">Waiting for stream events...</div>
          ) : (
            logs.map((item) => (
              <div key={`${item.eventId}-${item.operatorStage}-${item.changelogOp}-${item.timestamp}`} className={`telemetry-row stage-${item.operatorStage}`}>
                <span className="t-stage">{item.operatorStage}</span>
                <span className="t-op">[{item.changelogOp}]</span>
                <span className="t-source">{item.sourceType}</span>
                <span className="t-desc">{item.description}</span>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}
