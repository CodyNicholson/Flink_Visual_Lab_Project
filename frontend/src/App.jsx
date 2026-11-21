import React, { useEffect, useRef, useState } from 'react';

const API_BASE = '/api/flink';
const FLINK_JOB_UI = import.meta.env.VITE_FLINK_EMBEDDED_UI_URL || 'http://localhost:8082';
const FLINK_CLUSTER_UI = import.meta.env.VITE_FLINK_CLUSTER_UI_URL || 'http://localhost:8081';

// Scenarios 1, 4 and 5 all travel through the Kafka topic
const KAFKA_SOURCE_TYPES = ['KAFKA', 'JDBC_ENRICH', 'WATERMARK_CHECKPOINT'];

const LESSONS = {
  KAFKA: {
    title: '1. Kafka source connector: event-time ingestion',
    howItWorks: 'Clicking this button publishes an order event into the Kafka topic "flink-orders-topic". Flink\'s KafkaSource consumes the partition split, assigns an Event-Time Watermark, routes the event by keyBy(customerId) into Keyed ValueState, and writes the enriched aggregate to the Postgres & FileSystem Sinks.'
  },
  POSTGRES_CDC: {
    title: '2. PostgreSQL change data capture from the WAL',
    howItWorks: 'Clicking this button executes a direct SQL UPDATE on PostgreSQL\'s "customers_cdc_source" table. Flink does NOT poll the database: its CDC source reads the PostgreSQL Write-Ahead Log (wal_level=logical) through a replication slot in real time. Because the table uses REPLICA IDENTITY FULL, the UPDATE arrives as a "-U" (retract old row) followed by a "+U" (new row) changelog pair, and the "+U" immediately updates Flink\'s Customer Tier ValueState.'
  },
  FILESYSTEM: {
    title: '3. Local filesystem source and sink (continuous monitoring)',
    howItWorks: 'Clicking this button writes a new ".jsonl" micro-batch file directly into the shared Docker volume "/data/flink-fs/inbox". Flink\'s FileSource (configured with monitorContinuously every 1s) discovers the new file as a split, streams the record through the stateful pipeline, and appends the output to "/data/flink-fs/outbox".'
  },
  JDBC_ENRICH: {
    title: '4. Stateful stream-table join and PostgreSQL upsert sink',
    howItWorks: 'Demonstrates joining high-velocity stream events against relational database state inside Flink\'s KeyedCoProcessFunction. Flink enriches the incoming payment with the customer\'s live CDC tier and cumulative spend state, then executes an idempotent INSERT ... ON CONFLICT DO UPDATE upsert into PostgreSQL\'s "enriched_orders_sink" table.'
  },
  WATERMARK_CHECKPOINT: {
    title: '5. Late events, watermarks and checkpoints',
    howItWorks: 'Injects an event with a timestamp backdated by 60 seconds. The operator\'s watermark (max event time - 2s, and never more than 10s behind the wall clock) is already past that timestamp, so Flink flags the record LATE_OUT_OF_ORDER. Every 10s a checkpoint barrier flows through the DAG and the keyed ValueState is snapshotted to /data/flink-fs/checkpoints; watch the checkpoint badge.'
  }
};

const SCENARIOS = [
  { key: 'KAFKA', cls: 'btn-kafka', label: '1. Send a Kafka event (CUST-101, +$250)', endpoint: '/trigger/kafka?customerId=CUST-101&amount=250.00' },
  { key: 'POSTGRES_CDC', cls: 'btn-cdc', label: '2A. CDC: upgrade CUST-101 to VIP_DIAMOND', endpoint: '/trigger/postgres-cdc?customerId=CUST-101&newTier=VIP_DIAMOND' },
  { key: 'POSTGRES_CDC', cls: 'btn-cdc-alt', label: '2B. CDC: upgrade CUST-202 to PLATINUM', endpoint: '/trigger/postgres-cdc?customerId=CUST-202&newTier=PLATINUM' },
  { key: 'FILESYSTEM', cls: 'btn-file', label: '3. Drop a file into /inbox', endpoint: '/trigger/filesystem?customerId=CUST-202&amount=480.50' },
  { key: 'JDBC_ENRICH', cls: 'btn-jdbc', label: '4. Join with state and upsert to Postgres', endpoint: '/trigger/postgres-jdbc?customerId=CUST-101&amount=920.00' },
  { key: 'WATERMARK_CHECKPOINT', cls: 'btn-late', label: '5. Send a late, out-of-order event', endpoint: '/trigger/late-watermark?customerId=CUST-202&amount=99.99' }
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
          <h1>Apache Flink stateful streaming</h1>
          <p>Flink 1.19 with Kafka, Postgres CDC and filesystem connectors, driven from Spring Boot 3</p>
        </div>
        <div className="header-badges">
          <span className={`badge ${connected ? 'badge-green' : 'badge-red'}`}>
            {connected ? 'SSE telemetry: live' : 'SSE telemetry: connecting…'}
          </span>
          <span className={`badge badge-green ${checkpointFlash ? 'badge-flash' : ''}`}>
            Checkpoint every 10 s, exactly-once{lastCheckpoint ? ` · last ${lastCheckpoint.eventId}` : ''}
          </span>
          <a href={FLINK_JOB_UI} target="_blank" rel="noreferrer" className="badge badge-link">
            Lab job dashboard
          </a>
          <a href={FLINK_CLUSTER_UI} target="_blank" rel="noreferrer" className="badge badge-link">
            Standalone cluster UI
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
            <span className="live-pill" aria-live="polite">
              In flight: <strong>{activeEvent.eventId}</strong> · changelog <code>{activeEvent.changelogOp}</code> · stage <code>{activeEvent.operatorStage}</code>
            </span>
          )}
        </div>
        <p>{lesson.howItWorks}</p>
      </section>

      {/* Animated Flink Operator DAG Topology */}
      <section className="dag-canvas">
        {/* Column 1: Data Sources */}
        <div className="dag-column">
          <div className="column-label">1. Sources</div>

          <div className={`dag-node ${ingesting(KAFKA_SOURCE_TYPES) ? 'node-pulse-blue' : ''}`}>
            <div className="node-title">Kafka source</div>
            <div className="node-meta">Topic: <code>flink-orders-topic</code> (3 partitions)</div>
          </div>

          <div className={`dag-node ${activeStage === 'CDC_SYNC' ? 'node-pulse-purple' : ''}`}>
            <div className="node-title">Postgres WAL CDC connector</div>
            <div className="node-meta">Slot: <code>flink_cdc_lab_slot (test_decoding)</code></div>
          </div>

          <div className={`dag-node ${ingesting(['FILESYSTEM']) ? 'node-pulse-amber' : ''}`}>
            <div className="node-title">Local filesystem source</div>
            <div className="node-meta">Dir: <code>/data/flink-fs/inbox</code> ({dbState.inboxFileCount} files)</div>
          </div>
        </div>

        <div className={`dag-connector ${activeStage === 'SOURCE_INGEST' || activeStage === 'CDC_SYNC' ? 'connector-active' : ''}`}>
          <span>source split</span>
          <div className="connector-line" />
        </div>

        {/* Column 2: Event-Time Watermarks & Changelog Stream */}
        <div className="dag-column">
          <div className="column-label">2. Event time and changelog</div>
          <div className={`dag-node tall-node ${
            activeStage === 'WATERMARK_ASSIGN' ? (isLate ? 'node-pulse-red' : 'node-pulse-cyan')
              : activeStage === 'CDC_SYNC' ? 'node-pulse-cyan' : ''
          }`}>
            <div className="node-title">Watermark and changelog operator</div>
            <div className="node-meta">BoundedOutOfOrderness (2s)</div>
            <div className="operator-detail">
              <div>Changelog mode: <code>+I, -U, +U, -D</code></div>
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
          <div className="column-label">3. Managed keyed state</div>
          <div className={`dag-node tall-node ${
            activeStage === 'KEYED_STATE_JOIN' || activeStage === 'CDC_SYNC' ? 'node-pulse-green'
              : checkpointFlash ? 'node-pulse-checkpoint' : ''
          }`}>
            <div className="node-title">KeyedCoProcessFunction</div>
            <div className="node-meta">Fault-tolerant ValueState backend</div>
            <div className="operator-detail">
              <div>Customer key: <code>{activeEvent?.customerId || '—'}</code></div>
              <div>Joined tier: <code>{activeEvent?.customerTier || '—'}</code></div>
              <div>Running total: <code>${activeEvent ? Number(activeEvent.runningTotal).toFixed(2) : '0.00'}</code></div>
              <div>Last snapshot: <code>{lastCheckpoint ? lastCheckpoint.eventId : 'pending'}</code></div>
            </div>
          </div>
        </div>

        <div className={`dag-connector ${activeStage === 'SINK_COMMIT' ? 'connector-active' : ''}`}>
          <span>sink writer</span>
          <div className="connector-line" />
        </div>

        {/* Column 4: Multi-Target Sinks */}
        <div className="dag-column">
          <div className="column-label">4. Sinks</div>
          <div className={`dag-node ${activeStage === 'SINK_COMMIT' ? 'node-pulse-emerald' : ''}`}>
            <div className="node-title">PostgreSQL upsert sink</div>
            <div className="node-meta">Table: <code>enriched_orders_sink</code></div>
          </div>
          <div className={`dag-node ${activeStage === 'SINK_COMMIT' ? 'node-pulse-emerald' : ''}`}>
            <div className="node-title">Filesystem outbox sink</div>
            <div className="node-meta">File: <code>/outbox/flink-processed-sink.jsonl</code> ({dbState.outboxLineCount} lines)</div>
          </div>
        </div>
      </section>

      {/* Live Database & Sink Inspector Panels */}
      <section className="tables-grid">
        <div className="panel-card">
          <h4>Source table <code>customers_cdc_source</code>, watched by CDC</h4>
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Customer</th>
                  <th>Name</th>
                  <th>Live tier (CDC)</th>
                  <th>Credit limit</th>
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
          <h4>Sink table <code>enriched_orders_sink</code>, written by Flink</h4>
          <div className="table-scroll">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Order</th>
                  <th>Source</th>
                  <th>Customer</th>
                  <th>Amount</th>
                  <th>Joined tier</th>
                  <th>Running total (state)</th>
                  <th>Watermark</th>
                </tr>
              </thead>
              <tbody>
                {dbState.enrichedSinkTable.length === 0 ? (
                  <tr>
                    <td colSpan="7" className="empty-cell">No rows yet. Send an event above to stream it through Flink.</td>
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
        <h4>Live operator telemetry (SSE)</h4>
        <div className="telemetry-feed">
          {logs.length === 0 ? (
            <div className="empty-cell">Waiting for stream events…</div>
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
