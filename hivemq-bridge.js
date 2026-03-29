/**
 * HiveMQ → ICU Oxygen Monitor Bridge Script
 * Final Production Version
 *
 * Responsibilities:
 *   - Connect to HiveMQ Cloud via MQTT over TLS
 *   - Receive sensor payloads from ESP32 devices
 *   - Batch and forward readings to Supabase edge function
 *   - Alert logic is handled entirely by the edge function and pg_cron
 *
 * Prerequisites:
 *   node >= 18  (uses native fetch)
 *   npm install mqtt
 *
 * Environment variables (set in Railway dashboard):
 *   HIVEMQ_BROKER_URL      e.g. tls://abc123.s1.eu.hivemq.cloud:8883
 *   HIVEMQ_USERNAME        HiveMQ Cloud username
 *   HIVEMQ_PASSWORD        HiveMQ Cloud password
 *   SUPABASE_URL           e.g. https://xxxx.supabase.co
 *   SUPABASE_ANON_KEY      Supabase project anon/public key
 *   MQTT_TOPIC             (optional) defaults to icu/+/sensors
 */

"use strict";
const mqtt = require("mqtt");

// ── Configuration ──────────────────────────────────────────────────────────────
const BROKER_URL   = process.env.HIVEMQ_BROKER_URL || "tls://YOUR_BROKER.s1.eu.hivemq.cloud:8883";
const USERNAME     = process.env.HIVEMQ_USERNAME   || "YOUR_USERNAME";
const PASSWORD     = process.env.HIVEMQ_PASSWORD   || "YOUR_PASSWORD";
const TOPIC        = process.env.MQTT_TOPIC        || "icu/+/sensors";
const SUPABASE_URL = process.env.SUPABASE_URL      || "https://YOUR_PROJECT.supabase.co";
const ANON_KEY     = process.env.SUPABASE_ANON_KEY || "YOUR_ANON_KEY";

const INGEST_URL      = `${SUPABASE_URL}/functions/v1/ingest-sensor-data`;
const BATCH_SIZE      = 10;
const FLUSH_INTERVAL  = 5_000;   // ms — flush every 5 seconds if batch not full
const MAX_BUFFER_SIZE = 200;     // drop oldest if buffer exceeds this (network outage protection)

const supabaseHeaders = {
  "Content-Type":  "application/json",
  "apikey":        ANON_KEY,
  "Authorization": `Bearer ${ANON_KEY}`,
};

// ── MQTT Connection ────────────────────────────────────────────────────────────
console.log(`Connecting to HiveMQ: ${BROKER_URL}`);
console.log(`Topic: ${TOPIC}`);

const client = mqtt.connect(BROKER_URL, {
  username:           USERNAME,
  password:           PASSWORD,
  protocol:           "mqtts",
  rejectUnauthorized: true,
});

client.on("connect", () => {
  console.log("✅ Connected to HiveMQ");
  client.subscribe(TOPIC, { qos: 1 }, (err) => {
    if (err) console.error("❌ Subscribe error:", err.message);
    else     console.log(`✅ Subscribed to: ${TOPIC}`);
  });
});

client.on("error",     (err) => console.error("❌ MQTT error:", err.message));
client.on("reconnect", ()    => console.log("🔄 Reconnecting to HiveMQ..."));

// ── Message Handler ────────────────────────────────────────────────────────────
let buffer     = [];
let flushTimer = null;

client.on("message", (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());

    // Extract device_id from topic if missing from payload
    // Topic format: icu/{device_id}/sensors
    if (!payload.device_id) {
      const parts = topic.split("/");
      if (parts.length >= 2) payload.device_id = parts[1];
    }

    if (!payload.device_id) {
      console.warn("⚠️  No device_id in payload or topic — skipping");
      return;
    }

    // Convert Unix epoch timestamp → ISO 8601 string for Supabase timestamptz
    if (payload.timestamp && typeof payload.timestamp === "number") {
      payload.timestamp = new Date(payload.timestamp * 1000).toISOString();
    } else if (!payload.timestamp) {
      payload.timestamp = new Date().toISOString();
    }

    console.log(
      `📡 ${payload.device_id} | ` +
      `SpO2: ${payload.spo2 ?? "—"}% | ` +
      `O2: ${payload.o2_concentration ?? "—"}% | ` +
      `Temp: ${payload.temperature ?? "—"}°C | ` +
      `Humidity: ${payload.humidity ?? "—"}% | ` +
      `Pressure: ${payload.pressure ?? "—"}hPa | ` +
      `Weight: ${payload.cylinder_weight ?? "—"}g`
    );

    // Cap buffer to prevent unbounded memory growth during network outages
    if (buffer.length >= MAX_BUFFER_SIZE) {
      console.warn(`⚠️  Buffer full (${MAX_BUFFER_SIZE}) — dropping oldest reading`);
      buffer.shift();
    }

    buffer.push(payload);

    // Flush immediately if batch size reached, otherwise set timer
    if (buffer.length >= BATCH_SIZE) {
      flushBuffer();
    } else if (!flushTimer) {
      flushTimer = setTimeout(flushBuffer, FLUSH_INTERVAL);
    }

  } catch (err) {
    console.error("❌ Failed to parse MQTT message:", err.message);
  }
});

// ── Batch Flush to Supabase Edge Function ─────────────────────────────────────
async function flushBuffer() {
  if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
  if (!buffer.length) return;

  const batch = buffer.splice(0, buffer.length);

  try {
    const response = await fetch(INGEST_URL, {
      method:  "POST",
      headers: supabaseHeaders,
      body:    JSON.stringify(batch),
    });

    const result = await response.json();

    if (response.ok) {
      console.log(
        `✅ Sent ${batch.length} reading(s) → ` +
        `inserted: ${result.inserted ?? batch.length}` +
        (result.alerts_created ? ` | alerts created: ${result.alerts_created}` : "") +
        (result.alerts_resolved ? ` | alerts resolved: ${result.alerts_resolved}` : "")
      );
    } else {
      console.error(`❌ Edge function error (${response.status}):`, result.error ?? result);
      // Re-queue failed readings respecting buffer cap
      const requeue = batch.slice(0, MAX_BUFFER_SIZE - buffer.length);
      buffer.unshift(...requeue);
      console.log(`🔄 Re-queued ${requeue.length} reading(s) for retry`);
    }

  } catch (err) {
    console.error("❌ Network error:", err.message);
    const requeue = batch.slice(0, MAX_BUFFER_SIZE - buffer.length);
    buffer.unshift(...requeue);
    console.log(`🔄 Re-queued ${requeue.length} reading(s) for retry`);
  }
}

// ── Graceful Shutdown ──────────────────────────────────────────────────────────
async function shutdown() {
  console.log("\n🛑 Shutting down — flushing remaining buffer...");
  await flushBuffer();
  client.end();
  process.exit(0);
}

process.on("SIGINT",  shutdown);
process.on("SIGTERM", shutdown);