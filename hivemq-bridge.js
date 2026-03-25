/**
 * HiveMQ → ICU Oxygen Monitor Bridge Script
 *
 * Prerequisites:
 *   node >= 18  (uses native fetch)
 *   npm install mqtt
 *
 * Environment variables (set in Render/Railway dashboard):
 *   HIVEMQ_BROKER_URL      e.g. tls://abc123.s1.eu.hivemq.cloud:8883
 *   HIVEMQ_USERNAME
 *   HIVEMQ_PASSWORD
 *   SUPABASE_URL           e.g. https://xxxx.supabase.co
 *   SUPABASE_ANON_KEY      your project's anon/public key
 *   MQTT_TOPIC             (optional) defaults to icu/+/sensors
 */

"use strict";
const mqtt = require("mqtt");

const BROKER_URL   = process.env.HIVEMQ_BROKER_URL  || "tls://YOUR_BROKER.s1.eu.hivemq.cloud:8883";
const USERNAME     = process.env.HIVEMQ_USERNAME    || "YOUR_USERNAME";
const PASSWORD     = process.env.HIVEMQ_PASSWORD    || "YOUR_PASSWORD";
const TOPIC        = process.env.MQTT_TOPIC         || "icu/+/sensors";
const SUPABASE_URL = process.env.SUPABASE_URL       || "https://YOUR_PROJECT.supabase.co";
const ANON_KEY     = process.env.SUPABASE_ANON_KEY  || "YOUR_ANON_KEY";

const INGEST_URL  = `${SUPABASE_URL}/functions/v1/ingest-sensor-data`;
const ALERTS_URL  = `${SUPABASE_URL}/rest/v1/alerts`;

const BATCH_SIZE      = 10;
const FLUSH_INTERVAL  = 5_000;
const MAX_BUFFER_SIZE = 200;

// Alert thresholds (mirrors PRP Section 5)
const THRESHOLDS = {
  spo2: [
    { min: 90,  max: 94,  severity: "warning",  type: "low_spo2",  msg: (v) => `SpO2 is ${v}% (warning range 90–94%)` },
    { min: 0,   max: 89,  severity: "critical", type: "low_spo2",  msg: (v) => `SpO2 critically low at ${v}%` },
  ],
  o2_concentration: [
    { min: 18,    max: 19.5,  severity: "warning",  type: "low_o2",   msg: (v) => `O2 concentration low at ${v}%` },
    { min: 23.5,  max: 25,    severity: "warning",  type: "high_o2",  msg: (v) => `O2 concentration elevated at ${v}%` },
    { min: 0,     max: 17.99, severity: "critical", type: "low_o2",   msg: (v) => `O2 concentration critically low at ${v}%` },
    { min: 25.01, max: 100,   severity: "critical", type: "high_o2",  msg: (v) => `O2 concentration critically high at ${v}% (fire hazard)` },
  ],
  cylinder_weight: [
    { min: 500, max: 1500, severity: "warning",  type: "low_weight", msg: (v) => `Cylinder weight low at ${v}g` },
    { min: 0,   max: 499,  severity: "critical", type: "low_weight", msg: (v) => `Cylinder weight critically low at ${v}g` },
  ],
  temperature: [
    { min: 28, max: 32,  severity: "warning",  type: "high_temp", msg: (v) => `Temperature elevated at ${v}°C` },
    { min: 33, max: 999, severity: "critical", type: "high_temp", msg: (v) => `Temperature critically high at ${v}°C` },
    { min: 0,  max: 15,  severity: "critical", type: "low_temp",  msg: (v) => `Temperature critically low at ${v}°C` },
  ],
};

const deviceRoomCache = new Map();
const supabaseHeaders = {
  "Content-Type":  "application/json",
  "apikey":        ANON_KEY,
  "Authorization": `Bearer ${ANON_KEY}`,
};

async function getRoomId(device_id) {
  if (deviceRoomCache.has(device_id)) return deviceRoomCache.get(device_id);
  const res = await fetch(
    `${SUPABASE_URL}/rest/v1/rooms?device_id=eq.${encodeURIComponent(device_id)}&select=id`,
    { headers: supabaseHeaders }
  );
  if (!res.ok) return null;
  const rows = await res.json();
  if (!rows.length) { console.warn(`⚠️  No room for device_id: ${device_id}`); return null; }
  deviceRoomCache.set(device_id, rows[0].id);
  return rows[0].id;
}

async function insertAlerts(room_id, payload) {
  const alertRows = [];
  for (const [field, rules] of Object.entries(THRESHOLDS)) {
    const value = payload[field];
    if (value == null) continue;
    for (const rule of rules) {
      if (value >= rule.min && value <= rule.max) {
        alertRows.push({ room_id, type: rule.type, severity: rule.severity, message: rule.msg(value), acknowledged: false });
        break;
      }
    }
  }
  if (!alertRows.length) return;
  const res = await fetch(ALERTS_URL, {
    method: "POST",
    headers: { ...supabaseHeaders, "Prefer": "return=minimal" },
    body: JSON.stringify(alertRows),
  });
  if (!res.ok) console.error(`❌ Alert insert failed:`, await res.text());
  else console.log(`🚨 Inserted ${alertRows.length} alert(s) for room ${room_id}`);
}

// MQTT
const client = mqtt.connect(BROKER_URL, { username: USERNAME, password: PASSWORD, protocol: "mqtts", rejectUnauthorized: true });
client.on("connect", () => {
  console.log("✅ Connected to HiveMQ");
  client.subscribe(TOPIC, { qos: 1 }, (err) => err ? console.error("❌ Subscribe error:", err.message) : console.log(`✅ Subscribed: ${TOPIC}`));
});
client.on("error",     (err) => console.error("❌ MQTT error:", err.message));
client.on("reconnect", ()    => console.log("🔄 Reconnecting..."));

let buffer = [], flushTimer = null;

client.on("message", async (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());
    if (!payload.device_id) {
      const parts = topic.split("/");
      if (parts.length >= 2) payload.device_id = parts[1];
    }
    if (!payload.device_id) { console.warn("⚠️  No device_id, skipping"); return; }

    // FIX 1: Convert Unix epoch → ISO string for Supabase timestamptz
    if (payload.timestamp && typeof payload.timestamp === "number") {
      payload.timestamp = new Date(payload.timestamp * 1000).toISOString();
    } else if (!payload.timestamp) {
      payload.timestamp = new Date().toISOString();
    }

    console.log(`📡 ${payload.device_id} | SpO2: ${payload.spo2}% | O2: ${payload.o2_concentration}% | Wt: ${payload.cylinder_weight}g`);

    getRoomId(payload.device_id)
      .then((room_id) => { if (room_id) insertAlerts(room_id, payload).catch(console.error); })
      .catch(console.error);

    // FIX 2: Cap buffer to avoid unbounded memory growth
    if (buffer.length >= MAX_BUFFER_SIZE) { console.warn("⚠️  Buffer cap reached, dropping oldest"); buffer.shift(); }

    buffer.push(payload);
    if (buffer.length >= BATCH_SIZE) flushBuffer();
    else if (!flushTimer) flushTimer = setTimeout(flushBuffer, FLUSH_INTERVAL);
  } catch (err) {
    console.error("❌ Parse error:", err.message);
  }
});

async function flushBuffer() {
  if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
  if (!buffer.length) return;
  const batch = buffer.splice(0, buffer.length);
  try {
    const res = await fetch(INGEST_URL, { method: "POST", headers: supabaseHeaders, body: JSON.stringify(batch) });
    const result = await res.json();
    if (res.ok) console.log(`✅ Sent ${batch.length} reading(s) → inserted: ${result.inserted ?? batch.length}`);
    else { console.error(`❌ Edge function error (${res.status}):`, result.error ?? result); buffer.unshift(...batch.slice(0, MAX_BUFFER_SIZE - buffer.length)); }
  } catch (err) {
    console.error("❌ Network error:", err.message);
    buffer.unshift(...batch.slice(0, MAX_BUFFER_SIZE - buffer.length));
  }
}

async function shutdown() { console.log("\n🛑 Shutting down..."); await flushBuffer(); client.end(); process.exit(0); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);