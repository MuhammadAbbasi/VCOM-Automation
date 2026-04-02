/**
 * app.js — Mazara SCADA Monitor frontend logic.
 *
 * Polls /api/status every 10 seconds and updates all 5 dashboard sections:
 *   1. Macro health metrics (total / online / tripped / comms lost)
 *   2. Data ingestion status (6 file cards)
 *   3. Inverter health matrix (36 LED cards)
 *   4. Active diagnostic alerts
 *   5. Historical alarm trail (newest-first, max 100 rows)
 */

const POLL_INTERVAL_MS = 10_000;

// Ordered list of inverter names matching the analyser
const INVERTER_NAMES = [
  ...Array.from({ length: 12 }, (_, i) => `INV TX1-${String(i + 1).padStart(2, "0")}`),
  ...Array.from({ length: 12 }, (_, i) => `INV TX2-${String(i + 1).padStart(2, "0")}`),
  ...Array.from({ length: 12 }, (_, i) => `INV TX3-${String(i + 1).padStart(2, "0")}`),
];

const FILE_LABELS = {
  PR: "PR Inverter",
  Potenza_AC: "Potenza AC",
  Corrente_DC: "Corrente DC",
  Resistenza_Isolamento: "Resistenza Isol.",
  Temperatura: "Temperatura",
  Irraggiamento: "Irraggiamento",
};

// ─── Helpers ──────────────────────────────────────────────────────────────

function el(id) { return document.getElementById(id); }

function safeNum(v, fallback = "—") {
  return (v !== null && v !== undefined && !isNaN(v)) ? v : fallback;
}

function now() {
  return new Date().toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

// ─── 1. Macro health ──────────────────────────────────────────────────────

function updateMacro(data) {
  const m = data.macro_health || {};
  el("val-total").textContent   = safeNum(m.total_inverters, 36);
  el("val-online").textContent  = safeNum(m.online, "—");
  el("val-tripped").textContent = safeNum(m.tripped, "—");
  el("val-comms").textContent   = safeNum(m.comms_lost, "—");
}

// ─── 2. File ingestion status ─────────────────────────────────────────────

function updateIngestion(data) {
  const fs = data.file_status || {};
  const grid = el("ingestion-grid");
  grid.innerHTML = "";

  Object.entries(FILE_LABELS).forEach(([key, label]) => {
    const info = fs[key] || {};
    const status = info.status || "pending";
    const ts = info.timestamp ? info.timestamp.replace("T", " ").substring(11, 19) : "—";

    const card = document.createElement("div");
    card.className = `card file-card ${status}`;
    card.innerHTML = `
      <span class="file-name">${label}</span>
      <span class="file-status">${status === "loading" ? '<span class="spinner"></span>' : ""}${status}</span>
      <span class="file-time">${ts}</span>
    `;
    grid.appendChild(card);
  });
}

// ─── 3. Inverter health matrix ────────────────────────────────────────────

function ledHtml(color, title) {
  return `<span class="led ${color}" title="${title}"></span>`;
}

function updateInverterGrid(data) {
  const health = data.inverter_health || {};
  const grid = el("inverter-grid");
  grid.innerHTML = "";

  INVERTER_NAMES.forEach(name => {
    const flags = health[name] || {};
    const pr      = flags.pr           || "grey";
    const temp    = flags.temp         || "grey";
    const dc      = flags.dc_current   || "grey";
    const ac      = flags.ac_power     || "grey";
    const overall = flags.overall_status || "grey";

    // Short label: TX1-01
    const shortName = name.replace("INV ", "");

    const card = document.createElement("div");
    card.className = `inverter-card status-${overall}`;
    card.title = `${name}\nPR: ${pr} | Temp: ${temp} | DC: ${dc} | AC: ${ac}`;
    card.innerHTML = `
      <div class="inv-name">${shortName}</div>
      <div class="led-row">
        ${ledHtml(pr,   "PR")}
        ${ledHtml(temp, "Temp")}
        ${ledHtml(dc,   "DC")}
        ${ledHtml(ac,   "AC")}
      </div>
      <div class="led-labels">
        <span class="led-label">PR</span>
        <span class="led-label">T</span>
        <span class="led-label">DC</span>
        <span class="led-label">AC</span>
      </div>
    `;
    grid.appendChild(card);
  });
}

// ─── 4. Active alerts ─────────────────────────────────────────────────────

function updateAlerts(data) {
  const alerts = data.active_anomalies || [];
  const container = el("alerts-container");

  if (alerts.length === 0) {
    container.innerHTML = '<div class="empty-state">No active alerts — all systems nominal</div>';
    return;
  }

  container.innerHTML = alerts.map(a => `
    <div class="alert-item ${a.severity || "Info"}">
      <span class="alert-timestamp">${a.timestamp || "—"}</span>
      <span>
        <span class="alert-inverter">${a.inverter || "—"}</span>
        <span class="alert-type"> — ${a.type || "Unknown"}</span>
        <span class="alert-details">&nbsp;(${a.details || ""})</span>
      </span>
      <span class="severity-badge ${a.severity || "Info"}">${a.severity || "Info"}</span>
    </div>
  `).join("");
}

// ─── 5. Historical alarm trail ────────────────────────────────────────────

function updateHistory(data) {
  const trail = (data.historical_trail || []).slice(0, 100);
  const tbody = el("history-tbody");

  if (trail.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No historical alarms for today</td></tr>';
    return;
  }

  // Newest first
  const sorted = [...trail].sort((a, b) => (b.timestamp || "").localeCompare(a.timestamp || ""));

  tbody.innerHTML = sorted.map(a => `
    <tr>
      <td>${a.timestamp || "—"}</td>
      <td>${a.inverter  || "—"}</td>
      <td>${a.type      || "—"}</td>
      <td><span class="severity-badge ${a.severity || "Info"}">${a.severity || "Info"}</span></td>
      <td>${a.details   || ""}</td>
    </tr>
  `).join("");
}

// ─── Main update ──────────────────────────────────────────────────────────

async function updateDashboard() {
  try {
    const resp = await fetch("/api/status");
    if (!resp.ok) return;
    const data = await resp.json();

    if (!data || Object.keys(data).length === 0) return;

    updateMacro(data);
    updateIngestion(data);
    updateInverterGrid(data);
    updateAlerts(data);
    updateHistory(data);

    el("last-updated").textContent = `Last updated: ${now()}`;
  } catch (err) {
    console.warn("Poll failed:", err);
  }
}

// ─── Bootstrap ────────────────────────────────────────────────────────────
async function handleRescan() {
  const btn = el("rescan-btn");
  if (!btn || btn.classList.contains("loading")) return;

  if (!confirm("Are you sure you want to delete today's analysis and re-run all forensic rules?")) return;

  btn.classList.add("loading");
  btn.textContent = "RE-SCANNING";

  try {
    const resp = await fetch("/api/forensic/rescan", { method: "POST" });
    const result = await resp.json();
    
    if (result.status === "success") {
      alert("Rescan complete! Redrawing dashboard...");
      await updateDashboard();
    } else {
      alert("Rescan failed: " + result.message);
    }
  } catch (err) {
    alert("Error triggering rescan: " + err);
  } finally {
    btn.classList.remove("loading");
    btn.textContent = "FORENSIC RE-SCAN";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  updateDashboard();
  setInterval(updateDashboard, POLL_INTERVAL_MS);

  const rescanBtn = el("rescan-btn");
  if (rescanBtn) {
    rescanBtn.addEventListener("click", handleRescan);
  }
});
