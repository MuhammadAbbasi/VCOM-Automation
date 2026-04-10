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

let socket;
let reconnectInterval = 2000;
let currentConfig = null;
let historicalData = [];

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
  
  // Header Metadata
  const start = m.plant_start_time || "--:--";
  const fetch = m.last_data_fetch ? m.last_data_fetch.substring(11, 16) : "--:--";
  
  el("meta-start-time").textContent = `Plant Start: ${start}`;
  el("meta-last-fetch").textContent = `Latest Data: ${fetch}`;
  
  // Downtime Subtitle
  const sub = el("downtime-subtitle");
  if (sub) {
    sub.textContent = `(Daylight Hours Starting from Production @ ${start})`;
  }
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
      <span class="alert-timestamp">${a.trip_time || "—"}</span>
      <span>
        <span class="alert-inverter">${a.inverter || "—"}</span>
        <span class="alert-type"> — ${a.type || "Unknown"}</span>
        <span class="alert-details">&nbsp;(${a.message || ""})</span>
      </span>
      <span class="severity-badge ${a.severity || "Info"}">${a.severity || "Info"}</span>
    </div>
  `).join("");
}

// ─── 5. Historical alarm trail ────────────────────────────────────────────

function updateHistory(data) {
  historicalData = data.historical_trail || [];
  
  // Extract unique categories and populate dropdown if it exists
  const filterEl = el("history-filter");
  if (filterEl) {
    const currentVal = filterEl.value;
    const uniqueCats = new Set(historicalData.map(a => a.type));
    
    // Check if new categories appeared or we need to rebuild
    const existingOptions = Array.from(filterEl.options).map(o => o.value);
    let changed = false;
    for (const c of uniqueCats) {
      if (!existingOptions.includes(c)) changed = true;
    }
    
    if (changed || existingOptions.length - 1 !== uniqueCats.size) {
      const opts = ['<option value="ALL">All Categories</option>'];
      [...uniqueCats].sort().forEach(c => {
        opts.push(`<option value="${c}" ${c === currentVal ? 'selected' : ''}>${c}</option>`);
      });
      filterEl.innerHTML = opts.join("");
    }
  }

  renderHistoryTable();
}

function renderHistoryTable() {
  const tbody = el("history-tbody");
  if (!tbody || !historicalData) return;

  const filterEl = el("history-filter");
  const filterVal = filterEl ? filterEl.value : "ALL";

  const filteredArr = filterVal === "ALL" ? historicalData : historicalData.filter(a => a.type === filterVal);
  
  if (filteredArr.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No historical alarms for today</td></tr>';
    return;
  }

  // Newest first (using recovery_time first, fallback to trip_time)
  const sorted = [...filteredArr].sort((a, b) => (b.recovery_time || b.trip_time || "").localeCompare(a.recovery_time || a.trip_time || "")).slice(0, 100);

  tbody.innerHTML = sorted.map(a => `
    <tr>
      <td style="font-size:0.8rem">Recovered:<br/>${a.recovery_time || "—"}</td>
      <td>${a.inverter  || "—"}</td>
      <td>${a.type      || "—"}</td>
      <td><span class="severity-badge ${a.severity || "Info"}">${a.severity || "Info"}</span></td>
      <td>${a.message   || ""}</td>
    </tr>
  `).join("");
}

// ─── 6. Downtime Tracker ────────────────────────────────────────────────────

function updateDowntime(data) {
  const downtime = data.downtime_tracker || {};
  const tbody = el("downtime-tbody");

  const keys = Object.keys(downtime);
  if (keys.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-state">No downtime reported today</td></tr>';
    return;
  }

  // Sort by total time off descending
  const sorted = keys.map(k => downtime[k]).sort((a, b) => b.total_time_off - a.total_time_off);

  tbody.innerHTML = sorted.map(d => `
    <tr>
      <td>${d.inverter || "—"}</td>
      <td>${d.last_data_fetched || "—"}</td>
      <td>${d.last_poa || "—"}</td>
      <td>${d.time_stopped || "—"}</td>
      <td>${d.started_again || "—"}</td>
      <td><strong>${d.total_time_off || "0"}</strong></td>
    </tr>
  `).join("");
}

// ─── WebSocket and Config ───────────────────────────────────────────────────

function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws`;
  
  socket = new WebSocket(wsUrl);
  
  socket.onopen = () => {
    el("last-updated").textContent = `Connected: ${now()}`;
    reconnectInterval = 2000;
  };
  
  socket.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === "data_update") {
        const data = msg.data;
        if (!data || Object.keys(data).length === 0) return;
        updateMacro(data);
        updateIngestion(data);
        updateInverterGrid(data);
        updateAlerts(data);
        updateHistory(data);
        updateDowntime(data);
        el("last-updated").textContent = `Last updated: ${now()}`;
      } else if (msg.type === "config_update") {
        applyConfig(msg.data);
      }
    } catch (err) {
      console.warn("WS message parse error:", err);
    }
  };
  
  socket.onclose = () => {
    el("last-updated").textContent = `Disconnected. Reconnecting...`;
    setTimeout(connectWebSocket, reconnectInterval);
    reconnectInterval = Math.min(reconnectInterval * 1.5, 30000);
  };
}

function applyConfig(config) {
  currentConfig = config;
  if (!config) return;

  // Apply colors to CSS variables
  if (config.colors) {
    const root = document.documentElement;
    if (config.colors.green) root.style.setProperty('--green', config.colors.green);
    if (config.colors.yellow) root.style.setProperty('--yellow', config.colors.yellow);
    if (config.colors.red) root.style.setProperty('--red', config.colors.red);
    if (config.colors.grey) root.style.setProperty('--grey', config.colors.grey);
  }

  // Populate Settings form
  if (config.thresholds) {
    const t = config.thresholds;
    if (t.pr) {
      if (el("cfg-pr-green")) el("cfg-pr-green").value = t.pr.green;
      if (el("cfg-pr-yellow")) el("cfg-pr-yellow").value = t.pr.yellow;
    }
    if (t.temp) {
      if (el("cfg-temp-yellow")) el("cfg-temp-yellow").value = t.temp.yellow;
      if (el("cfg-temp-red")) el("cfg-temp-red").value = t.temp.red;
    }
    if (t.ac) {
      if (el("cfg-ac-green")) el("cfg-ac-green").value = t.ac.green;
      if (el("cfg-ac-yellow")) el("cfg-ac-yellow").value = t.ac.yellow;
    }
    if (t.dc) {
      if (el("cfg-dcm-green")) el("cfg-dcm-green").value = t.dc.morning_green;
      if (el("cfg-dcm-yellow")) el("cfg-dcm-yellow").value = t.dc.morning_yellow;
      if (el("cfg-dca-green")) el("cfg-dca-green").value = t.dc.afternoon_green;
      if (el("cfg-dca-yellow")) el("cfg-dca-yellow").value = t.dc.afternoon_yellow;
    }
    if (el("cfg-min-downtime") && t.min_downtime_minutes !== undefined) {
      el("cfg-min-downtime").value = t.min_downtime_minutes;
    }
  }
  if (config.colors) {
    const c = config.colors;
    if (el("cfg-color-green")) el("cfg-color-green").value = c.green;
    if (el("cfg-color-yellow")) el("cfg-color-yellow").value = c.yellow;
    if (el("cfg-color-red")) el("cfg-color-red").value = c.red;
    if (el("cfg-color-grey")) el("cfg-color-grey").value = c.grey;
  }
}

async function handleSaveSettings() {
  const btn = el("save-settings-btn");
  if (btn.classList.contains("loading")) return;
  
  btn.classList.add("loading");
  btn.textContent = "SAVING...";

  const newConfig = {
    thresholds: {
      pr: {
        green: parseFloat(el("cfg-pr-green").value) || 85.0,
        yellow: parseFloat(el("cfg-pr-yellow").value) || 75.0
      },
      temp: {
        yellow: parseFloat(el("cfg-temp-yellow").value) || 40.0,
        red: parseFloat(el("cfg-temp-red").value) || 45.0
      },
      ac: {
        green: parseFloat(el("cfg-ac-green").value) || 5000,
        yellow: parseFloat(el("cfg-ac-yellow").value) || 1000
      },
      dc: {
        morning_green: parseFloat(el("cfg-dcm-green").value) || 10.0,
        morning_yellow: parseFloat(el("cfg-dcm-yellow").value) || 2.0,
        afternoon_green: parseFloat(el("cfg-dca-green").value) || 5.0,
        afternoon_yellow: parseFloat(el("cfg-dca-yellow").value) || 0.5
      },
      min_downtime_minutes: parseFloat(el("cfg-min-downtime").value) !== undefined && !isNaN(parseFloat(el("cfg-min-downtime").value)) ? parseFloat(el("cfg-min-downtime").value) : 9
    },
    colors: {
      green: el("cfg-color-green").value || "#10b981",
      yellow: el("cfg-color-yellow").value || "#f59e0b",
      red: el("cfg-color-red").value || "#ef4444",
      grey: el("cfg-color-grey").value || "#6b7280"
    }
  };

  try {
    const resp = await fetch("/api/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(newConfig)
    });
    
    if (resp.ok) {
      el("settings-modal").classList.add("modal-hidden");
    } else {
      alert("Failed to save settings.");
    }
  } catch (err) {
    alert("Error saving settings: " + err);
  } finally {
    btn.classList.remove("loading");
    btn.textContent = "SAVE & APPLY";
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
  connectWebSocket();

  const rescanBtn = el("rescan-btn");
  if (rescanBtn) rescanBtn.addEventListener("click", handleRescan);

  const settingsBtn = el("settings-btn");
  const closeSettingsBtn = el("close-settings");
  const saveSettingsBtn = el("save-settings-btn");
  const settingsModal = el("settings-modal");

  if (settingsBtn && settingsModal) {
    settingsBtn.addEventListener("click", () => {
      settingsModal.classList.remove("modal-hidden");
    });
  }
  
  if (closeSettingsBtn && settingsModal) {
    closeSettingsBtn.addEventListener("click", () => {
      settingsModal.classList.add("modal-hidden");
    });
  }

  if (saveSettingsBtn) {
    saveSettingsBtn.addEventListener("click", handleSaveSettings);
  }

  // History Filter
  const filterEl = el("history-filter");
  if (filterEl) {
    filterEl.addEventListener("change", renderHistoryTable);
  }
});
