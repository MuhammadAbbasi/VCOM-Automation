/**
 * app.js — Mazara SCADA Monitor frontend logic.
 *
 * WebSocket-based real-time dashboard with 5 tabs:
 *   Tab 1 (Overview): Macro health, ingestion, LED matrix, alerts, downtime
 *   Tab 2 (PR): Performance Ratio detail table for all 36 inverters
 *   Tab 3 (Temp): Temperature detail table for all 36 inverters
 *   Tab 4 (DC): DC Current detail table for all 36 inverters
 *   Tab 5 (AC): AC Power detail table for all 36 inverters
 */

let socket;
let reconnectInterval = 2000;
let currentConfig = null;
let historicalData = [];
let lastData = null; // cache for tab switching

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

// Sort state for detail tables
const sortState = {
  pr:   { column: "value", direction: "desc" },
  temp: { column: "value", direction: "desc" },
  dc:   { column: "value", direction: "desc" },
  ac:   { column: "value", direction: "desc" },
};

// ─── Helpers ──────────────────────────────────────────────────────────────

function el(id) { return document.getElementById(id); }

function safeNum(v, fallback = "—") {
  return (v !== null && v !== undefined && !isNaN(v)) ? v : fallback;
}

function now() {
  return new Date().toLocaleTimeString("it-IT", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function getDomain(name) {
  // "INV TX1-01" → "TX1"
  return name.replace("INV ", "").substring(0, 3);
}

// ─── 1. Macro health ──────────────────────────────────────────────────────

function updateMacro(data) {
  const m = data.macro_health || {};
  el("val-total").textContent   = safeNum(m.total_inverters, 36);
  el("val-online").textContent  = safeNum(m.online, "—");
  el("val-tripped").textContent = safeNum(m.tripped, "—");
  el("val-comms").textContent   = safeNum(m.comms_lost, "—");
  
  // Update Sensor Macro
  const sData = data.sensor_data || {};
  // Try to find a POA value to show on overview
  const poaKey = Object.keys(sData).find(k => k.includes("POA"));
  if (poaKey && el("val-poa")) {
      const pVal = sData[poaKey];
      el("val-poa").textContent = (typeof pVal === 'number') ? `${pVal.toFixed(1)} W/m²` : pVal;
  }

  
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

function ledHtml(color, label, value) {
  let displayVal = "";
  if (value !== undefined && value !== null) {
    if (typeof value === "number") {
      if (label === "PR") displayVal = `: ${value.toFixed(1)}%`;
      else if (label === "Temp") displayVal = `: ${value.toFixed(1)}°C`;
      else if (label === "DC") displayVal = `: ${value.toFixed(1)}A`;
      else if (label === "AC") {
          displayVal = (value >= 1000) ? `: ${(value/1000).toFixed(1)}kW` : `: ${Math.round(value)}W`;
      }
      else displayVal = `: ${value}`;
    } else {
      displayVal = `: ${value}`;
    }
  }
  return `<span class="led ${color}" title="${label}${displayVal}"></span>`;
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
    card.title = `${name}`;
    
    // Preparation for tooltips
    const prT   = ledHtml(pr,   "PR",   flags.pr_v);
    const tempT = ledHtml(temp, "Temp", flags.temp_v);
    const dcT   = ledHtml(dc,   "DC",   flags.dc_v);
    const acT   = ledHtml(ac,   "AC",   flags.ac_v);
    
    // Extract titles for labels
    const getTitle = (html) => {
        const match = html.match(/title="([^"]+)"/);
        return match ? match[1] : "";
    };
    
    card.innerHTML = `
      <div class="inv-name">${shortName}</div>
      <div class="led-row">
        ${prT} ${tempT} ${dcT} ${acT}
      </div>
      <div class="led-labels">
        <span class="led-label" title="${getTitle(prT)}">PR</span>
        <span class="led-label" title="${getTitle(tempT)}">T</span>
        <span class="led-label" title="${getTitle(dcT)}">DC</span>
        <span class="led-label" title="${getTitle(acT)}">AC</span>
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

// ═══════════════════════════════════════════════════════════
// 7. DETAIL TABS (PR, Temperature, DC Current, AC Power)
// ═══════════════════════════════════════════════════════════

/**
 * Generic detail table renderer.
 * @param {Object}  opts
 * @param {string}  opts.metric    - "pr" | "temp" | "dc" | "ac"
 * @param {string}  opts.valueKey  - key in inverter_health flags, e.g. "pr_v"
 * @param {string}  opts.statusKey - key for LED status, e.g. "pr"
 * @param {string}  opts.unit      - "%" | "°C" | "A" | "W"
 * @param {string}  opts.tbodyId   - target <tbody> id
 * @param {Object}  opts.statIds   - { avg, max, min, extra } element IDs
 * @param {boolean} opts.higherIsBetter - true for PR/DC/AC, false for Temp
 * @param {Function} opts.formatValue - custom value formatter
 * @param {Function} opts.formatExtra - custom extra stat formatter
 * @param {Object}  data - full data payload
 */
function renderDetailTable(opts, data) {
  const health = data.inverter_health || {};
  const tbody  = el(opts.tbodyId);
  if (!tbody) return;

  // Collect all values
  const rows = [];
  INVERTER_NAMES.forEach(name => {
    const flags = health[name] || {};
    const rawVal = flags[opts.valueKey];
    const status = flags[opts.statusKey] || "grey";
    const domain = getDomain(name);
    const shortName = name.replace("INV ", "");

    rows.push({
      name,
      shortName,
      domain,
      value: (rawVal !== null && rawVal !== undefined && !isNaN(rawVal)) ? Number(rawVal) : null,
      status,
    });
  });

  // Calculate statistics
  const validValues = rows.filter(r => r.value !== null).map(r => r.value);
  const avg = validValues.length > 0 ? validValues.reduce((s, v) => s + v, 0) / validValues.length : null;
  const maxVal = validValues.length > 0 ? Math.max(...validValues) : null;
  const minVal = validValues.length > 0 ? Math.min(...validValues) : null;

  // Update stat cards
  if (el(opts.statIds.avg)) {
    el(opts.statIds.avg).textContent = avg !== null ? opts.formatValue(avg) : "—";
  }
  if (el(opts.statIds.max)) {
    el(opts.statIds.max).textContent = maxVal !== null ? opts.formatValue(maxVal) : "—";
  }
  if (el(opts.statIds.min)) {
    el(opts.statIds.min).textContent = minVal !== null ? opts.formatValue(minVal) : "—";
  }
  if (el(opts.statIds.extra) && opts.formatExtra) {
    el(opts.statIds.extra).textContent = opts.formatExtra(rows, validValues, avg);
  }

  // Sort rows
  const state = sortState[opts.metric];
  const sorted = [...rows].sort((a, b) => {
    let va, vb;
    switch (state.column) {
      case "name": va = a.shortName; vb = b.shortName; break;
      case "domain": va = a.domain; vb = b.domain; break;
      case "value":
      case "bar":
        va = a.value !== null ? a.value : -Infinity;
        vb = b.value !== null ? b.value : -Infinity;
        break;
      default: va = a.value; vb = b.value;
    }
    let cmp = 0;
    if (typeof va === "string") cmp = va.localeCompare(vb);
    else cmp = (va > vb) ? 1 : (va < vb) ? -1 : 0;
    return state.direction === "asc" ? cmp : -cmp;
  });

  // Calculate bar scale — use max of all values for 100% width
  const barMax = maxVal || 1;

  // Render rows
  if (sorted.length === 0 || validValues.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty-state">No data available yet</td></tr>`;
    return;
  }

  tbody.innerHTML = sorted.map(r => {
    const valStr = r.value !== null ? opts.formatValue(r.value) : "—";
    const statusLabel = getStatusLabel(r.status);

    // Deviation from average
    let devHtml = '<span class="deviation neutral">—</span>';
    if (r.value !== null && avg !== null && avg !== 0) {
      const devPct = ((r.value - avg) / avg) * 100;
      const sign = devPct >= 0 ? "+" : "";
      let devClass = "neutral";
      if (opts.higherIsBetter) {
        devClass = devPct >= 1 ? "positive" : devPct <= -3 ? "negative" : "neutral";
      } else {
        // For temp: lower is better
        devClass = devPct <= -1 ? "positive" : devPct >= 3 ? "negative" : "neutral";
      }
      devHtml = `<span class="deviation ${devClass}">${sign}${devPct.toFixed(1)}%</span>`;
    }

    // Distribution bar
    const barPct = r.value !== null ? Math.max(0, Math.min(100, (r.value / barMax) * 100)) : 0;
    const avgPct = avg !== null ? Math.max(0, Math.min(100, (avg / barMax) * 100)) : 0;

    return `
      <tr>
        <td><strong>${r.shortName}</strong></td>
        <td><span class="domain-badge ${r.domain}">${r.domain}</span></td>
        <td class="value-cell">${valStr}</td>
        <td>${statusLabel}</td>
        <td>${devHtml}</td>
        <td>
          <div class="dist-bar-container">
            <div class="dist-bar ${r.status}" style="width:${barPct}%"></div>
            <div class="dist-avg-marker" style="left:${avgPct}%" title="Plant Avg: ${avg !== null ? opts.formatValue(avg) : '—'}"></div>
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function getStatusLabel(status) {
  const labels = {
    green:  "OK",
    yellow: "Warning",
    red:    "Critical",
    grey:   "No Data",
  };
  return `<span class="status-pill ${status}"><span class="pill-dot"></span>${labels[status] || status}</span>`;
}

// ─── PR Detail ────────────────────────────────────────────────────────────

function updatePRDetail(data) {
  renderDetailTable({
    metric: "pr",
    valueKey: "pr_v",
    statusKey: "pr",
    unit: "%",
    tbodyId: "pr-tbody",
    statIds: { avg: "pr-avg", max: "pr-max", min: "pr-min", extra: "pr-ok-count" },
    higherIsBetter: true,
    formatValue: (v) => `${v.toFixed(1)}%`,
    formatExtra: (rows, vals) => {
      const okCount = vals.filter(v => v >= 85).length;
      return `${okCount} / ${vals.length}`;
    },
  }, data);
}

// ─── Temperature Detail ──────────────────────────────────────────────────

function updateTempDetail(data) {
  renderDetailTable({
    metric: "temp",
    valueKey: "temp_v",
    statusKey: "temp",
    unit: "°C",
    tbodyId: "temp-tbody",
    statIds: { avg: "temp-avg", max: "temp-max", min: "temp-min", extra: "temp-ok-count" },
    higherIsBetter: false,
    formatValue: (v) => `${v.toFixed(1)}°C`,
    formatExtra: (rows, vals) => {
      const okCount = vals.filter(v => v <= 40).length;
      return `${okCount} / ${vals.length}`;
    },
  }, data);
}

// ─── DC Current Detail ────────────────────────────────────────────────────

function updateDCDetail(data) {
  const container = document.getElementById("dc-tbody");
  if (!container || !data || !data.inverter_health) return;

  const invIds = Object.keys(data.inverter_health).sort();
  const rows = invIds.map(id => {
    const h = data.inverter_health[id];
    return {
      id: id,
      shortName: id,
      domain: h.domain || id.split("-")[0].replace("INV ", ""),
      value: h.dc_v,
      status: h.dc_current || "grey",
      mppt_data: h.mppt_data || []
    };
  });

  // Stats
  const validValues = rows.filter(r => r.value !== null).map(r => r.value);
  const avg = validValues.length > 0 ? validValues.reduce((a, b) => a + b, 0) / validValues.length : 0;
  const maxVal = validValues.length > 0 ? Math.max(...validValues) : 0;
  const minVal = validValues.length > 0 ? Math.min(...validValues) : 0;
  
  document.getElementById("dc-avg").textContent = `${avg.toFixed(2)} A`;
  document.getElementById("dc-max").textContent = `${maxVal.toFixed(2)} A`;
  document.getElementById("dc-min").textContent = `${minVal.toFixed(2)} A`;
  document.getElementById("dc-ok-count").textContent = `${rows.filter(r => r.status === "green").length} / ${rows.length}`;

  // Sorting (Reuse state from renderDetailTable if exists)
  // For now just sort by ID
  const sorted = rows;

  container.innerHTML = sorted.map(r => {
    const valStr = r.value !== null ? `${r.value.toFixed(2)} A` : "—";
    const statusLabel = getStatusLabel(r.status);

    // Deviation
    let devHtml = '<span class="deviation neutral">—</span>';
    if (r.value !== null && avg !== null && avg !== 0) {
      const devPct = ((r.value - avg) / avg) * 100;
      const sign = devPct >= 0 ? "+" : "";
      const devClass = devPct >= 1 ? "positive" : devPct <= -5 ? "negative" : "neutral";
      devHtml = `<span class="deviation ${devClass}">${sign}${devPct.toFixed(1)}%</span>`;
    }

    // MPPT Grid
    let mpptGridHtml = '<div class="mppt-grid">';
    if (r.mppt_data && r.mppt_data.length > 0) {
        mpptGridHtml += r.mppt_data.map(m => {
            let mColor = "grey";
            if (m.v !== null && m.exp !== null && m.exp > 0.5) {
                const ratio = m.v / m.exp;
                if (ratio < 0.2) mColor = "red";
                else if (ratio < 0.7) mColor = "yellow";
                else mColor = "green";
            } else if (m.strings === 0) {
                mColor = "empty";
            }
            const title = `MPPT ${m.mppt} (${m.strings} strings): ${m.v !== null ? m.v.toFixed(1) : '—'}A (Exp: ${m.exp !== null ? m.exp.toFixed(1) : '—'}A)`;
            return `<div class="mppt-dot ${mColor} strings-${m.strings}" title="${title}"></div>`;
        }).join("");
    } else {
        mpptGridHtml += '<span class="empty-state">No MPPT data</span>';
    }
    mpptGridHtml += '</div>';

    return `
      <tr>
        <td><strong>${r.shortName}</strong></td>
        <td><span class="domain-badge ${r.domain}">${r.domain}</span></td>
        <td class="value-cell">${valStr}</td>
        <td>${statusLabel}</td>
        <td>${devHtml}</td>
        <td>${mpptGridHtml}</td>
      </tr>
    `;
  }).join("");
}

// ─── AC Power Detail ──────────────────────────────────────────────────────

function updateACDetail(data) {
  renderDetailTable({
    metric: "ac",
    valueKey: "ac_v",
    statusKey: "ac_power",
    unit: "W",
    tbodyId: "ac-tbody",
    statIds: { avg: "ac-avg", max: "ac-max", min: "ac-min", extra: "ac-total" },
    higherIsBetter: true,
    formatValue: (v) => v >= 1000 ? `${(v/1000).toFixed(1)} kW` : `${Math.round(v)} W`,
    formatExtra: (rows, vals) => {
      const total = vals.reduce((s, v) => s + v, 0);
      return total >= 1000000 ? `${(total/1000000).toFixed(2)} MW` : total >= 1000 ? `${(total/1000).toFixed(0)} kW` : `${Math.round(total)} W`;
    },
  }, data);
}

// ─── Sensors Detail ────────────────────────────────────────────────────────
function updateSensorsTab(data) {
    const sData = data.sensor_data || {};
    const container = el("sensor-grid-container");
    if (!container) return;

    if (Object.keys(sData).length === 0) {
        container.innerHTML = '<div class="empty-state">No sensor data available in this cycle.</div>';
        return;
    }

    container.innerHTML = "";
    const grid = document.createElement("div");
    grid.className = "sensor-container";

    // Macro stats for sensor tab
    const macroStats = el("sensor-macro-stats");
    if (macroStats) {
        let poaTotal = 0, poaCount = 0;
        Object.entries(sData).forEach(([k, v]) => {
            if (k.includes("POA")) { poaTotal += v; poaCount++; }
        });
        const avgPoa = poaCount > 0 ? (poaTotal / poaCount).toFixed(1) : "—";
        macroStats.innerHTML = `
            <div class="stat-card">
              <div class="stat-value">${avgPoa}</div>
              <div class="stat-label">Avg Plant POA</div>
            </div>
        `;
    }

    Object.entries(sData).forEach(([key, val]) => {
        const box = document.createElement("div");
        let type = "other";
        let icon = "📊";
        let unit = "—";
        let label = key;

        if (key.includes("POA")) { type = "poa"; icon = "☀️"; unit = "W/m²"; label = "Plane of Array"; }
        else if (key.includes("GHI")) { type = "ghi"; icon = "🌍"; unit = "W/m²"; label = "Global Horiz. Irrad."; }
        else if (key.includes("Temp") || key.includes("JB") && (key.includes("IT") || key.includes("AL"))) {
             type = "temp"; icon = "🌡️"; unit = "°C"; label = "Ambient/Module Temp";
        }
        
        if (key.includes("-DOWN")) label = "Module Temp (Lower)";
        if (key.includes("-UP")) label = "Module Temp (Upper)";
        if (key.includes("AL-")) { type = "al"; icon = "📏"; unit = "°C"; label = "Irradiance Sensor Temp"; }

        box.className = `sensor-box ${type}`;
        box.innerHTML = `
            <div class="sensor-header">
                <span class="sensor-id">${key}</span>
                <span class="sensor-type-icon">${icon}</span>
            </div>
            <div class="sensor-val-row">
                <span class="sensor-val">${typeof val === 'number' ? val.toFixed(1) : val}</span>
                <span class="sensor-unit">${unit}</span>
            </div>
            <div class="sensor-label">${label}</div>
        `;
        grid.appendChild(box);
    });
    container.appendChild(grid);
}



// ═══════════════════════════════════════════════════════════
// TAB NAVIGATION
// ═══════════════════════════════════════════════════════════

function initTabs() {
  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabContents = document.querySelectorAll(".tab-content");

  tabBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      const targetId = btn.dataset.tab;

      // Deactivate all
      tabBtns.forEach(b => b.classList.remove("active"));
      tabContents.forEach(c => c.classList.remove("active"));

      // Activate target
      btn.classList.add("active");
      const target = document.getElementById(targetId);
      if (target) target.classList.add("active");

      // Re-render detail tab with cached data if switching to it
      if (lastData && targetId !== "tab-overview") {
        renderActiveDetailTab();
      }
    });
  });
}

function renderActiveDetailTab() {
  if (!lastData) return;
  const activeTab = document.querySelector(".tab-content.active");
  if (!activeTab) return;
  const id = activeTab.id;

  if (id === "tab-pr")   updatePRDetail(lastData);
  if (id === "tab-temp") updateTempDetail(lastData);
  if (id === "tab-dc")   updateDCDetail(lastData);
  if (id === "tab-ac")   updateACDetail(lastData);
  if (id === "tab-sensors") updateSensorsTab(lastData);
}


// ─── Sort click handler for detail tables ──────────────────────────────────

function initSortableHeaders() {
  document.querySelectorAll(".detail-table th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      const table = th.closest("table");
      const tableId = table.id; // e.g. "pr-table"
      const metric = tableId.replace("-table", ""); // e.g. "pr"
      const col = th.dataset.sort;

      const state = sortState[metric];
      if (!state) return;

      // Toggle direction or change column
      if (state.column === col) {
        state.direction = state.direction === "asc" ? "desc" : "asc";
      } else {
        state.column = col;
        state.direction = "desc";
      }

      // Update header classes
      table.querySelectorAll("th.sortable").forEach(h => {
        h.classList.remove("sort-asc", "sort-desc");
      });
      th.classList.add(state.direction === "asc" ? "sort-asc" : "sort-desc");

      // Re-render
      renderActiveDetailTab();
    });
  });
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
        
        lastData = data; // cache data
        
        // Always update overview
        updateMacro(data);
        updateIngestion(data);
        updateInverterGrid(data);
        updateAlerts(data);
        updateHistory(data);
        updateDowntime(data);
        updateSensorsTab(data);
        
        // Update whichever detail tab is active

        renderActiveDetailTab();
        
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
  if (config.collection_interval !== undefined && el("cfg-collection-interval")) {
    el("cfg-collection-interval").value = config.collection_interval;
  }
  
  // Telegram status
  if (config.telegram) {
    const tg = config.telegram;
    if (el("cfg-tg-enabled")) el("cfg-tg-enabled").checked = !!tg.enabled;
    if (el("cfg-tg-token")) el("cfg-tg-token").value = tg.bot_token || "";
    if (el("cfg-tg-chat")) el("cfg-tg-chat").value = tg.chat_id || "";
    if (el("cfg-tg-personal")) el("cfg-tg-personal").value = tg.personal_id || "";
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
    },
    collection_interval: parseFloat(el("cfg-collection-interval").value) || 15,
    telegram: {
      enabled: el("cfg-tg-enabled").checked,
      bot_token: el("cfg-tg-token").value,
      chat_id: el("cfg-tg-chat").value,
      personal_id: el("cfg-tg-personal").value
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

async function handleTestTelegram() {
  const btn = el("test-tg-btn");
  if (!btn || btn.classList.contains("loading")) return;

  btn.classList.add("loading");
  const oldText = btn.textContent;
  btn.textContent = "SENDING...";

  try {
    const resp = await fetch("/api/telegram/test", { method: "POST" });
    const result = await resp.json();
    if (result.status === "success") {
      alert("Test message sent! Check your Telegram group.");
    } else {
      alert("Error: " + result.message);
    }
  } catch (err) {
    alert("Connection failed: " + err);
  } finally {
    btn.classList.remove("loading");
    btn.textContent = oldText;
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
      alert("Rescan triggered! The dashboard will update automatically in a few moments.");
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
  // Initialize tabs
  initTabs();
  initSortableHeaders();
  
  // WebSocket
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

  // Telegram test
  const testTgBtn = el("test-tg-btn");
  if (testTgBtn) testTgBtn.addEventListener("click", handleTestTelegram);

  // History Filter
  const filterEl = el("history-filter");
  if (filterEl) {
    filterEl.addEventListener("change", renderHistoryTable);
  }
});
