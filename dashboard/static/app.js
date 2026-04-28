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
let lastTrackerData = null;
let currentNcuFilter = "all";
let activeAlertFilter = "ALL";

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
  Potenza_Attiva: "Grid Limit (Power Control)",
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

function updateDashboard(data) {
  if (!data || Object.keys(data).length === 0) return;
  updateMacro(data);
  updateIngestion(data);
  updateInverterGrid(data);
  updateAlerts(data);
  updateHistory(data);
  updateDowntime(data);
  updateSensorsTab(data);
}

// ─── 1. Macro health ──────────────────────────────────────────────────────

function updateMacro(data) {
  const m = data.macro_health || {};
  el("val-total").textContent   = safeNum(m.total_inverters, 36);
  el("val-online").textContent  = safeNum(m.online, "—");
  el("val-tripped").textContent = safeNum(m.tripped, "—");
  el("val-comms").textContent   = safeNum(m.comms_lost, "—");
  
  // Update New Macro Metrics
  if (el("val-total-power")) {
      const pMw = m.total_ac_power_mw || 0;
      el("val-total-power").textContent = `${pMw.toFixed(2)} MW`;
  }
  if (el("val-daily-energy")) {
      const eMwh = m.total_energy_mwh || 0;
      el("val-daily-energy").textContent = `${eMwh.toFixed(2)} MWh`;
  }
  if (el("val-avg-pr")) {
      const avgPr = m.avg_pr || 0;
      el("val-avg-pr").textContent = `${avgPr.toFixed(1)}%`;
  }
  
  // Update Sensor Macro
  const sData = data.sensor_data || {};
  // Try to find a POA value to show on overview
  let poaKey = Object.keys(sData).find(k => k.toUpperCase().includes("POA"));
  // Fallback to any Irradiance key if POA not found
  if (!poaKey) {
      poaKey = Object.keys(sData).find(k => k.includes("Irraggiamento"));
  }

  if (poaKey && el("val-poa")) {
      const pVal = sData[poaKey];
      // If it's a number, format it; otherwise show as is (unless it's just the header name)
      if (typeof pVal === 'number') {
          el("val-poa").textContent = `${pVal.toFixed(1)} W/m²`;
      } else if (typeof pVal === 'string' && !pVal.includes("Irraggiamento")) {
          el("val-poa").textContent = pVal;
      } else {
          el("val-poa").textContent = "—";
      }
  }
  
  // Update Grid Limit
  if (el("val-grid-limit")) {
      // Standard is 87.6% (maximum allowed for this plant)
      const limit = m.grid_limit !== undefined ? m.grid_limit : 87.6;
      el("val-grid-limit").textContent = `${limit.toFixed(1)}%`;
      
      const card = el("card-grid-limit");
      if (limit < 87.5) {
          // Critical drop below allowed maximum
          card.classList.add("alert-red");
          card.classList.remove("alert-yellow", "normal");
      } else {
          // Normal production at or above 87.6%
          card.classList.remove("alert-red", "alert-yellow");
          card.classList.add("normal");
      }
  }

  
  // Header Metadata
  const start = m.plant_start_time || "--:--";
  const fetch = m.last_data_fetch ? m.last_data_fetch.substring(11, 16) : "--:--";
  
  el("meta-start-time").textContent = start;
  el("meta-last-fetch").textContent = fetch;
  
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

  // Add Tracker Status Card
  if (lastTrackerData && lastTrackerData.length > 0) {
    const latest = [...lastTrackerData].sort((a,b) => (b.last_update || "").localeCompare(a.last_update || ""))[0];
    const ts = latest.last_update ? latest.last_update.replace("T", " ").substring(11, 19) : "—";
    
    const card = document.createElement("div");
    card.className = `card file-card success`;
    card.style.borderLeft = "4px solid var(--accent)";
    card.innerHTML = `
      <span class="file-name">TRACKER FIELD</span>
      <span class="file-status">connected</span>
      <span class="file-time">${ts}</span>
    `;
    grid.appendChild(card);
  }
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
      else if (label === "ISO") {
          displayVal = `: ${value.toFixed(0)} kΩ`;
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
  
  // Group inverters by TX
  const groups = {};
  INVERTER_NAMES.forEach(name => {
      const tx = getDomain(name);
      if (!groups[tx]) groups[tx] = [];
      groups[tx].push(name);
  });

  Object.entries(groups).sort().forEach(([tx, inverters]) => {
      const groupContainer = document.createElement("div");
      groupContainer.className = "inverter-group";
      groupContainer.innerHTML = `<div class="group-title">${tx}</div><div class="group-cards"></div>`;
      const cardsContainer = groupContainer.querySelector(".group-cards");
      
      inverters.forEach(name => {
        const flags = health[name] || {};
        const pr      = flags.pr           || "grey";
        const temp    = flags.temp         || "grey";
        const dc      = flags.dc_current   || "grey";
        const ac      = flags.ac_power     || "grey";
        const iso     = flags.iso          || "grey";
        const overall = flags.overall_status || "grey";

        const shortName = name.replace("INV ", "");
        const card = document.createElement("div");
        card.className = `inverter-card status-${overall}`;
        card.title = `${name}`;
        
        const prT   = ledHtml(pr,   "PR",   flags.pr_v);
        const tempT = ledHtml(temp, "Temp", flags.temp_v);
        const dcT   = ledHtml(dc,   "DC",   flags.dc_v);
        const acT   = ledHtml(ac,   "AC",   flags.ac_v);
        const isoT  = ledHtml(iso,  "ISO",  flags.iso_v);
        
        const getTitle = (html) => {
            const match = html.match(/title="([^"]+)"/);
            return match ? match[1] : "";
        };
        
        card.innerHTML = `
          <div class="inv-name">${shortName}</div>
          <div class="led-row">
            ${prT} ${tempT} ${dcT} ${acT} ${isoT}
          </div>
          <div class="led-labels">
            <span class="led-label" title="${getTitle(prT)}">PR</span>
            <span class="led-label" title="${getTitle(tempT)}">T</span>
            <span class="led-label" title="${getTitle(dcT)}">DC</span>
            <span class="led-label" title="${getTitle(acT)}">AC</span>
            <span class="led-label" title="${getTitle(isoT)}">ISO</span>
          </div>
        `;
        cardsContainer.appendChild(card);
      });
      grid.appendChild(groupContainer);
  });
}

// ─── 4. Active alerts ─────────────────────────────────────────────────────

function updateAlerts(data) {
  const alerts = data.active_anomalies || [];
  const containerCrit = el("alerts-container-critical");
  const containerOthers = el("alerts-container-others");

  if (!containerCrit) return;

  const getSevClass = (a) => {
    const s = (a.severity || "").toLowerCase();
    if (s.includes("red") || s.includes("crit")) return "red";
    if (s.includes("yellow") || s.includes("warn")) return "yellow";
    return "info";
  };

  const alertTemplate = (a) => {
    const sevClass = getSevClass(a);
    const titleText = `${a.inverter || "Unit"} · ${a.type || "Alert"}`;
    const timeDisplay = (a.trip_time || "").includes('T') ? a.trip_time.split('T')[1] : (a.trip_time || "—");
    
    return `
      <div class="alert-item ${sevClass}">
        <div class="alert-header-row">
          <span class="alert-title">${titleText}</span>
          <div class="header-right-meta">
            <span class="alert-time-small">${timeDisplay}</span>
          </div>
        </div>
        <div class="alert-body-text">
           <span class="alert-id-pill">${(a.id || "").slice(-4)}</span> ${a.message || ""}
        </div>
      </div>
    `;
  };

  const criticals = alerts.filter(a => getSevClass(a) === "red")
    .sort((a, b) => (b.trip_time || "").localeCompare(a.trip_time || ""));
    
  const others = alerts.filter(a => getSevClass(a) !== "red")
    .sort((a, b) => {
       const pa = (getSevClass(a) === "yellow" ? 1 : 2);
       const pb = (getSevClass(b) === "yellow" ? 1 : 2);
       if (pa !== pb) return pa - pb;
       return (b.trip_time || "").localeCompare(a.trip_time || "");
    });

  if (containerCrit) {
    containerCrit.innerHTML = criticals.length > 0 
      ? criticals.map(alertTemplate).join("") 
      : '<div class="empty-state" style="padding:1rem; font-size:0.7rem; opacity:0.6;">No critical alerts</div>';
  }

  if (containerOthers) {
    containerOthers.innerHTML = others.length > 0 
      ? others.map(alertTemplate).join("") 
      : '<div class="empty-state" style="padding:1rem; font-size:0.7rem; opacity:0.6;">No active warnings or info messages</div>';
  }
}

// Add event listeners for alert filter chips
function initAlertFilters() {
    const chips = document.querySelectorAll("#alert-filter-chips .filter-chip");
    chips.forEach(chip => {
        chip.addEventListener("click", () => {
            chips.forEach(c => c.classList.remove("active"));
            chip.classList.add("active");
            activeAlertFilter = chip.getAttribute("data-filter");
            if (lastData) updateAlerts(lastData);
        });
    });
}

// ─── 5. Historical alarm trail ────────────────────────────────────────────

function updateHistory(data) {
  historicalData = data.historical_trail || [];
  
  const filterEl = el("history-filter");
  if (filterEl) {
    const currentVal = filterEl.value;
    const uniqueCats = new Set(historicalData.map(a => a.type));
    
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

  renderHistoryTiles();
}

function renderHistoryTiles() {
  const container = el("historical-container");
  if (!container || !historicalData) return;

  const filterEl = el("history-filter");
  const filterVal = filterEl ? filterEl.value : "ALL";

  const getSevClass = (a) => {
    const s = (a.severity || "").toLowerCase();
    if (s.includes("red") || s.includes("crit")) return "red";
    if (s.includes("yellow") || s.includes("warn")) return "yellow";
    return "info";
  };

  const filteredArr = filterVal === "ALL" ? historicalData : historicalData.filter(a => a.type === filterVal);
  
  if (filteredArr.length === 0) {
    container.innerHTML = '<div class="empty-state" style="padding:2rem;">No historical alarms matching filter</div>';
    return;
  }

  // Newest first
  const sorted = [...filteredArr].sort((a, b) => (b.recovery_time || b.trip_time || "").localeCompare(a.recovery_time || a.trip_time || "")).slice(0, 40);

  container.innerHTML = sorted.map(a => {
    const sevClass = getSevClass(a);
    const titleText = `${a.inverter || "Unit"} · ${a.type || "Event"}`;
    // Show recovery time as primary, trip time as secondary
    const timeDisplay = (a.recovery_time || "").includes('T') ? a.recovery_time.split('T')[1] : (a.recovery_time || "—");
    const tripDisplay = (a.trip_time || "").includes('T') ? a.trip_time.split('T')[1] : (a.trip_time || "—");
    
    return `
      <div class="alert-item ${sevClass}" style="filter: grayscale(0.5); opacity: 0.85;">
        <div class="alert-header-row">
          <span class="alert-title">${titleText}</span>
          <div class="header-right-meta">
            <span class="alert-time-small" title="Trip: ${tripDisplay}">Rec: ${timeDisplay}</span>
          </div>
        </div>
        <div class="alert-body-text" style="font-size: 0.65rem;">
           <span class="alert-id-pill" style="opacity:0.6">${(a.id || "").slice(-4)}</span> ${a.message || ""}
        </div>
      </div>
    `;
  }).join("");
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
    
    // Macro stats for sensor tab
    const macroStats = el("sensor-macro-stats");
    if (macroStats) {
        let poaTotal = 0, poaCount = 0;
        Object.entries(sData).forEach(([k, v]) => {
            if (k.toUpperCase().includes("POA") && typeof v === 'number') { 
                poaTotal += v; poaCount++; 
            }
        });
        const avgPoa = poaCount > 0 ? (poaTotal / poaCount).toFixed(1) : "—";
        macroStats.innerHTML = `
            <div class="stat-card">
              <div class="stat-value">${avgPoa}</div>
              <div class="stat-label">Avg Plant POA (W/m²)</div>
            </div>
        `;
    }

    // Find the site-wide last acquisition index (the last point where ANY sensor has data)
    const history = data.sensor_history || {};
    const oraList = history.Ora || [];
    let lastAcqIdx = -1;
    
    Object.entries(history).forEach(([k, series]) => {
        if (k === "Ora" || k === "Timestamp Fetch") return;
        if (!Array.isArray(series)) return;
        for (let i = series.length - 1; i > lastAcqIdx; i--) {
            if (series[i] !== 0 && series[i] !== null) {
                lastAcqIdx = i;
                break;
            }
        }
    });

    Object.entries(sData).forEach(([key, val]) => {
        const keyUpper = key.toUpperCase();
        if (keyUpper === "TIMESTAMP FETCH" || keyUpper === "ORA") return;
        
        const box = document.createElement("div");
        let type = "other";
        let icon = "📊";
        let unit = "—";
        let label = key;

        if (keyUpper.includes("IRRAGGIAMENTO") || keyUpper.includes("POA") || keyUpper.includes("GHI")) {
            unit = "W/m²";
            type = keyUpper.includes("GHI") ? "ghi" : "poa";
            icon = keyUpper.includes("GHI") ? "🌍" : "☀️";
            label = keyUpper.includes("GHI") ? "Global Horiz. Irrad." : "Plane of Array Irradiance";
        } else if (keyUpper.includes("TEMP") || keyUpper.includes("°C") || (key.includes("JB") && (key.includes("IT") || key.includes("AL")))) {
            type = "temp"; 
            icon = "🌡️"; 
            unit = "°C"; 
            label = "Ambient/Module Temperature";
        }
        
        if (key.includes("-DOWN")) label = "Module Temp (Lower)";
        if (key.includes("-UP")) label = "Module Temp (Upper)";
        if (key.includes("AL-") && !keyUpper.includes("IRRAGGIAMENTO")) { 
            type = "al"; icon = "📏"; unit = "°C"; label = "Irradiance Sensor Temp"; 
        }

        box.className = `sensor-box ${type}`;
        const chartId = `sensor-chart-${key.replace(/[^a-z0-9]/gi, '_')}`;
        
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
            <div id="${chartId}" class="sensor-sparkline-container"></div>
        `;
        container.appendChild(box);

        // Render the sparkline if we have history
        if (history[key] && history[key].length > 1) {
            // Process series: cut off precisely at the last site-wide acquisition point
            const processedSeries = history[key].map((v, idx) => (idx > lastAcqIdx) ? null : v);

            setTimeout(() => {
                renderSensorSparkline(chartId, key, processedSeries, type);
            }, 0);
        }
    });
}

/**
 * Render a subtle background sparkline for a sensor box.
 */
function renderSensorSparkline(chartId, name, seriesData, type) {
    const container = document.getElementById(chartId);
    if (!container) return;

    let color = '#3b82f6'; // default blue
    if (type === 'poa') color = '#f59e0b'; // yellow/orange for sun
    if (type === 'ghi') color = '#60a5fa'; // bright blue for sky
    if (type === 'temp') color = '#f97316'; // orange/red for heat
    if (type === 'al') color = '#8b5cf6'; // purple for specific electronics
    
    const options = {
        series: [{
            name: name,
            data: seriesData
        }],
        chart: {
            type: 'area',
            height: 60,
            width: 140,
            sparkline: { enabled: true },
            animations: { enabled: true, easing: 'easeinout', speed: 1000 },
            toolbar: { show: false }
        },
        stroke: {
            curve: 'smooth',
            width: 1.8,
            colors: [color],
            lineCap: 'round'
        },
        fill: {
            type: 'gradient',
            gradient: {
                shadeIntensity: 1,
                opacityFrom: 0.15,
                opacityTo: 0.0,
                stops: [0, 90, 100]
            }
        },
        colors: [color],
        tooltip: {
            enabled: false // Disabled for pure background aesthetics
        },
        grid: {
            padding: {
                left: 0,
                right: 0,
                top: 0,
                bottom: 0
            }
        }
    };

    const chart = new ApexCharts(container, options);
    chart.render();
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
    const statusEl = el("global-last-update") || el("last-updated");
    if (statusEl) {
        statusEl.textContent = `Connected`;
        statusEl.style.color = "var(--green)";
        statusEl.style.opacity = "1";
    }
    reconnectInterval = 2000;
  };
  
  socket.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === "data_update") {
        const payload = msg;
        if (payload.data) {
          lastData = payload.data;
          updateDashboard(payload.data);
          
          // Update Link Status UI from nested data
          if (payload.data.link_status) {
            updateLinkStatusUI(payload.data.link_status);
          }
        }
        
        if (payload.trackers) {
          lastTrackerData = payload.trackers;
          updateTrackers(payload.trackers);
          if (lastData) updateIngestion(lastData);
        }

        // Global Last Update
        const lastUpdEl = el("global-last-update") || el("last-updated");
        if (lastUpdEl) {
          lastUpdEl.textContent = now();
        }
        
        // Update whichever detail tab is active
        renderActiveDetailTab();
        
      } else if (msg.type === "config_update") {
        applyConfig(msg.data);
      } else if (msg.type === "extraction_status") {
        updateExtractionUI(msg.is_extracting);
      }
    } catch (err) {
      console.warn("WS message parse error:", err);
    }
  };
  
  socket.onclose = () => {
    const statusEl = el("last-updated");
    statusEl.textContent = `Offline: Reconnecting...`;
    statusEl.style.color = "var(--yellow)";
    statusEl.style.opacity = "0.7";
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

  // Alert Preferences
  if (config.alert_preferences) {
    const ap = config.alert_preferences;
    if (el("pref-comm-db")) el("pref-comm-db").checked = !!ap.comm_lost?.dashboard;
    if (el("pref-comm-tg")) el("pref-comm-tg").checked = !!ap.comm_lost?.telegram;
    
    if (el("pref-site-db")) el("pref-site-db").checked = !!ap.plant_drop?.dashboard;
    if (el("pref-site-tg")) el("pref-site-tg").checked = !!ap.plant_drop?.telegram;

    if (el("pref-trip-db")) el("pref-trip-db").checked = !!ap.inverter_trip?.dashboard;
    if (el("pref-trip-tg")) el("pref-trip-tg").checked = !!ap.inverter_trip?.telegram;
    
    if (el("pref-ac-db")) el("pref-ac-db").checked = !!ap.ac_drop?.dashboard;
    if (el("pref-ac-tg")) el("pref-ac-tg").checked = !!ap.ac_drop?.telegram;

    if (el("pref-pr-low-db")) el("pref-pr-low-db").checked = !!ap.low_pr?.dashboard;
    if (el("pref-pr-low-tg")) el("pref-pr-low-tg").checked = !!ap.low_pr?.telegram;
    if (el("pref-pr-crit-db")) el("pref-pr-crit-db").checked = !!ap.crit_pr?.dashboard;
    if (el("pref-pr-crit-tg")) el("pref-pr-crit-tg").checked = !!ap.crit_pr?.telegram;
    
    if (el("pref-temp-warn-db")) el("pref-temp-warn-db").checked = !!ap.high_temp?.dashboard;
    if (el("pref-temp-warn-tg")) el("pref-temp-warn-tg").checked = !!ap.high_temp?.telegram;
    if (el("pref-temp-crit-db")) el("pref-temp-crit-db").checked = !!ap.crit_temp?.dashboard;
    if (el("pref-temp-crit-tg")) el("pref-temp-crit-tg").checked = !!ap.crit_temp?.telegram;
    
    if (el("pref-dc-warn-db")) el("pref-dc-warn-db").checked = !!ap.dc_warning?.dashboard;
    if (el("pref-dc-warn-tg")) el("pref-dc-warn-tg").checked = !!ap.dc_warning?.telegram;
    if (el("pref-dc-crit-db")) el("pref-dc-crit-db").checked = !!ap.dc_critical?.dashboard;
    if (el("pref-dc-crit-tg")) el("pref-dc-crit-tg").checked = !!ap.dc_critical?.telegram;

    if (el("pref-iso-db")) el("pref-iso-db").checked = !!ap.iso_fault?.dashboard;
    if (el("pref-iso-tg")) el("pref-iso-tg").checked = !!ap.iso_fault?.telegram;
    if (el("pref-grid-db")) el("pref-grid-db").checked = !!ap.grid_limit_change?.dashboard;
    if (el("pref-grid-tg")) el("pref-grid-tg").checked = !!ap.grid_limit_change?.telegram;

    if (el("pref-tracker-db")) el("pref-tracker-db").checked = !!ap.tracker_comm?.dashboard;
    if (el("pref-tracker-tg")) el("pref-tracker-tg").checked = !!ap.tracker_comm?.telegram;
    if (el("pref-mqtt-db")) el("pref-mqtt-db").checked = !!ap.mqtt_pulse?.dashboard;
    if (el("pref-mqtt-tg")) el("pref-mqtt-tg").checked = !!ap.mqtt_pulse?.telegram;

    if (el("pref-recovery-tg")) el("pref-recovery-tg").checked = !!ap.recovery?.telegram;
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

  const val = (id) => el(id) ? el(id).value : null;
  const num = (id, def) => el(id) ? (parseFloat(el(id).value) || def) : def;

  const newConfig = {
    thresholds: {
      pr: {
        green: num("cfg-pr-green", 85.0),
        yellow: num("cfg-pr-yellow", 75.0)
      },
      temp: {
        yellow: num("cfg-temp-yellow", 40.0),
        red: num("cfg-temp-red", 45.0)
      },
      ac: {
        green: num("cfg-ac-green", 5000),
        yellow: num("cfg-ac-yellow", 1000)
      },
      dc: {
        morning_green: num("cfg-dcm-green", 10.0),
        morning_yellow: num("cfg-dcm-yellow", 2.0),
        afternoon_green: num("cfg-dca-green", 5.0),
        afternoon_yellow: num("cfg-dca-yellow", 0.5)
      },
      min_downtime_minutes: num("cfg-min-downtime", 9)
    },
    colors: {
      green: val("cfg-color-green") || "#10b981",
      yellow: val("cfg-color-yellow") || "#f59e0b",
      red: val("cfg-color-red") || "#ef4444",
      grey: val("cfg-color-grey") || "#6b7280"
    },
    collection_interval: num("cfg-collection-interval", 15),
    telegram: {
      enabled: el("cfg-tg-enabled") ? el("cfg-tg-enabled").checked : false,
      bot_token: val("cfg-tg-token") || "",
      chat_id: val("cfg-tg-chat") || "",
      personal_id: val("cfg-tg-personal") || ""
    },
    alert_preferences: {
      comm_lost: { 
        dashboard: !!el("pref-comm-db")?.checked,
        telegram: !!el("pref-comm-tg")?.checked
      },
      plant_drop: {
        dashboard: !!el("pref-site-db")?.checked,
        telegram: !!el("pref-site-tg")?.checked
      },
      inverter_trip: {
        dashboard: !!el("pref-trip-db")?.checked,
        telegram: !!el("pref-trip-tg")?.checked
      },
      ac_drop: {
        dashboard: !!el("pref-ac-db")?.checked,
        telegram: !!el("pref-ac-tg")?.checked
      },
      low_pr: {
        dashboard: !!el("pref-pr-low-db")?.checked,
        telegram: !!el("pref-pr-low-tg")?.checked
      },
      crit_pr: {
        dashboard: !!el("pref-pr-crit-db")?.checked,
        telegram: !!el("pref-pr-crit-tg")?.checked
      },
      high_temp: {
        dashboard: !!el("pref-temp-warn-db")?.checked,
        telegram: !!el("pref-temp-warn-tg")?.checked
      },
      crit_temp: {
        dashboard: !!el("pref-temp-crit-db")?.checked,
        telegram: !!el("pref-temp-crit-tg")?.checked
      },
      dc_warning: {
        dashboard: !!el("pref-dc-warn-db")?.checked,
        telegram: !!el("pref-dc-warn-tg")?.checked
      },
      dc_critical: {
        dashboard: !!el("pref-dc-crit-db")?.checked,
        telegram: !!el("pref-dc-crit-tg")?.checked
      },
      iso_fault: {
        dashboard: !!el("pref-iso-db")?.checked,
        telegram: !!el("pref-iso-tg")?.checked
      },
      grid_limit_change: {
        dashboard: !!el("pref-grid-db")?.checked,
        telegram: !!el("pref-grid-tg")?.checked
      },
      tracker_comm: {
        dashboard: !!el("pref-tracker-db")?.checked,
        telegram: !!el("pref-tracker-tg")?.checked
      },
      mqtt_pulse: {
        dashboard: !!el("pref-mqtt-db")?.checked,
        telegram: !!el("pref-mqtt-tg")?.checked
      },
      recovery: {
        telegram: !!el("pref-recovery-tg")?.checked
      }
    }
  };

  try {
    // Add a controller to timeout the request if it hangs
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), 10000);

    const resp = await fetch("/api/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(newConfig),
      signal: controller.signal
    });
    
    clearTimeout(id);
    
    if (resp.ok) {
      const summary = `
✅ SETTINGS SAVED SUCCESSFULLY

Threshold Updated:
- PR Green: ${newConfig.thresholds.pr.green}%
- Temp Red: ${newConfig.thresholds.temp.red}°C
- Min Downtime: ${newConfig.thresholds.min_downtime_minutes} min
- Collection: ${newConfig.collection_interval} min

Telegram: ${newConfig.telegram.enabled ? "ENABLED" : "DISABLED"}
      `;
      alert(summary);
      el("settings-modal").classList.add("modal-hidden");
    } else {
      const err = await resp.json();
      alert("Failed to save: " + (err.message || "Unknown error"));
    }
  } catch (err) {
    if (err.name === 'AbortError') {
      alert("Error: Saving timed out. The server is taking too long to respond.");
    } else {
      alert("Error saving settings: " + err);
    }
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

// ─── Analytics Manager ───────────────────────────────────────────────────
class AnalyticsManager {
    constructor() {
        this.chart = null;
        this.metrics = [];
        this.inverters = [];
        this.availableDates = [];
        this.isInitialized = false;
    }

    async init() {
        if (this.isInitialized) return;
        
        try {
            const resp = await fetch("/api/analytics/config");
            const config = await resp.json();
            
            this.metrics = config.metrics;
            this.inverters = config.inverters;
            this.availableDates = config.available_dates;
            
            this.populateFilters();
            this.initChart();
            this.setupListeners();
            
            this.isInitialized = true;
            console.log("Analytics Manager Initialized");
        } catch (err) {
            console.error("Failed to init analytics:", err);
        }
    }

    populateFilters() {
        const metricSelect = el("ana-metric-select");
        metricSelect.innerHTML = this.metrics.map(m => `<option value="${m}">${m}</option>`).join("");
        
        const invList = el("ana-inv-list");
        invList.innerHTML = `
            <div class="inv-item">
                <input type="checkbox" id="ana-inv-all" checked>
                <label for="ana-inv-all">Select All Inverters</label>
            </div>
            ${this.inverters.map(inv => `
                <div class="inv-item">
                    <input type="checkbox" class="ana-inv-check" value="${inv}" checked>
                    <label>${inv}</label>
                </div>
            `).join("")}
        `;

        // Set default dates (last 3 days)
        if (this.availableDates.length > 0) {
            el("ana-end-date").value = this.availableDates[this.availableDates.length - 1];
            el("ana-start-date").value = this.availableDates[Math.max(0, this.availableDates.length - 3)];
        } else {
            const today = new Date().toISOString().split('T')[0];
            el("ana-end-date").value = today;
            el("ana-start-date").value = today;
        }
    }

    initChart() {
        const options = {
            series: [],
            chart: {
                type: 'line',
                height: 500,
                background: 'transparent',
                foreColor: 'var(--muted)',
                toolbar: { show: true },
                animations: { enabled: true }
            },
            stroke: { curve: 'smooth', width: 2 },
            colors: ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4'],
            xaxis: {
                type: 'datetime',
                labels: { datetimeUTC: false }
            },
            yaxis: {
                title: { text: 'Value' }
            },
            grid: { borderColor: 'var(--border)' },
            tooltip: { theme: 'dark', x: { format: 'dd MMM HH:mm' } },
            legend: { position: 'top', horizontalAlign: 'right' },
            noData: { text: 'Select filters and run analysis' }
        };

        this.chart = new ApexCharts(el("analytics-chart"), options);
        this.chart.render();
    }

    setupListeners() {
        el("ana-run-btn").addEventListener("click", () => this.fetchData());
        
        el("ana-inv-all").addEventListener("change", (e) => {
            const checks = document.querySelectorAll(".ana-inv-check");
            checks.forEach(c => c.checked = e.target.checked);
        });
    }

    async fetchData() {
        const btn = el("ana-run-btn");
        btn.disabled = true;
        btn.textContent = "Processing...";
        
        const metric = el("ana-metric-select").value;
        const start = el("ana-start-date").value;
        const end = el("ana-end-date").value;
        
        const selectedInverters = Array.from(document.querySelectorAll(".ana-inv-check:checked")).map(c => c.value);
        const invParam = el("ana-inv-all").checked ? "" : selectedInverters.join(",");
        
        try {
            const url = `/api/analytics/data?metric=${encodeURIComponent(metric)}&start=${start}&end=${end}&inverters=${invParam}`;
            const resp = await fetch(url);
            const data = await resp.json();
            
            if (data.error) {
                alert("Error: " + data.error);
                return;
            }

            this.updateChart(data);
            this.updateStats(data);
        } catch (err) {
            alert("Failed to fetch analytics data: " + err);
        } finally {
            btn.disabled = false;
            btn.textContent = "Generate Analysis";
        }
    }

    updateChart(data) {
        // Convert timestamps to JS Date objects or numbers
        const timestamps = data.timestamps.map(ts => new Date(ts).getTime());
        
        const series = data.series.map(s => ({
            name: s.name,
            data: s.data.map((val, i) => [timestamps[i], val])
        }));
        
        this.chart.updateSeries(series);
    }

    updateStats(data) {
        let allVals = [];
        data.series.forEach(s => allVals = allVals.concat(s.data));
        
        if (allVals.length === 0) {
            el("ana-stat-peak").textContent = "—";
            el("ana-stat-avg").textContent = "—";
            el("ana-stat-points").textContent = "0";
            return;
        }

        let peak = -Infinity;
        let sum = 0;
        let count = 0;
        
        for (let i = 0; i < allVals.length; i++) {
            const v = allVals[i];
            if (v !== null && v !== undefined && !isNaN(v)) {
                if (v > peak) peak = v;
                sum += v;
                count++;
            }
        }
        
        if (count === 0) {
            el("ana-stat-peak").textContent = "—";
            el("ana-stat-avg").textContent = "—";
            el("ana-stat-points").textContent = "0";
            return;
        }

        const avg = sum / count;
        
        el("ana-stat-peak").textContent = peak === -Infinity ? "—" : peak.toFixed(1);
        el("ana-stat-avg").textContent = avg.toFixed(1);
        el("ana-stat-points").textContent = count.toLocaleString();
    }
}

const analyticsManager = new AnalyticsManager();

document.addEventListener("DOMContentLoaded", () => {
  // Initialize tabs
  initTabs();
  initSortableHeaders();
  initAlertFilters();
  initTrackerFilters();

  // Initialize Analytics if tab is switched to
  document.querySelector('[data-tab="tab-analytics"]').addEventListener('click', () => {
      analyticsManager.init();
  });

  // Settings Modal Tabs
  const modalTabBtns = document.querySelectorAll(".m-tab-btn");
  const modalTabPanels = document.querySelectorAll(".m-tab-panel");
  modalTabBtns.forEach(btn => {
    btn.addEventListener("click", () => {
      const targetId = btn.dataset.mTab;
      modalTabBtns.forEach(b => b.classList.remove("active"));
      modalTabPanels.forEach(p => p.classList.remove("active"));
      btn.classList.add("active");
      if (el(targetId)) el(targetId).classList.add("active");
    });
  });
  
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
    filterEl.addEventListener("change", renderHistoryTiles);
  }

  // AI Chatbot logic
  const chatInput = el("chat-input");
  const chatSendBtn = el("chat-send-btn");
  const chatMessages = el("chat-messages");

  function appendChatMessage(text, sender) {
    if (!chatMessages) return;
    const msgDiv = document.createElement("div");
    msgDiv.className = `chat-message ${sender}`;
    if (sender === "user") msgDiv.setAttribute("data-avatar", "ME");
    else msgDiv.setAttribute("data-avatar", "AI");
    
    chatMessages.appendChild(msgDiv);
    
    const contentDiv = document.createElement("div");
    contentDiv.className = "msg-content";
    
    // Auto-formatting for AI responses
    if (sender.includes("bot") && (text.includes("```") || text.includes("**"))) {
        // Very basic markdown formatting (bullet points and bold)
        let formatted = text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/^\s*[-*]\s+(.*)/gm, '• $1')
            .replace(/```python([\s\S]*?)```/g, '<code>$1</code>');
        contentDiv.innerHTML = formatted;
    } else {
      contentDiv.textContent = text;
    }
    
    msgDiv.appendChild(contentDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
  }

  async function handleChatSend() {
    if (!chatInput || !chatSendBtn) return;
    const question = chatInput.value.trim();
    if (!question) return;

    chatInput.value = "";
    appendChatMessage(question, "user");

    const thinkingDiv = document.createElement("div");
    thinkingDiv.className = "chat-message bot thinking";
    thinkingDiv.setAttribute("data-avatar", "AI");
    thinkingDiv.innerHTML = '<div class="msg-content"><span class="spinner"></span> Thinking...</div>';
    chatMessages.appendChild(thinkingDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;

    try {
      chatSendBtn.disabled = true;
      chatSendBtn.style.opacity = "0.5";

      const resp = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question })
      });
      
      const result = await resp.json();
      if (thinkingDiv) thinkingDiv.remove();

      if (result.status === "success") {
        appendChatMessage(result.answer, "bot");
      } else {
        appendChatMessage("Error: " + result.message, "bot");
      }
    } catch (err) {
      if (thinkingDiv) thinkingDiv.remove();
      appendChatMessage("Error communicating with AI: " + err, "bot");
    } finally {
      if (chatSendBtn) {
        chatSendBtn.disabled = false;
        chatSendBtn.style.opacity = "1";
      }
    }
  }

  if (chatSendBtn) {
    chatSendBtn.addEventListener("click", handleChatSend);
  }
  
  if (chatInput) {
    chatInput.addEventListener("keypress", (e) => {
      if (e.key === "Enter") handleChatSend();
    });
  }
});

/**
 * Update the UI to reflect if the scraper is currently extracting data
 */
function updateExtractionUI(isExtracting) {
  const btn = el("btn-trigger-extraction");
  const icon = el("trigger-icon");
  const text = el("trigger-text");
  const statusLabel = el("extraction-status-label");

  if (!btn || !statusLabel) return;

  if (isExtracting) {
    btn.classList.add("extracting");
    btn.disabled = true;
    if (icon) icon.innerHTML = `<span class="spinner"></span>`;
    if (text) text.textContent = "Extracting data...";
    statusLabel.textContent = "BUSY";
    statusLabel.style.color = "var(--yellow)";
  } else {
    btn.classList.remove("extracting");
    btn.disabled = false;
    if (icon) icon.textContent = "🚀";
    if (text) text.textContent = "Get Data Now";
    statusLabel.textContent = "Idle";
    statusLabel.style.color = "var(--text)";
  }
}

/**
 * Manual trigger for VCOM data extraction
 */
async function triggerExtraction() {
  const btn = el("btn-trigger-extraction");
  if (!btn || btn.classList.contains("extracting")) return;

  try {
    // Optimistic UI update
    updateExtractionUI(true);
    
    const resp = await fetch("/api/extraction/trigger", { method: "POST" });
    const result = await resp.json();
    
    if (result.status === "error") {
      alert("Trigger failed: " + result.message);
      updateExtractionUI(false);
    } else {
      console.log("Extraction triggered successfully.");
    }
  } catch (err) {
    console.error("Failed to trigger extraction:", err);
    alert("Error communicating with scraper. Check if the dashboard is connected.");
    updateExtractionUI(false);
  }
}

/**
 * Rapid-fire suggestion handler for the AI Chat
 */
function sendSuggestion(text) {
  const input = el("chat-input");
  if (input) {
    input.value = text;
    input.focus();
  }
}

// ─── Tracker Field Rendering ─────────────────────────────────────────────

function updateTrackers(trackers) {
  const gridContainer = el("tracker-led-grid");
  if (!gridContainer) return;

  if (!trackers) trackers = [];

  // 1. Group data by Global Tracker No (1-370)
  const trackerMap = {};
  const getGlobalId = (t) => {
    // Robust parsing for string prefixes
    const extractNum = (str) => {
        if (!str) return null;
        const match = String(str).match(/(\d+)/);
        return match ? parseInt(match[1]) : null;
    };

    const tno = extractNum(t.tracker_no);
    if (tno) return tno;

    const tcu = extractNum(t.tcu_id);
    const ncuStr = String(t.ncu_id || "").replace("_", " "); // "NCU_01" -> "NCU 01"
    
    if (tcu) {
        if (ncuStr === "NCU 01") return tcu;
        if (ncuStr === "NCU 02") return 121 + tcu;
        if (ncuStr === "NCU 03") return 121 + 122 + tcu;
    }
    return null;
  };

  trackers.forEach(t => {
    const gid = getGlobalId(t);
    if (gid) trackerMap[gid] = t;
  });

  // 2. Stats
  const stats = {
    total: 370,
    connected: trackers.length,
    am: trackers.filter(t => t.mode === "AM").length,
    mm: trackers.filter(t => t.mode === "MM").length,
    wm: trackers.filter(t => t.mode === "WM").length,
    alarm: trackers.filter(t => t.alarm && t.alarm !== 'green' && t.alarm !== 'grey').length,
    ncu: {
      "NCU 01": { total: 121, connected: 0, am: 0, mm: 0, wm: 0 },
      "NCU 02": { total: 122, connected: 0, am: 0, mm: 0, wm: 0 },
      "NCU 03": { total: 127, connected: 0, am: 0, mm: 0, wm: 0 }
    }
  };

  trackers.forEach(t => {
    const ncuKey = String(t.ncu_id || "").replace("_", " ");
    if (stats.ncu[ncuKey]) {
      stats.ncu[ncuKey].connected++;
      if (t.mode === "AM") stats.ncu[ncuKey].am++;
      if (t.mode === "MM") stats.ncu[ncuKey].mm++;
      if (t.mode === "WM") stats.ncu[ncuKey].wm++;
    }
  });

  // 3. Update UI Elements
  if (el("stat-total-connected")) el("stat-total-connected").textContent = `${stats.connected} / ${stats.total}`;
  if (el("stat-total-auto"))      el("stat-total-auto").textContent = stats.am;
  if (el("stat-total-manual"))    el("stat-total-manual").textContent = stats.mm;
  if (el("stat-total-wind"))      el("stat-total-wind").textContent = stats.wm;
  if (el("stat-total-critical"))  el("stat-total-critical").textContent = stats.alarm;

  if (el("stat-last-sync") && trackers.length > 0) {
      const latest = [...trackers].sort((a,b) => (b.last_update || "").localeCompare(a.last_update || ""))[0];
      if (latest && latest.last_update) {
          el("stat-last-sync").textContent = latest.last_update.split("T")[1].substring(0, 5);
      }
  }

  ["NCU 01", "NCU 02", "NCU 03"].forEach((ncu, idx) => {
    const id = idx + 1;
    if (el(`stat-n${id}-count`)) el(`stat-n${id}-count`).textContent = stats.ncu[ncu].connected;
    if (el(`stat-n${id}-am`))    el(`stat-n${id}-am`).textContent = stats.ncu[ncu].am;
    if (el(`stat-n${id}-mm`))    el(`stat-n${id}-mm`).textContent = stats.ncu[ncu].mm;
    if (el(`stat-n${id}-wm`))    el(`stat-n${id}-wm`).textContent = stats.ncu[ncu].wm;
  });

  // Update Overview Card (Macro health)
  if (el("val-tracker-health")) el("val-tracker-health").textContent = `${stats.am} / ${stats.total}`;
  if (el("val-n1-am")) el("val-n1-am").textContent = stats.ncu["NCU 01"].am;
  if (el("val-n2-am")) el("val-n2-am").textContent = stats.ncu["NCU 02"].am;
  if (el("val-n3-am")) el("val-n3-am").textContent = stats.ncu["NCU 03"].am;
  if (el("val-tracker-crit")) el("val-tracker-crit").textContent = `${stats.alarm} Alarms`;

  if (el("val-tracker-last-sync") && trackers.length > 0) {
      const latest = [...trackers].sort((a,b) => (b.last_update || "").localeCompare(a.last_update || ""))[0];
      if (latest && latest.last_update) {
          el("val-tracker-last-sync").textContent = latest.last_update.split("T")[1].substring(0, 5);
      }
  }

  // 4. Render Grid
  let html = "";
  const ranges = [
    { label: "NCU 01 (1-121)", start: 1, end: 121, id: "NCU 01" },
    { label: "NCU 02 (122-243)", start: 122, end: 243, id: "NCU 02" },
    { label: "NCU 03 (244-370)", start: 244, end: 370, id: "NCU 03" }
  ];

  ranges.forEach(range => {
    if (currentNcuFilter !== "all" && currentNcuFilter !== range.id) return;

    html += `<div class="ncu-header">${range.label}</div>`;
    for (let i = range.start; i <= range.end; i++) {
      const t = trackerMap[i];
      let statusClass = "status-grey";
      let modeClass = "";
      
      if (t) {
        if (t.alarm && t.alarm !== 'green' && t.alarm !== 'grey') {
            statusClass = "status-red";
            modeClass = "mode-alarm";
        } else if (t.mode === "MM") {
            statusClass = "status-yellow";
            modeClass = "mode-mm";
        } else if (t.mode === "WM") {
            statusClass = "status-blue";
            modeClass = "mode-wm";
        } else if (t.mode === "AM") {
            statusClass = "status-green";
            modeClass = "mode-am";
        }
      }

      const tcu = t ? t.tcu_id : "—";
      const ncu = t ? t.ncu_id : "—";
      const mode = t ? t.mode : "OFFLINE";
      const target = t ? t.target_angle.toFixed(1) : "—";
      const actual = t ? t.actual_angle.toFixed(1) : "—";

      html += `
        <div class="tracker-card-detailed ${statusClass}" onclick="showTrackerDetail(${t ? JSON.stringify(t).replace(/"/g, '&quot;') : 'null'})">
          <div class="card-id-row">
            <span class="card-tracker-num">#${i}</span>
            <span class="card-tcu-label">TCU ${tcu}</span>
          </div>
          <div class="card-data-grid">
            <div class="data-item">
              <span class="data-label">Actual</span>
              <span class="data-val">${actual}°</span>
            </div>
            <div class="data-item">
              <span class="data-label">Target</span>
              <span class="data-val">${target}°</span>
            </div>
            <div class="data-item">
              <span class="data-label">NCU</span>
              <span class="data-val">${ncu}</span>
            </div>
            <div class="data-item">
              <span class="data-label">Status</span>
              <span class="data-val">${t ? 'ONLINE' : 'OFFLINE'}</span>
            </div>
          </div>
          <div class="card-mode-badge ${modeClass}">${mode} MODE</div>
        </div>`;
    }
  });

  gridContainer.innerHTML = html;

  // 5. Render Deviation LED Strips
  const devContainer = el("tracker-deviation-container");
  if (devContainer) {
    let devHtml = "";
    ranges.forEach(range => {
      devHtml += `
        <div class="deviation-ncu-block">
          <div class="deviation-label">
            <span>${range.id} Accuracy Health</span>
            <span style="opacity: 0.6">50 trackers per row</span>
          </div>
          <div class="deviation-led-strip">`;
      
      for (let i = range.start; i <= range.end; i++) {
        const t = trackerMap[i];
        let colorClass = "grey";
        let tip = `Tracker ${i}: No Data`;

        if (t) {
          const dev = Math.abs(t.actual_angle - t.target_angle);
          if (dev <= 5) colorClass = "green";
          else if (dev <= 10) colorClass = "yellow";
          else colorClass = "red";
          
          tip = `Tracker ${i}\nActual: ${t.actual_angle.toFixed(1)}°\nTarget: ${t.target_angle.toFixed(1)}°\nDev: ${dev.toFixed(1)}°`;
        }
        
        devHtml += `<div class="dev-led ${colorClass}" title="${tip}"></div>`;
      }
      
      devHtml += `</div></div>`;
    });
    devContainer.innerHTML = devHtml;
  }
}

// Re-bind filter buttons (since they might have changed or need to handle new container)
function initTrackerFilters() {
    document.querySelectorAll(".filter-btn").forEach(btn => {
        btn.onclick = () => {
          document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
          btn.classList.add("active");
          currentNcuFilter = btn.dataset.ncu;
          if (lastTrackerData) updateTrackers(lastTrackerData);
        };
    });
}

// ─── Link Status Monitoring ──────────────────────────────────────────────

function updateLinkStatusUI(linkInfo) {
  const badge = el("link-status-badge");
  if (!badge) return;

  const status = linkInfo.status || "offline";
  const site = linkInfo.site || "GATEWAY";

  badge.className = `link-status-pill ${status}`;
  
  if (status === "online") {
    badge.textContent = "LINK: ONLINE";
  } else if (status === "stale") {
    badge.textContent = "LINK: STALE";
  } else {
    badge.textContent = "LINK: OFFLINE";
  }
}
