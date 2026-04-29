/**
 * plant_map.js — 3-level interactive plant map visualization
 *
 * Features:
 *   - Level 1: SVG map with inverter circles, color-coded by health
 *   - Level 2: Modal showing all strings in clicked inverter
 *   - Tooltips with quick health info
 *   - Real-time health updates
 */

class PlantMap {
  constructor(svgSelector = "#plant-map-svg") {
    this.svgElement = document.querySelector(svgSelector);
    this.plantData = null;
    this.currentInverter = null;
    this.updateInterval = 30000; // Auto-refresh every 30s

    if (!this.svgElement) {
      console.warn(`SVG element not found: ${svgSelector}`);
      return;
    }

    this.init();
  }

  async init() {
    try {
      // Load plant layout (static topology)
      const layoutResp = await fetch("/api/plant/layout");
      const layout = await layoutResp.json();

      // Load inverter health (dynamic)
      const overviewResp = await fetch("/api/plant/overview");
      const overview = await overviewResp.json();

      this.plantData = { layout, overview };
      this.renderLevel1();
      this.setupAutoRefresh();
    } catch (err) {
      console.error("Failed to initialize plant map:", err);
    }
  }

  renderLevel1() {
    if (!this.plantData) return;

    const { layout, overview } = this.plantData;
    const plantWidth = layout.metadata.plant_width;
    const plantHeight = layout.metadata.plant_height;

    // Clear SVG
    this.svgElement.innerHTML = "";
    this.svgElement.setAttribute("viewBox", `0 0 ${plantWidth} ${plantHeight}`);

    // Draw background (sections)
    this.drawSections(layout.sections, plantWidth, plantHeight);

    // Draw inverters
    const inverterMap = {};
    overview.inverters.forEach((inv) => {
      inverterMap[inv.inverter_id] = inv;
    });

    layout.inverter_locations &&
      Object.entries(layout.inverter_locations).forEach(([invId, loc]) => {
        const health = inverterMap[invId] || { health_status: "unknown" };
        this.drawInverter(invId, loc.x, loc.y, health);
      });

    // Draw legend
    this.drawLegend(plantWidth, plantHeight);
  }

  drawSections(sections, width, height) {
    const sectionsGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
    sectionsGroup.id = "sections";

    sections.forEach((section) => {
      const { bounds } = section;
      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("x", bounds.x_min);
      rect.setAttribute("y", bounds.y_min);
      rect.setAttribute("width", bounds.x_max - bounds.x_min);
      rect.setAttribute("height", bounds.y_max - bounds.y_min);
      rect.setAttribute("fill", "rgba(200,200,200,0.05)");
      rect.setAttribute("stroke", "rgba(255,255,255,0.1)");
      rect.setAttribute("stroke-width", "1");

      // Section label
      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("x", bounds.x_min + 10);
      label.setAttribute("y", bounds.y_min + 20);
      label.setAttribute("font-size", "12");
      label.setAttribute("fill", "rgba(255,255,255,0.3)");
      label.textContent = `Section ${section.section_number}: ${section.name}`;

      sectionsGroup.appendChild(rect);
      sectionsGroup.appendChild(label);
    });

    this.svgElement.appendChild(sectionsGroup);
  }

  drawInverter(invId, x, y, health) {
    const statusColor = this.getStatusColor(health.health_status);
    const radius = 20;

    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.classList.add("inverter-group");
    group.setAttribute("data-inv-id", invId);

    // Main circle
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", x);
    circle.setAttribute("cy", y);
    circle.setAttribute("r", radius);
    circle.setAttribute("fill", statusColor);
    circle.setAttribute("stroke", "rgba(255,255,255,0.3)");
    circle.setAttribute("stroke-width", "2");
    circle.setAttribute("cursor", "pointer");
    circle.setAttribute("class", `inverter-circle status-${health.health_status}`);

    // Inverter label
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", x);
    label.setAttribute("y", y + 5);
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("font-size", "11");
    label.setAttribute("font-weight", "bold");
    label.setAttribute("fill", "white");
    label.setAttribute("pointer-events", "none");
    label.textContent = invId;

    group.appendChild(circle);
    group.appendChild(label);

    // Add tooltip
    circle.addEventListener("mouseenter", (e) => this.showTooltip(e, invId, health));
    circle.addEventListener("mouseleave", () => this.hideTooltip());

    // Click to show Level 2 (strings)
    circle.addEventListener("click", () => this.showLevel2(invId));

    this.svgElement.appendChild(group);
  }

  drawLegend(width, height) {
    const legendX = width - 150;
    const legendY = 20;

    const legend = document.createElementNS("http://www.w3.org/2000/svg", "g");
    legend.id = "legend";

    // Background
    const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    bg.setAttribute("x", legendX - 10);
    bg.setAttribute("y", legendY - 10);
    bg.setAttribute("width", "140");
    bg.setAttribute("height", "100");
    bg.setAttribute("fill", "rgba(0,0,0,0.6)");
    bg.setAttribute("stroke", "rgba(255,255,255,0.2)");
    bg.setAttribute("rx", "5");

    legend.appendChild(bg);

    // Title
    const title = document.createElementNS("http://www.w3.org/2000/svg", "text");
    title.setAttribute("x", legendX);
    title.setAttribute("y", legendY + 10);
    title.setAttribute("font-size", "11");
    title.setAttribute("font-weight", "bold");
    title.setAttribute("fill", "white");
    title.textContent = "Health Status";
    legend.appendChild(title);

    // Legend items
    const items = [
      { label: "Online", color: "#00ff00", y: 30 },
      { label: "Warning", color: "#ffff00", y: 50 },
      { label: "Critical", color: "#ff0000", y: 70 },
    ];

    items.forEach((item) => {
      const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      circle.setAttribute("cx", legendX + 5);
      circle.setAttribute("cy", legendY + item.y);
      circle.setAttribute("r", "3");
      circle.setAttribute("fill", item.color);
      legend.appendChild(circle);

      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("x", legendX + 15);
      label.setAttribute("y", legendY + item.y + 3);
      label.setAttribute("font-size", "10");
      label.setAttribute("fill", "white");
      label.textContent = item.label;
      legend.appendChild(label);
    });

    this.svgElement.appendChild(legend);
  }

  getStatusColor(status) {
    const colors = {
      green: "#00ff00",
      yellow: "#ffff00",
      red: "#ff0000",
      unknown: "#808080",
    };
    return colors[status] || colors.unknown;
  }

  showTooltip(event, invId, health) {
    const tooltip = document.getElementById("plant-map-tooltip") || this.createTooltip();

    tooltip.innerHTML = `
      <div style="font-weight: bold; margin-bottom: 4px;">${invId}</div>
      <div>Health: <b>${health.health_score}/100</b></div>
      <div>Section: ${health.section}</div>
      <div>Trackers: ${health.trackers} | Strings: ${health.strings}</div>
      <div style="margin-top: 6px; font-size: 0.85rem; color: #aaa;">Click for details</div>
    `;

    tooltip.style.display = "block";
    tooltip.style.left = event.clientX + 10 + "px";
    tooltip.style.top = event.clientY + 10 + "px";
  }

  hideTooltip() {
    const tooltip = document.getElementById("plant-map-tooltip");
    if (tooltip) tooltip.style.display = "none";
  }

  createTooltip() {
    const tooltip = document.createElement("div");
    tooltip.id = "plant-map-tooltip";
    tooltip.style.cssText = `
      position: fixed;
      background: rgba(0,0,0,0.9);
      border: 1px solid rgba(255,255,255,0.3);
      border-radius: 4px;
      padding: 8px 12px;
      font-size: 12px;
      color: white;
      pointer-events: none;
      z-index: 1000;
      display: none;
    `;
    document.body.appendChild(tooltip);
    return tooltip;
  }

  async showLevel2(invId) {
    this.currentInverter = invId;

    try {
      const resp = await fetch(`/api/plant/inverter/${invId}/strings`);
      const data = await resp.json();

      this.renderLevel2Modal(data);
    } catch (err) {
      console.error("Failed to load inverter strings:", err);
    }
  }

  renderLevel2Modal(data) {
    let modal = document.getElementById("strings-modal");
    if (!modal) {
      modal = this.createStringsModal();
    }

    const { inverter_id, section, num_strings, strings, summary } = data;

    modal.querySelector(".modal-title").textContent = `${inverter_id} - String Details (${section})`;

    // Summary stats
    const statsHtml = `
      <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 15px;">
        <div style="background: rgba(0,255,0,0.1); padding: 8px; border-radius: 4px; text-align: center;">
          <div style="font-size: 18px; font-weight: bold; color: #00ff00;">${summary.healthy}</div>
          <div style="font-size: 11px; color: #aaa;">Healthy</div>
        </div>
        <div style="background: rgba(255,255,0,0.1); padding: 8px; border-radius: 4px; text-align: center;">
          <div style="font-size: 18px; font-weight: bold; color: #ffff00;">${summary.warning}</div>
          <div style="font-size: 11px; color: #aaa;">Warning</div>
        </div>
        <div style="background: rgba(255,0,0,0.1); padding: 8px; border-radius: 4px; text-align: center;">
          <div style="font-size: 18px; font-weight: bold; color: #ff0000;">${summary.critical}</div>
          <div style="font-size: 11px; color: #aaa;">Critical</div>
        </div>
      </div>
    `;

    // String grid
    let stringsHtml = statsHtml + '<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 8px;">';

    strings.forEach((str) => {
      const bgColor = this.getStatusBg(str.health_status);
      stringsHtml += `
        <div style="${bgColor} padding: 8px; border-radius: 4px; border-left: 3px solid ${this.getStatusColor(str.health_status)};">
          <div style="font-weight: bold; font-size: 12px;">${str.string_id}</div>
          <div style="font-size: 10px; color: #ccc;">Tracker: ${str.tracker_id}</div>
          <div style="font-size: 10px; color: #ccc;">MPPT: ${str.mppt}</div>
          <div style="margin-top: 4px; font-size: 11px;">Score: <b>${str.health_score}/100</b></div>
        </div>
      `;
    });

    stringsHtml += "</div>";

    modal.querySelector(".modal-content").innerHTML = stringsHtml;
    modal.style.display = "block";
  }

  getStatusBg(status) {
    const bgs = {
      green: "background: rgba(0,255,0,0.05);",
      yellow: "background: rgba(255,255,0,0.05);",
      red: "background: rgba(255,0,0,0.05);",
    };
    return bgs[status] || "background: rgba(128,128,128,0.05);";
  }

  createStringsModal() {
    const modal = document.createElement("div");
    modal.id = "strings-modal";
    modal.innerHTML = `
      <div style="position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); z-index: 999; display: flex; align-items: center; justify-content: center;">
        <div style="background: #1a1a1a; border: 1px solid rgba(255,255,255,0.2); border-radius: 8px; padding: 20px; max-width: 900px; max-height: 80vh; overflow-y: auto; width: 90%;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
            <div class="modal-title" style="font-size: 18px; font-weight: bold; color: white;"></div>
            <button onclick="document.getElementById('strings-modal').style.display='none'" style="background: none; border: none; color: white; font-size: 20px; cursor: pointer;">×</button>
          </div>
          <div class="modal-content" style="color: white; font-family: monospace;"></div>
        </div>
      </div>
    `;
    document.body.appendChild(modal);

    // Close on background click
    modal.addEventListener("click", (e) => {
      if (e.target === modal.querySelector("div:first-child")) {
        modal.style.display = "none";
      }
    });

    return modal;
  }

  setupAutoRefresh() {
    setInterval(() => {
      this.init(); // Reload plant data every 30s
    }, this.updateInterval);
  }
}

// Initialize on page load
document.addEventListener("DOMContentLoaded", () => {
  window.plantMap = new PlantMap("#plant-map-svg");
});
