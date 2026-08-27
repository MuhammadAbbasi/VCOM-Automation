/*
 * plant_map_surveyed.js - Mappa Impianto, surveyed layout.
 *
 * Draws the plant as built: 370 trackers at their surveyed positions, split into
 * their 808 strings, over the internal roads, the pond and the 3 substations,
 * with the 36 inverter stations in place.
 *
 * Colours are the dashboard's own severity scheme (green / yellow / red / grey)
 * and come from /api/plant/surveyed/state, which routes the watchdog's existing
 * anomalies, inverter health and tracker alarms onto the element they belong to.
 * No alert rule is defined here.
 */
(function () {
  "use strict";

  var API = "/api/plant/surveyed";
  var REFRESH_MS = 60000;
  var SEV = ["red", "yellow", "grey", "green"];
  var SEV_LABEL = { red: "Critico", yellow: "Attenzione", grey: "Dati assenti", green: "Regolare" };
  var RANK = { green: 0, grey: 1, yellow: 2, red: 3 };

  var NS = "http://www.w3.org/2000/svg";
  function sv(tag, attrs) {
    var n = document.createElementNS(NS, tag);
    for (var k in attrs) n.setAttribute(k, attrs[k]);
    return n;
  }
  function el(tag, cls, txt) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (txt !== undefined && txt !== null) n.textContent = txt;
    return n;
  }

  function PlantMapSurveyed(root) {
    this.root = root;
    this.layout = null;
    this.state = null;
    this.filter = null;       // legend filter: severity or problem key
    this.sel = null;          // {kind, id}
    this.k = 1; this.ox = 0; this.oy = 0; this.fit = 1;
    this.build();
  }

  PlantMapSurveyed.prototype.build = function () {
    var self = this;
    this.root.innerHTML = "";
    this.root.classList.add("svm");

    var bar = el("div", "svm-bar");
    this.counts = el("div", "svm-counts");
    var tools = el("div", "svm-tools");
    this.search = el("input", "svm-search");
    this.search.type = "search";
    this.search.placeholder = "Cerca TRACKER 198, STR21, MPPT05, TX2-INV08…";
    this.search.addEventListener("input", function () { self.runSearch(); });
    var fitBtn = el("button", "svm-btn", "Adatta");
    fitBtn.onclick = function () { self.fitAll(); self.select(null); };
    var zin = el("button", "svm-btn", "+");
    zin.onclick = function () { self.zoomTo(self.k * 1.6); };
    var zout = el("button", "svm-btn", "−");
    zout.onclick = function () { self.zoomTo(self.k / 1.6); };
    tools.appendChild(this.search); tools.appendChild(zin);
    tools.appendChild(zout); tools.appendChild(fitBtn);
    bar.appendChild(this.counts); bar.appendChild(tools);

    var grid = el("div", "svm-grid");
    var left = el("div", "svm-mapwrap");
    this.svg = sv("svg", { class: "svm-svg", role: "img",
      "aria-label": "Mappa impianto, nord in alto" });
    left.appendChild(this.svg);
    this.results = el("div", "svm-results");
    left.appendChild(this.results);
    this.legend = el("div", "svm-legend");
    left.appendChild(this.legend);

    var right = el("aside", "svm-side");
    this.detail = el("div", "svm-detail");
    this.problems = el("div", "svm-problems");
    right.appendChild(this.detail); right.appendChild(this.problems);

    grid.appendChild(left); grid.appendChild(right);
    this.tip = el("div", "svm-tip");
    this.root.appendChild(bar); this.root.appendChild(grid); this.root.appendChild(this.tip);

    this.svg.addEventListener("pointerdown", function (e) { self.onDown(e); });
    this.svg.addEventListener("pointermove", function (e) { self.onMove(e); });
    this.svg.addEventListener("pointerup", function (e) { self.onUp(e); });
    this.svg.addEventListener("pointerleave", function () { self.tip.classList.remove("on"); });
    this.svg.addEventListener("wheel", function (e) {
      e.preventDefault();
      var r = self.svg.getBoundingClientRect();
      self.zoomTo(self.k * Math.pow(1.0016, -e.deltaY), e.clientX - r.left, e.clientY - r.top);
    }, { passive: false });
    window.addEventListener("resize", function () { self.measure(); self.apply(); });

    this.load();
    setInterval(function () { self.refresh(); }, REFRESH_MS);
  };

  PlantMapSurveyed.prototype.load = function () {
    var self = this;
    fetch(API + "/layout", { credentials: "same-origin" })
      .then(function (r) { if (!r.ok) throw new Error("layout " + r.status); return r.json(); })
      .then(function (j) { self.layout = j; self.draw(); return self.refresh(); })
      .catch(function (e) {
        self.root.innerHTML = '<div class="svm-err">Mappa non disponibile: ' + e.message + "</div>";
      });
  };

  PlantMapSurveyed.prototype.refresh = function () {
    var self = this;
    return fetch(API + "/state", { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { if (j && !j.error) { self.state = j; self.paint(); } });
  };

  /* ---------------------------------------------------------------- draw */
  PlantMapSurveyed.prototype.draw = function () {
    var L = this.layout, self = this;
    var PAD = 18;
    var xs = [], ys = [];
    L.trackers.forEach(function (t) {
      xs.push(t.x - t.w / 2); xs.push(t.x + t.w / 2); ys.push(t.y0); ys.push(t.y1);
    });
    (L.transformers || []).forEach(function (o) {
      o.p.forEach(function (p) { xs.push(p[0]); ys.push(p[1]); });
    });
    (L.pond || []).forEach(function (o) {
      o.p.forEach(function (p) { xs.push(p[0]); ys.push(p[1]); });
    });
    this.b = { x0: Math.min.apply(null, xs) - PAD, x1: Math.max.apply(null, xs) + PAD,
               y0: Math.min.apply(null, ys) - PAD, y1: Math.max.apply(null, ys) + PAD };
    this.VW = this.b.x1 - this.b.x0; this.VH = this.b.y1 - this.b.y0;
    this.svg.setAttribute("viewBox", "0 0 " + this.VW + " " + this.VH);
    this.svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    this.svg.innerHTML = "";

    this.scene = sv("g", {});
    var gSite = sv("g", {}), gTrk = sv("g", {}), gStr = sv("g", {}),
        gDev = sv("g", {}), gSel = sv("g", {}), gLab = sv("g", {});
    this.scene.appendChild(gSite); this.scene.appendChild(gTrk);
    this.scene.appendChild(gStr); this.scene.appendChild(gDev);
    this.scene.appendChild(gSel); this.scene.appendChild(gLab);
    this.svg.appendChild(this.scene);

    var px = function (x) { return x - self.b.x0; };
    var py = function (y) { return self.b.y1 - y; };
    this.px = px; this.py = py;
    var d = function (pts, close) {
      return pts.map(function (p, i) { return (i ? "L" : "M") + px(p[0]) + " " + py(p[1]); })
        .join(" ") + (close ? " Z" : "");
    };

    (L.pond || []).forEach(function (o) {
      gSite.appendChild(sv("path", { class: "svm-pond", d: d(o.p, true) }));
    });
    (L.roads || []).forEach(function (o) {
      gSite.appendChild(sv("path", { class: "svm-road " + (o.k || "").split(" ")[0],
        d: d(o.p, o.c) }));
    });

    // one rect per string, butted together so a tracker reads as one structure
    this.rects = {};
    this.trackerRects = {};
    L.trackers.forEach(function (t) {
      var n = t.strings.length || 1, span = (t.y0 - t.y1) / n, cx = px(t.x), own = [];
      t.strings.forEach(function (sid, i) {
        var top = py(t.y0) + i * span;
        var r = sv("rect", { class: "svm-str", x: cx - t.w / 2, y: top, width: t.w, height: span });
        var hit = sv("rect", { class: "svm-hit", x: cx - 2.6, y: top, width: 5.2, height: span });
        hit.dataset.str = sid; hit.dataset.trk = t.id;
        hit.dataset.mppt = t.mppts[i] || "";
        gTrk.appendChild(r); gTrk.appendChild(hit);
        self.rects[sid] = r; own.push(r);
      });
      for (var i = 1; i < n; i++) {
        gStr.appendChild(sv("line", { class: "svm-div", x1: cx - t.w / 2, x2: cx + t.w / 2,
          y1: py(t.y0) + i * span, y2: py(t.y0) + i * span }));
      }
      self.trackerRects[t.id] = own;
    });

    this.txLabels = [];
    (L.transformers || []).forEach(function (o) {
      gDev.appendChild(sv("path", { class: "svm-tx", d: d(o.p, true) }));
      var hit = sv("path", { class: "svm-hit", d: d(o.p, true) });
      hit.dataset.tx = o.id; gDev.appendChild(hit);
      var lab = sv("text", { class: "svm-txlab", "text-anchor": "middle" });
      lab.textContent = o.id;
      lab.dataset.x = px(o.cx); lab.dataset.y = py(o.cy) - 9;
      gLab.appendChild(lab); self.txLabels.push(lab);
    });

    this.invShapes = [];
    (L.inverters || []).forEach(function (o) {
      var cx = px(o.x) + o.w / 2, cy = py(o.y) - o.d / 2;
      var shape = sv("rect", { class: "svm-inv", rx: 0.12 });
      var hit = sv("rect", { class: "svm-hit" });
      hit.dataset.inv = o.id;
      gDev.appendChild(shape); gDev.appendChild(hit);
      self.invShapes.push({ shape: shape, hit: hit, cx: cx, cy: cy, w: o.w, d: o.d, id: o.id });
    });

    this.ring = sv("rect", { class: "svm-ring", style: "display:none" });
    gSel.appendChild(this.ring);

    this.measure(); this.fitAll();
  };

  /* ---------------------------------------------------------------- paint */
  PlantMapSurveyed.prototype.statusOf = function (sid) {
    var st = this.state;
    if (!st) return "grey";
    var s = st.strings && st.strings[sid];
    return (s && s.status) || "grey";
  };

  PlantMapSurveyed.prototype.paint = function () {
    var self = this, L = this.layout, st = this.state;
    if (!L || !st) return;

    var keep = this.filterSet();
    L.trackers.forEach(function (t) {
      t.strings.forEach(function (sid) {
        var r = self.rects[sid];
        if (!r) return;
        var s = self.statusOf(sid);
        r.setAttribute("class", "svm-str s-" + s);
        r.classList.toggle("dim", !!keep && !keep.has(sid));
      });
    });
    this.invShapes.forEach(function (o) {
      var s = (st.inverters[o.id] && st.inverters[o.id].status) || "grey";
      o.shape.setAttribute("class", "svm-inv s-" + s);
    });

    var c = st.counts || {};
    this.counts.innerHTML = "";
    var order = [["strings", "Stringhe"], ["mppts", "MPPT"],
                 ["trackers", "Tracker"], ["inverters", "Inverter"]];
    order.forEach(function (pair) {
      var g = c[pair[0]] || {};
      var box = el("div", "svm-count");
      box.appendChild(el("span", "svm-count-label", pair[1]));
      var row = el("div", "svm-count-row");
      SEV.forEach(function (s) {
        if (!g[s]) return;
        var chip = el("span", "svm-chip s-" + s, g[s]);
        chip.title = SEV_LABEL[s];
        chip.onclick = function () { self.setFilter(self.filter === s ? null : s); };
        row.appendChild(chip);
      });
      box.appendChild(row);
      self.counts.appendChild(box);
    });
    if (!st.has_snapshot) {
      var w = el("div", "svm-count svm-nodata", "Nessuno snapshot per " + st.date);
      this.counts.appendChild(w);
    }

    this.drawLegend();
    this.drawProblems();
    this.renderDetail();
    this.apply();
  };

  PlantMapSurveyed.prototype.filterSet = function () {
    var st = this.state, L = this.layout;
    if (!this.filter || !st) return null;
    var keep = new Set();
    var self = this;
    if (RANK[this.filter] !== undefined) {
      L.strings.forEach(function (s) {
        if (self.statusOf(s.id) === self.filter) keep.add(s.id);
      });
      return keep;
    }
    (st.problems || []).forEach(function (p) {
      if (p.key !== self.filter) return;
      (p.mppts || []).forEach(function (m) {
        L.strings.forEach(function (s) { if (s.mppt === m) keep.add(s.id); });
      });
      (p.inverters || []).forEach(function (i) {
        L.strings.forEach(function (s) { if (s.inverter === i) keep.add(s.id); });
      });
      (p.trackers || []).forEach(function (t) {
        L.strings.forEach(function (s) { if (s.tracker === t) keep.add(s.id); });
      });
    });
    return keep;
  };

  PlantMapSurveyed.prototype.setFilter = function (f) {
    this.filter = f;
    this.paint();
  };

  PlantMapSurveyed.prototype.drawLegend = function () {
    var self = this, st = this.state;
    this.legend.innerHTML = "";
    var head = el("div", "svm-legend-head", "Legenda — clicca per filtrare");
    this.legend.appendChild(head);
    var ul = el("div", "svm-legend-list");
    SEV.forEach(function (s) {
      var n = ((st.counts || {}).strings || {})[s] || 0;
      var b = el("button", "svm-legend-item" + (self.filter === s ? " on" : ""));
      b.appendChild(el("span", "svm-sw s-" + s));
      b.appendChild(el("span", "svm-legend-label", SEV_LABEL[s]));
      b.appendChild(el("span", "svm-legend-n", n));
      b.onclick = function () { self.setFilter(self.filter === s ? null : s); };
      ul.appendChild(b);
    });
    (st.legend || []).forEach(function (e) {
      var b = el("button", "svm-legend-item" + (self.filter === e.key ? " on" : ""));
      b.appendChild(el("span", "svm-sw s-" + e.severity));
      b.appendChild(el("span", "svm-legend-label", e.label));
      b.appendChild(el("span", "svm-legend-n", e.count));
      b.onclick = function () { self.setFilter(self.filter === e.key ? null : e.key); };
      ul.appendChild(b);
    });
    if (!(st.legend || []).length) {
      ul.appendChild(el("div", "svm-legend-none", "Nessuna anomalia attiva"));
    }
    this.legend.appendChild(ul);
  };

  PlantMapSurveyed.prototype.drawProblems = function () {
    var self = this, st = this.state;
    this.problems.innerHTML = "";
    var h = el("div", "svm-side-head");
    h.appendChild(el("span", null, "Problemi attivi"));
    h.appendChild(el("span", "svm-side-n", String((st.problems || []).length)));
    this.problems.appendChild(h);
    if (!(st.problems || []).length) {
      this.problems.appendChild(el("div", "svm-none",
        "Nessun problema attivo. Tutti gli elementi seguono lo stato pubblicato dal watchdog."));
      return;
    }
    var list = el("div", "svm-plist");
    st.problems.forEach(function (p) {
      var b = el("button", "svm-prob");
      var top = el("div", "svm-prob-top");
      top.appendChild(el("span", "svm-sw s-" + p.severity));
      top.appendChild(el("span", "svm-prob-type", p.type || p.key));
      top.appendChild(el("span", "svm-prob-el", p.element || ""));
      b.appendChild(top);
      if (p.message) b.appendChild(el("div", "svm-prob-msg", p.message));
      b.onclick = function () { self.gotoProblem(p); };
      list.appendChild(b);
    });
    this.problems.appendChild(list);
  };

  PlantMapSurveyed.prototype.gotoProblem = function (p) {
    if (p.mppts && p.mppts.length) return this.select({ kind: "mppt", id: p.mppts[0] });
    if (p.trackers && p.trackers.length) return this.select({ kind: "tracker", id: p.trackers[0] });
    if (p.inverters && p.inverters.length) return this.select({ kind: "inverter", id: p.inverters[0] });
    this.fitAll();
  };

  /* ---------------------------------------------------------------- detail */
  PlantMapSurveyed.prototype.stringsFor = function (sel) {
    var L = this.layout;
    if (!sel) return [];
    if (sel.kind === "string") return L.strings.filter(function (s) { return s.id === sel.id; });
    if (sel.kind === "mppt") return L.strings.filter(function (s) { return s.mppt === sel.id; });
    if (sel.kind === "tracker") return L.strings.filter(function (s) { return s.tracker === sel.id; });
    if (sel.kind === "inverter") return L.strings.filter(function (s) { return s.inverter === sel.id; });
    if (sel.kind === "tx") return L.strings.filter(function (s) { return s.tx === sel.id; });
    return [];
  };

  PlantMapSurveyed.prototype.select = function (sel, zoom) {
    this.sel = sel;
    this.renderDetail();
    this.ringFor(sel);
    if (sel && zoom !== false) this.zoomToSel(sel);
  };

  PlantMapSurveyed.prototype.ringFor = function (sel) {
    var ss = this.stringsFor(sel), self = this;
    if (!ss.length) { this.ring.style.display = "none"; return; }
    var x0 = 1e18, y0 = 1e18, x1 = -1e18, y1 = -1e18;
    ss.forEach(function (s) {
      var r = self.rects[s.id];
      if (!r) return;
      var x = +r.getAttribute("x"), y = +r.getAttribute("y");
      var w = +r.getAttribute("width"), h = +r.getAttribute("height");
      x0 = Math.min(x0, x); y0 = Math.min(y0, y);
      x1 = Math.max(x1, x + w); y1 = Math.max(y1, y + h);
    });
    this.ring.setAttribute("x", x0 - 1.6); this.ring.setAttribute("y", y0 - 1.6);
    this.ring.setAttribute("width", x1 - x0 + 3.2);
    this.ring.setAttribute("height", y1 - y0 + 3.2);
    this.ring.style.display = "";
  };

  PlantMapSurveyed.prototype.renderDetail = function () {
    var st = this.state, L = this.layout, self = this;
    this.detail.innerHTML = "";
    if (!this.sel) {
      var m = (L && L.metadata) || {};
      var h0 = el("div", "svm-side-head");
      h0.appendChild(el("span", null, "Impianto"));
      this.detail.appendChild(h0);
      var g0 = el("div", "svm-kv");
      [["Tracker", m.trackers], ["Stringhe", m.strings], ["MPPT", m.mppts],
       ["Inverter", m.inverters], ["Moduli", m.modules], ["Cabine", m.transformers]]
        .forEach(function (p) {
          g0.appendChild(el("span", "svm-k", p[0]));
          g0.appendChild(el("span", "svm-v", p[1] != null ? p[1] : "-"));
        });
      this.detail.appendChild(g0);
      this.detail.appendChild(el("div", "svm-hint",
        "Clicca una stringa sulla mappa, o una voce nella lista dei problemi."));
      return;
    }
    var sel = this.sel;
    var head = el("div", "svm-side-head");
    head.appendChild(el("span", null, sel.id));
    var up = el("button", "svm-btn svm-up", "Impianto");
    up.onclick = function () { self.select(null); self.fitAll(); };
    head.appendChild(up);
    this.detail.appendChild(head);

    var kv = el("div", "svm-kv");
    function add(k, v) {
      if (v === undefined || v === null || v === "") return;
      kv.appendChild(el("span", "svm-k", k));
      kv.appendChild(el("span", "svm-v", v));
    }
    var ss = this.stringsFor(sel);
    var s0 = ss[0];
    if (s0) {
      add("Inverter", s0.inverter); add("TX", s0.tx); add("Area", s0.area);
      add("Tracker", s0.tracker); add("TCU", s0.tcu); add("NCU", s0.ncu);
      if (sel.kind !== "string") add("Stringhe", ss.length);
    }
    if (sel.kind === "string") add("MPPT", s0 && s0.mppt);
    if (sel.kind === "mppt" && st.mppts[sel.id]) {
      var mm = st.mppts[sel.id];
      add("Stato", SEV_LABEL[mm.status] || mm.status);
      add("Corrente", mm.v != null ? mm.v + " A" : null);
      add("Attesa", mm.exp != null ? mm.exp + " A" : null);
    }
    if (sel.kind === "tracker" && st.trackers[sel.id]) {
      var tt = st.trackers[sel.id];
      add("Stato", SEV_LABEL[tt.status] || tt.status);
      add("Angolo target", tt.target_angle);
      add("Angolo attuale", tt.actual_angle);
      add("Scarto", tt.deviation != null ? tt.deviation + " deg" : null);
      add("Modo", tt.mode); add("Allarme", tt.alarm);
      add("Nota", tt.reason);
    }
    if (sel.kind === "inverter" && st.inverters[sel.id]) {
      var ii = st.inverters[sel.id];
      add("Stato", SEV_LABEL[ii.status] || ii.status);
      add("PR", ii.pr_v != null ? ii.pr_v + " %" : null);
      add("Temperatura", ii.temp_v != null ? ii.temp_v + " °C" : null);
      add("Corrente DC", ii.dc_v != null ? Math.round(ii.dc_v * 100) / 100 + " A" : null);
      add("Potenza AC", ii.ac_v != null ? ii.ac_v + " W" : null);
      add("Isolamento", ii.iso_v);
      add("Comunicazione", ii.comms_lost ? "persa" : "ok");
      add("Dato", ii.data_time);
    }
    var trk = L.trackers.filter(function (t) { return t.id === (s0 && s0.tracker); })[0];
    if (trk && (sel.kind === "string" || sel.kind === "tracker")) {
      add("Moduli", trk.modules); add("Pali", trk.piles);
      add("Lunghezza", trk.len + " m"); add("Quota", trk.alt + " m");
      add("Coordinate", trk.lat + ", " + trk.lon);
    }
    this.detail.appendChild(kv);

    if (sel.kind !== "string" && ss.length > 1) {
      var lst = el("div", "svm-sub");
      lst.appendChild(el("div", "svm-sub-head", "Stringhe"));
      ss.forEach(function (s) {
        var b = el("button", "svm-subrow");
        b.appendChild(el("span", "svm-sw s-" + self.statusOf(s.id)));
        b.appendChild(el("span", null, s.id));
        b.appendChild(el("span", "svm-subrow-m", s.mppt.slice(-6)));
        b.onclick = function () { self.select({ kind: "string", id: s.id }); };
        lst.appendChild(b);
      });
      this.detail.appendChild(lst);
    }
  };

  /* ---------------------------------------------------------------- search */
  PlantMapSurveyed.prototype.runSearch = function () {
    var q = this.search.value.trim().toUpperCase(), self = this, L = this.layout;
    this.results.innerHTML = "";
    if (q.length < 2 || !L) { this.results.classList.remove("on"); return; }
    var hits = [];
    function push(kind, id) { if (id.toUpperCase().indexOf(q) >= 0) hits.push({ kind: kind, id: id }); }
    L.trackers.forEach(function (t) { push("tracker", t.id); });
    L.strings.forEach(function (s) { push("string", s.id); });
    L.mppts.forEach(function (m) { push("mppt", m.id); });
    (L.inverters || []).forEach(function (i) { push("inverter", i.id); });
    hits = hits.slice(0, 40);
    if (!hits.length) { this.results.classList.remove("on"); return; }
    hits.forEach(function (h) {
      var b = el("button", "svm-res");
      b.appendChild(el("span", null, h.id));
      b.appendChild(el("span", "svm-res-kind", h.kind));
      b.onclick = function () {
        self.select(h); self.results.classList.remove("on"); self.search.blur();
      };
      self.results.appendChild(b);
    });
    this.results.classList.add("on");
  };

  /* ---------------------------------------------------------------- view */
  PlantMapSurveyed.prototype.measure = function () {
    var r = this.svg.getBoundingClientRect();
    this.fit = Math.min(r.width / this.VW, r.height / this.VH) || 1;
  };
  PlantMapSurveyed.prototype.apply = function () {
    if (!this.scene) return;
    this.scene.setAttribute("transform",
      "translate(" + this.ox + " " + this.oy + ") scale(" + this.k + ")");
    var s = 1 / (this.fit * this.k);
    (this.txLabels || []).forEach(function (l) {
      l.setAttribute("transform",
        "translate(" + l.dataset.x + " " + l.dataset.y + ") scale(" + s + ")");
    });
    var pxPerM = this.fit * this.k;
    (this.invShapes || []).forEach(function (o) {
      var w = Math.max(o.w, 7 / pxPerM), d = Math.max(o.d, 3.5 / pxPerM), pad = 3 / pxPerM;
      o.shape.setAttribute("x", o.cx - w / 2); o.shape.setAttribute("y", o.cy - d / 2);
      o.shape.setAttribute("width", w); o.shape.setAttribute("height", d);
      o.hit.setAttribute("x", o.cx - w / 2 - pad); o.hit.setAttribute("y", o.cy - d / 2 - pad);
      o.hit.setAttribute("width", w + pad * 2); o.hit.setAttribute("height", d + pad * 2);
    });
  };
  PlantMapSurveyed.prototype.clamp = function () {
    var lx = this.VW * (this.k - 1), ly = this.VH * (this.k - 1);
    this.ox = Math.min(0, Math.max(-lx, this.ox));
    this.oy = Math.min(0, Math.max(-ly, this.oy));
  };
  PlantMapSurveyed.prototype.fitAll = function () {
    this.k = 1; this.ox = 0; this.oy = 0; this.apply();
  };
  PlantMapSurveyed.prototype.zoomTo = function (nk, cx, cy) {
    nk = Math.max(1, Math.min(60, nk));
    var r = this.svg.getBoundingClientRect();
    cx = cx == null ? r.width / 2 : cx; cy = cy == null ? r.height / 2 : cy;
    var gx = (cx - this.ox * this.fit - (r.width - this.VW * this.fit) / 2) / (this.fit * this.k);
    var gy = (cy - this.oy * this.fit - (r.height - this.VH * this.fit) / 2) / (this.fit * this.k);
    this.ox += gx * (this.k - nk); this.oy += gy * (this.k - nk);
    this.k = nk; this.clamp(); this.apply();
  };
  PlantMapSurveyed.prototype.zoomToSel = function (sel) {
    var ss = this.stringsFor(sel), self = this;
    if (!ss.length) return;
    var x0 = 1e18, y0 = 1e18, x1 = -1e18, y1 = -1e18;
    ss.forEach(function (s) {
      var r = self.rects[s.id];
      if (!r) return;
      var x = +r.getAttribute("x"), y = +r.getAttribute("y");
      x0 = Math.min(x0, x); y0 = Math.min(y0, y);
      x1 = Math.max(x1, x + (+r.getAttribute("width")));
      y1 = Math.max(y1, y + (+r.getAttribute("height")));
    });
    var m = sel.kind === "inverter" || sel.kind === "tx" ? 26 : 16;
    var w = x1 - x0 + m * 2, h = y1 - y0 + m * 2;
    this.k = Math.max(1, Math.min(60, Math.min(this.VW / w, this.VH / h)));
    this.ox = -(x0 - m) * this.k; this.oy = -(y0 - m) * this.k;
    this.clamp(); this.apply();
  };

  /* ---------------------------------------------------------------- input */
  PlantMapSurveyed.prototype.onDown = function (e) {
    if (e.button !== 0) return;
    this.drag = { x: e.clientX, y: e.clientY, ox: this.ox, oy: this.oy,
                  moved: false, hit: e.target };
    this.svg.setPointerCapture(e.pointerId);
  };
  PlantMapSurveyed.prototype.onMove = function (e) {
    var d = e.target.dataset || {};
    if (d.str || d.inv || d.tx) {
      var txt = d.str ? d.str + " · " + d.trk
        : d.inv ? d.inv + " · inverter" : d.tx + " · cabina";
      if (d.str && this.state) {
        var s = this.statusOf(d.str);
        txt += " · " + (SEV_LABEL[s] || s);
      }
      this.tip.textContent = txt;
      this.tip.classList.add("on");
      this.tip.style.left = Math.min(e.clientX + 14, window.innerWidth - 240) + "px";
      this.tip.style.top = (e.clientY + 14) + "px";
    } else {
      this.tip.classList.remove("on");
    }
    if (!this.drag) return;
    var dx = e.clientX - this.drag.x, dy = e.clientY - this.drag.y;
    if (Math.abs(dx) + Math.abs(dy) > 3) this.drag.moved = true;
    this.ox = this.drag.ox + dx / this.fit; this.oy = this.drag.oy + dy / this.fit;
    this.clamp(); this.apply();
  };
  PlantMapSurveyed.prototype.onUp = function () {
    var d = this.drag;
    this.drag = null;
    if (!d || d.moved) return;
    var ds = (d.hit && d.hit.dataset) || {};
    if (ds.str) this.select({ kind: "string", id: ds.str }, false);
    else if (ds.inv) this.select({ kind: "inverter", id: ds.inv });
    else if (ds.tx) this.select({ kind: "tx", id: ds.tx });
  };

  window.PlantMapSurveyed = PlantMapSurveyed;
  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("surveyed-map-root");
    if (root) window.__plantMapSurveyed = new PlantMapSurveyed(root);
  });
})();
