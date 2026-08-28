/*
 * plant_map_surveyed.js - Mappa Impianto, surveyed layout.
 *
 * 370 trackers at their surveyed positions over the internal roads, the pond,
 * the 3 substations and the 36 inverter stations.
 *
 * Vista     Stringhe (808 clickable) or Tracker (370 clickable)
 * Colore    Stato (severity), TX, or Area
 * Seriali   toggle, then click a string to list its 25 panel serials
 *
 * Severity comes from /api/plant/surveyed/state, which routes the watchdog's
 * existing anomalies, inverter health and tracker alarms onto the element they
 * belong to. No alert rule is defined here.
 */
(function () {
  "use strict";

  var API = "/api/plant/surveyed";
  var REFRESH_MS = 60000;
  var SEV = ["red", "yellow", "grey", "green"];
  var SEV_LABEL = { red: "Critico", yellow: "Attenzione", grey: "Dati assenti", green: "Regolare" };
  var RANK = { green: 0, grey: 1, yellow: 2, red: 3 };
  var TX_COL = { TX1: "#3b82f6", TX2: "#8b5cf6", TX3: "#6366f1" };
  var AREA_COL = { 1: "#3b82f6", 2: "#60a5fa", 3: "#8b5cf6",
                   4: "#a78bfa", 5: "#6366f1", 6: "#818cf8" };

  var LAYOUT_MODES = [
    ["status", "Stato"], ["tx", "TX"], ["area", "Area"],
    ["alt", "Quota"], ["type", "Tipologia"], ["nstr", "Stringhe"], ["serial", "Seriali"]
  ];
  // one hue, light to dark, for the continuous ones
  var RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"];
  var TYPE_COL = { 25: "#6da7ec", 50: "#2a78d6", 75: "#104281" };
  var NSTR_COL = { 1: "#6da7ec", 2: "#2a78d6", 3: "#104281" };

  /* Minimum-area rectangle over a set of points (rotating calipers on the
     hull). Used for the substations, whose surveyed corners are not quite
     square. Returns the four corners in order. */
  function minAreaRect(pts) {
    if (!pts || pts.length < 3) return pts || [];
    var p = pts.slice().sort(function (a, b) { return a[0] - b[0] || a[1] - b[1]; });
    var cross = function (o, a, b) {
      return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
    };
    var half = function (seq) {
      var out = [];
      for (var i = 0; i < seq.length; i++) {
        while (out.length >= 2 &&
               cross(out[out.length - 2], out[out.length - 1], seq[i]) <= 0) out.pop();
        out.push(seq[i]);
      }
      out.pop();
      return out;
    };
    var h = half(p).concat(half(p.slice().reverse()));
    if (h.length < 3) return pts;
    var best = null;
    for (var i = 0; i < h.length; i++) {
      var a = h[i], b = h[(i + 1) % h.length];
      var dx = b[0] - a[0], dy = b[1] - a[1];
      var len = Math.hypot(dx, dy) || 1;
      var ux = dx / len, uy = dy / len;          // along the edge
      var vx = -uy, vy = ux;                     // across it
      var lo1 = Infinity, hi1 = -Infinity, lo2 = Infinity, hi2 = -Infinity;
      for (var j = 0; j < h.length; j++) {
        var e1 = h[j][0] * ux + h[j][1] * uy;
        var e2 = h[j][0] * vx + h[j][1] * vy;
        if (e1 < lo1) lo1 = e1; if (e1 > hi1) hi1 = e1;
        if (e2 < lo2) lo2 = e2; if (e2 > hi2) hi2 = e2;
      }
      var area = (hi1 - lo1) * (hi2 - lo2);
      if (!best || area < best.area) {
        best = { area: area, ux: ux, uy: uy, vx: vx, vy: vy,
                 lo1: lo1, hi1: hi1, lo2: lo2, hi2: hi2 };
      }
    }
    var mk = function (e1, e2) {
      return [e1 * best.ux + e2 * best.vx, e1 * best.uy + e2 * best.vy];
    };
    var rect = [mk(best.lo1, best.lo2), mk(best.hi1, best.lo2),
                mk(best.hi1, best.hi2), mk(best.lo1, best.hi2)];
    // the enclosing rectangle is larger than the trapezoid it wraps, so pull it
    // back to the surveyed footprint about its own centre
    var poly = Math.abs(shoelace(pts)), box = Math.abs(shoelace(rect));
    if (poly > 0 && box > poly) {
      var k = Math.sqrt(poly / box);
      var rx = (rect[0][0] + rect[2][0]) / 2, ry = (rect[0][1] + rect[2][1]) / 2;
      rect = rect.map(function (q) {
        return [rx + (q[0] - rx) * k, ry + (q[1] - ry) * k];
      });
    }
    // sit it on the building's own centre of area, not the wrapper's
    var c = centroid(pts);
    var mx = (rect[0][0] + rect[2][0]) / 2, my = (rect[0][1] + rect[2][1]) / 2;
    return rect.map(function (q) { return [q[0] + c[0] - mx, q[1] + c[1] - my]; });
  }

  function centroid(p) {
    var a = 0, x = 0, y = 0;
    for (var i = 0; i < p.length; i++) {
      var q = p[(i + 1) % p.length];
      var f = p[i][0] * q[1] - q[0] * p[i][1];
      a += f; x += (p[i][0] + q[0]) * f; y += (p[i][1] + q[1]) * f;
    }
    if (!a) {
      var sx = 0, sy = 0;
      p.forEach(function (q2) { sx += q2[0]; sy += q2[1]; });
      return [sx / p.length, sy / p.length];
    }
    return [x / (3 * a), y / (3 * a)];
  }

  function shoelace(p) {
    var a = 0;
    for (var i = 0; i < p.length; i++) {
      var q = p[(i + 1) % p.length];
      a += p[i][0] * q[1] - q[0] * p[i][1];
    }
    return a / 2;
  }

  var NS = "http://www.w3.org/2000/svg";
  function sv(t, a) { var n = document.createElementNS(NS, t); for (var k in a) n.setAttribute(k, a[k]); return n; }
  function el(t, c, x) { var n = document.createElement(t); if (c) n.className = c;
    if (x !== undefined && x !== null) n.textContent = x; return n; }

  function PlantMapSurveyed(root) {
    this.root = root;
    this.layout = null; this.state = null;
    this.filter = null; this.sel = null;
    this.view = "string";      // string | tracker
    this.colour = "status";    // status | tx | area
    this.serialMode = false;
    this.k = 1; this.ox = 0; this.oy = 0; this.fit = 1;
    this.build();
  }
  var P = PlantMapSurveyed.prototype;

  P.build = function () {
    var self = this;
    this.root.innerHTML = "";
    this.root.classList.add("svm");

    var bar = el("div", "svm-bar");
    this.counts = el("div", "svm-counts");

    var modes = el("div", "svm-modes");
    function group(label, opts, get, set) {
      var g = el("div", "svm-seg");
      g.appendChild(el("span", "svm-seg-label", label));
      opts.forEach(function (o) {
        var b = el("button", "svm-segbtn", o[1]);
        b.dataset.val = o[0];
        b.onclick = function () { set(o[0]); };
        g.appendChild(b);
      });
      g.sync = function () {
        Array.prototype.forEach.call(g.querySelectorAll(".svm-segbtn"), function (b) {
          b.classList.toggle("on", b.dataset.val === get());
        });
      };
      return g;
    }
    this.segView = group("Vista", [["string", "Stringhe"], ["tracker", "Tracker"]],
      function () { return self.view; },
      function (v) { self.view = v; self.sel = null; self.draw(); self.paint(); });
    this.segCol = group("Colore", LAYOUT_MODES,
      function () { return self.colour; },
      function (v) {
        self.colour = v;
        if (v === "serial") self.ensureCoverage();
        self.paint();
      });
    modes.appendChild(this.segView); modes.appendChild(this.segCol);

    // one click to the physical picture of the site
    this.layoutBtn = el("button", "svm-btn", "Layout generale");
    this.layoutBtn.title = "Vista d'insieme: quota, tipologia, stringhe, seriali";
    this.layoutBtn.onclick = function () {
      self.colour = "alt"; self.view = "tracker"; self.sel = null;
      self.draw(); self.paint();
    };
    modes.appendChild(this.layoutBtn);

    this.serialBtn = el("button", "svm-btn svm-serialbtn", "Vedi seriali");
    this.serialBtn.onclick = function () {
      self.serialMode = !self.serialMode;
      self.serialBtn.classList.toggle("on", self.serialMode);
      if (self.serialMode && self.view !== "string") {
        self.view = "string"; self.draw(); self.paint();
      }
      self.renderDetail();
    };
    modes.appendChild(this.serialBtn);

    var tools = el("div", "svm-tools");
    this.search = el("input", "svm-search");
    this.search.type = "search";
    this.search.placeholder = "Cerca TRACKER 198, STR21, MPPT05, seriale…";
    this.search.addEventListener("input", function () { self.runSearch(); });
    var mk = function (t, fn, cls) { var b = el("button", "svm-btn" + (cls || ""), t); b.onclick = fn; return b; };
    tools.appendChild(this.search);
    tools.appendChild(mk("+", function () { self.zoomTo(self.k * 1.6); }));
    tools.appendChild(mk("−", function () { self.zoomTo(self.k / 1.6); }));
    tools.appendChild(mk("Adatta", function () { self.fitAll(); self.select(null); }));
    bar.appendChild(this.counts); bar.appendChild(modes); bar.appendChild(tools);

    var grid = el("div", "svm-grid");
    var side = el("aside", "svm-side");
    this.detail = el("div", "svm-detail");
    this.problems = el("div", "svm-problems");
    side.appendChild(this.detail); side.appendChild(this.problems);

    var mapwrap = el("div", "svm-mapwrap");
    this.svg = sv("svg", { class: "svm-svg", role: "img", "aria-label": "Mappa impianto, nord in alto" });
    mapwrap.appendChild(this.svg);
    this.results = el("div", "svm-results");
    this.legend = el("div", "svm-legend");
    mapwrap.appendChild(this.results); mapwrap.appendChild(this.legend);

    grid.appendChild(side); grid.appendChild(mapwrap);   // panel on the left
    this.tip = el("div", "svm-tip");
    this.root.appendChild(bar); this.root.appendChild(grid); this.root.appendChild(this.tip);

    this.svg.addEventListener("pointerdown", function (e) { self.onDown(e); });
    this.svg.addEventListener("pointermove", function (e) { self.onMove(e); });
    this.svg.addEventListener("pointerup", function () { self.onUp(); });
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

  P.syncSegs = function () { this.segView.sync(); this.segCol.sync(); };

  P.load = function () {
    var self = this;
    fetch(API + "/layout", { credentials: "same-origin" })
      .then(function (r) { if (!r.ok) throw new Error("layout " + r.status); return r.json(); })
      .then(function (j) {
        self.layout = j;
        self.byTracker = {};
        j.trackers.forEach(function (t) { self.byTracker[t.id] = t; });
        self.draw(); return self.refresh();
      })
      .catch(function (e) {
        self.root.innerHTML = '<div class="svm-err">Mappa non disponibile: ' + e.message + "</div>";
      });
  };

  P.refresh = function () {
    var self = this;
    return fetch(API + "/state", { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { if (j && !j.error) { self.state = j; self.paint(); } });
  };

  /* ---------------------------------------------------------------- draw */
  P.draw = function () {
    var L = this.layout, self = this, PAD = 18;
    var xs = [], ys = [];
    L.trackers.forEach(function (t) {
      xs.push(t.x - t.w / 2); xs.push(t.x + t.w / 2); ys.push(t.y0); ys.push(t.y1);
    });
    (L.transformers || []).concat(L.pond || []).forEach(function (o) {
      o.p.forEach(function (p) { xs.push(p[0]); ys.push(p[1]); });
    });
    this.b = { x0: Math.min.apply(null, xs) - PAD, x1: Math.max.apply(null, xs) + PAD,
               y0: Math.min.apply(null, ys) - PAD, y1: Math.max.apply(null, ys) + PAD };
    this.VW = this.b.x1 - this.b.x0; this.VH = this.b.y1 - this.b.y0;
    this.svg.setAttribute("viewBox", "0 0 " + this.VW + " " + this.VH);
    this.svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
    this.svg.innerHTML = "";

    var px = this.px = function (x) { return x - self.b.x0; };
    var py = this.py = function (y) { return self.b.y1 - y; };
    var d = function (pts, close) {
      return pts.map(function (p, i) { return (i ? "L" : "M") + px(p[0]) + " " + py(p[1]); })
        .join(" ") + (close ? " Z" : "");
    };

    this.scene = sv("g", {});
    var gSite = sv("g", {}), gTrk = sv("g", {}), gPan = sv("g", {}),
        gDiv = sv("g", {}), gMark = sv("g", {}), gDev = sv("g", {}),
        gSel = sv("g", {}), gLab = sv("g", {});
    [gSite, gTrk, gPan, gDiv, gMark, gDev, gSel, gLab]
      .forEach(function (g) { self.scene.appendChild(g); });
    this.gPan = gPan; this.gMark = gMark;
    this.svg.appendChild(this.scene);

    (L.pond || []).forEach(function (o) { gSite.appendChild(sv("path", { class: "svm-pond", d: d(o.p, true) })); });
    (L.roads || []).forEach(function (o) {
      gSite.appendChild(sv("path", { class: "svm-road " + (o.k || "").split(" ")[0], d: d(o.p, o.c) }));
    });

    this.rects = {};       // string id -> rect   (string view)
    this.trkRects = {};    // tracker id -> rect  (tracker view)
    L.trackers.forEach(function (t) {
      var cx = px(t.x), top = py(t.y0), h = t.y0 - t.y1;
      if (self.view === "tracker") {
        var r = sv("rect", { class: "svm-str", x: cx - t.w / 2, y: top, width: t.w, height: h });
        var hit = sv("rect", { class: "svm-hit", x: cx - 2.6, y: top, width: 5.2, height: h });
        hit.dataset.trk = t.id;
        gTrk.appendChild(r); gTrk.appendChild(hit);
        self.trkRects[t.id] = r;
      } else {
        var n = t.strings.length || 1, span = h / n;
        t.strings.forEach(function (sid, i) {
          var y = top + i * span;
          var rr = sv("rect", { class: "svm-str", x: cx - t.w / 2, y: y, width: t.w, height: span });
          var hh = sv("rect", { class: "svm-hit", x: cx - 2.6, y: y, width: 5.2, height: span });
          hh.dataset.str = sid; hh.dataset.trk = t.id; hh.dataset.mppt = t.mppts[i] || "";
          gTrk.appendChild(rr); gTrk.appendChild(hh);
          self.rects[sid] = rr;
        });
        for (var i = 1; i < n; i++) {
          gDiv.appendChild(sv("line", { class: "svm-div", x1: cx - t.w / 2, x2: cx + t.w / 2,
            y1: top + i * span, y2: top + i * span }));
        }
      }
    });

    this.txLabels = [];
    (L.transformers || []).forEach(function (o) {
      var rect = minAreaRect(o.p);
      gDev.appendChild(sv("path", { class: "svm-tx", d: d(rect, true) }));
      var hit = sv("path", { class: "svm-hit", d: d(rect, true) });
      hit.dataset.tx = o.id; gDev.appendChild(hit);
      var lab = sv("text", { class: "svm-txlab", "text-anchor": "middle" });
      lab.textContent = o.id;
      lab.dataset.x = px(o.cx); lab.dataset.y = py(o.cy) - 9;
      gLab.appendChild(lab); self.txLabels.push(lab);
    });

    this.invShapes = [];
    (L.inverters || []).forEach(function (o) {
      var shape = sv("rect", { class: "svm-inv", rx: 0.12 });
      var hit = sv("rect", { class: "svm-hit" });
      hit.dataset.inv = o.id;
      gDev.appendChild(shape); gDev.appendChild(hit);
      self.invShapes.push({ shape: shape, hit: hit, cx: px(o.x) + o.w / 2,
        cy: py(o.y) - o.d / 2, w: o.w, d: o.d, id: o.id });
    });

    this.gSel = gSel;
    this.measure(); this.fitAll();
    this.syncSegs();
  };

  /* ---------------------------------------------------------------- colour */
  P.statusOf = function (sid) {
    var st = this.state;
    return (st && st.strings && st.strings[sid] && st.strings[sid].status) || "grey";
  };
  P.trackerStatus = function (t) {
    var self = this, worst = "green";
    if (this.state && this.state.trackers && this.state.trackers[t.id]) {
      worst = this.state.trackers[t.id].status || "green";
    }
    t.strings.forEach(function (s) {
      var v = self.statusOf(s);
      if (RANK[v] > RANK[worst]) worst = v;
    });
    return worst;
  };
  P.fillFor = function (t, sid) {
    var c = this.colour;
    if (c === "tx") return TX_COL[t.tx] || "#6b7280";
    if (c === "area") return AREA_COL[t.area] || "#6b7280";
    if (c === "type") return TYPE_COL[t.modules] || "#6b7280";
    if (c === "nstr") return NSTR_COL[t.strings.length] || "#6b7280";
    if (c === "alt") {
      var r = this.altRange();
      if (!r) return "#6b7280";
      var f = (t.alt - r[0]) / (r[1] - r[0] || 1);
      return RAMP[Math.max(0, Math.min(RAMP.length - 1, Math.round(f * (RAMP.length - 1))))];
    }
    if (c === "serial") {
      var cov = (this.coverage || {})[t.id];
      if (!cov) return "#6b7280";
      if (cov.unassigned) return "#f59e0b";
      return cov.panels === t.modules ? "#10b981" : "#ef4444";
    }
    return null;   // severity handled by class
  };

  P.altRange = function () {
    if (!this.layout) return null;
    if (!this._alt) {
      var a = this.layout.trackers.map(function (t) { return t.alt; });
      this._alt = [Math.min.apply(null, a), Math.max.apply(null, a)];
    }
    return this._alt;
  };

  P.ensureCoverage = function () {
    if (this.coverage || this._covPending) return;
    this._covPending = 1;
    var self = this;
    fetch(API + "/serials", { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { if (j && j.coverage) { self.coverage = j.coverage; self.paint(); } })
      .catch(function () {});
  };

  P.paint = function () {
    var self = this, L = this.layout, st = this.state;
    if (!L) return;
    var keep = this.filterSet();

    L.trackers.forEach(function (t) {
      if (self.view === "tracker") {
        var r = self.trkRects[t.id];
        if (!r) return;
        var s = self.trackerStatus(t);
        var c = self.fillFor(t);
        r.setAttribute("class", "svm-str" + (c ? "" : " s-" + s));
        if (c) r.style.fill = c; else r.style.fill = "";
        r.classList.toggle("dim", !(!keep || t.strings.some(function (x) { return keep.has(x); })));
      } else {
        t.strings.forEach(function (sid) {
          var r = self.rects[sid];
          if (!r) return;
          var c = self.fillFor(t, sid);
          r.setAttribute("class", "svm-str" + (c ? "" : " s-" + self.statusOf(sid)));
          if (c) r.style.fill = c; else r.style.fill = "";
          r.classList.toggle("dim", !!keep && !keep.has(sid));
        });
      }
    });
    this.invShapes.forEach(function (o) {
      var s = (st && st.inverters[o.id] && st.inverters[o.id].status) || "grey";
      o.shape.setAttribute("class", "svm-inv s-" + s);
    });

    this.paintDim();
    this.drawCounts(); this.drawLegend(); this.drawProblems();
    this.renderDetail(); this.syncSegs(); this.apply();
  };

  P.drawCounts = function () {
    var self = this, st = this.state;
    this.counts.innerHTML = "";
    if (!st) return;
    [["strings", "Stringhe"], ["mppts", "MPPT"], ["trackers", "Tracker"], ["inverters", "Inverter"]]
      .forEach(function (pair) {
        var g = (st.counts || {})[pair[0]] || {};
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
        box.appendChild(row); self.counts.appendChild(box);
      });
    if (!st.has_snapshot) this.counts.appendChild(el("div", "svm-count svm-nodata",
      "Nessuno snapshot per " + st.date));
  };

  P.filterSet = function () {
    var st = this.state, L = this.layout, self = this;
    if (!this.filter || !st) return null;
    var keep = new Set();
    if (RANK[this.filter] !== undefined) {
      L.strings.forEach(function (s) { if (self.statusOf(s.id) === self.filter) keep.add(s.id); });
      return keep;
    }
    (st.problems || []).forEach(function (p) {
      if (p.key !== self.filter) return;
      L.strings.forEach(function (s) {
        if ((p.mppts || []).indexOf(s.mppt) >= 0) keep.add(s.id);
        if ((p.inverters || []).indexOf(s.inverter) >= 0) keep.add(s.id);
        if ((p.trackers || []).indexOf(s.tracker) >= 0) keep.add(s.id);
      });
    });
    return keep;
  };
  P.setFilter = function (f) { this.filter = f; this.paint(); };

  P.drawLegend = function () {
    var self = this, st = this.state || {};
    this.legend.innerHTML = "";
    var mk = function (colour, cls, label, n, on, fn) {
      var b = el("button", "svm-legend-item" + (on ? " on" : ""));
      var sw = el("span", "svm-sw" + (cls ? " s-" + cls : ""));
      if (colour) sw.style.background = colour;
      b.appendChild(sw); b.appendChild(el("span", "svm-legend-label", label));
      if (n !== null && n !== undefined) b.appendChild(el("span", "svm-legend-n", n));
      if (fn) b.onclick = fn; else b.disabled = true;
      return b;
    };
    var TITLES = { status: "Legenda — clicca per filtrare", tx: "Sotto-campo (TX)",
      area: "Area", alt: "Quota telaio (m s.l.m.)", type: "Tipologia struttura",
      nstr: "Stringhe per tracker", serial: "Copertura seriali" };
    var head = el("div", "svm-legend-head", TITLES[this.colour] || "Legenda");
    this.legend.appendChild(head);
    var ul = el("div", "svm-legend-list");

    if (this.colour === "tx") {
      Object.keys(TX_COL).forEach(function (k) {
        var n = self.layout.trackers.filter(function (t) { return t.tx === k; }).length;
        ul.appendChild(mk(TX_COL[k], null, k, n, false, null));
      });
    } else if (this.colour === "area") {
      Object.keys(AREA_COL).forEach(function (k) {
        var n = self.layout.trackers.filter(function (t) { return String(t.area) === k; }).length;
        ul.appendChild(mk(AREA_COL[k], null, "Area " + k, n, false, null));
      });
    } else if (this.colour === "alt") {
      var r = this.altRange() || [0, 1];
      var ramp = el("div", "svm-ramp");
      RAMP.forEach(function (c2) { var i = el("i"); i.style.background = c2; ramp.appendChild(i); });
      this.legend.appendChild(ramp);
      var ends = el("div", "svm-ramp-ends");
      ends.appendChild(el("span", null, r[0].toFixed(1) + " m"));
      ends.appendChild(el("span", null, r[1].toFixed(1) + " m"));
      this.legend.appendChild(ends);
      ul.appendChild(el("div", "svm-legend-none",
        "Dislivello " + (r[1] - r[0]).toFixed(1) + " m sull'impianto"));
    } else if (this.colour === "type") {
      [25, 50, 75].forEach(function (m2) {
        var n = self.layout.trackers.filter(function (t) { return t.modules === m2; }).length;
        ul.appendChild(mk(TYPE_COL[m2], null, m2 + " moduli", n, false, null));
      });
    } else if (this.colour === "nstr") {
      [1, 2, 3].forEach(function (m2) {
        var n = self.layout.trackers.filter(function (t) { return t.strings.length === m2; }).length;
        ul.appendChild(mk(NSTR_COL[m2], null, m2 + (m2 === 1 ? " stringa" : " stringhe"), n, false, null));
      });
    } else if (this.colour === "serial") {
      var cov = this.coverage || {};
      var ok = 0, bad = 0, warn = 0, none = 0;
      self.layout.trackers.forEach(function (t) {
        var c2 = cov[t.id];
        if (!c2) { none++; } else if (c2.unassigned) { warn++; }
        else if (c2.panels === t.modules) { ok++; } else { bad++; }
      });
      ul.appendChild(mk("#10b981", null, "Seriali completi", ok, false, null));
      if (warn) ul.appendChild(mk("#f59e0b", null, "Seriali in eccesso", warn, false, null));
      if (bad) ul.appendChild(mk("#ef4444", null, "Seriali mancanti", bad, false, null));
      if (none) ul.appendChild(mk("#6b7280", null, "Non rilevato", none, false, null));
    } else {
      SEV.forEach(function (s) {
        var n = ((st.counts || {}).strings || {})[s] || 0;
        ul.appendChild(mk(null, s, SEV_LABEL[s], n, self.filter === s, function () {
          self.setFilter(self.filter === s ? null : s);
        }));
      });
      (st.legend || []).forEach(function (e) {
        ul.appendChild(mk(null, e.severity, e.label, e.count, self.filter === e.key, function () {
          self.setFilter(self.filter === e.key ? null : e.key);
        }));
      });
      if (!(st.legend || []).length) ul.appendChild(el("div", "svm-legend-none", "Nessuna anomalia attiva"));
    }
    this.legend.appendChild(ul);
  };

  P.drawProblems = function () {
    var self = this, st = this.state || {};
    this.problems.innerHTML = "";
    var h = el("div", "svm-side-head");
    h.appendChild(el("span", null, "Problemi attivi"));
    h.appendChild(el("span", "svm-side-n", String((st.problems || []).length)));
    this.problems.appendChild(h);
    if (!(st.problems || []).length) {
      this.problems.appendChild(el("div", "svm-none", "Nessun problema attivo."));
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

  P.gotoProblem = function (p) {
    if (p.mppts && p.mppts.length) return this.select({ kind: "mppt", id: p.mppts[0] });
    if (p.trackers && p.trackers.length) return this.select({ kind: "tracker", id: p.trackers[0] });
    if (p.inverters && p.inverters.length) return this.select({ kind: "inverter", id: p.inverters[0] });
    this.fitAll();
  };

  /* ---------------------------------------------------------------- select */
  P.stringsFor = function (sel) {
    var L = this.layout;
    if (!sel || !L) return [];
    var f = { string: "id", mppt: "mppt", tracker: "tracker",
              inverter: "inverter", tx: "tx", area: "area" }[sel.kind];
    if (!f) return [];
    return L.strings.filter(function (s) { return String(s[f]) === String(sel.id); });
  };

  P.select = function (sel, zoom) {
    this.sel = sel;
    this.outline(sel);
    this.renderDetail();
    if (sel && zoom !== false) this.zoomToSel(sel);
  };

  /* Precise marking. A hull around an inverter's trackers swallows every
     string that happens to lie between them, which is exactly what it must not
     do: an inverter's strings are interleaved with its neighbours'. So each
     member segment is outlined individually and everything else is dimmed. */
  P.outline = function (sel) {
    var self = this;
    while (this.gSel.firstChild) this.gSel.removeChild(this.gSel.firstChild);
    this.marked = null;
    if (!sel) { this.paintDim(); return; }
    var ss = this.stringsFor(sel);
    if (!ss.length) { this.paintDim(); return; }
    var ids = new Set();
    ss.forEach(function (s) { ids.add(s.id); });
    this.marked = ids;

    if (this.view === "tracker") {
      var seen = {};
      ss.forEach(function (s) {
        if (seen[s.tracker]) return;
        seen[s.tracker] = 1;
        var r = self.trkRects[s.tracker];
        if (r) self.gSel.appendChild(sv("rect", { class: "svm-mark",
          x: +r.getAttribute("x") - 0.5, y: +r.getAttribute("y") - 0.5,
          width: +r.getAttribute("width") + 1, height: +r.getAttribute("height") + 1 }));
      });
    } else {
      ss.forEach(function (s) {
        var r = self.rects[s.id];
        if (r) self.gSel.appendChild(sv("rect", { class: "svm-mark",
          x: +r.getAttribute("x") - 0.5, y: +r.getAttribute("y") - 0.5,
          width: +r.getAttribute("width") + 1, height: +r.getAttribute("height") + 1 }));
      });
    }
    this.paintDim();
  };

  /* members stay lit, everything else drops back */
  P.paintDim = function () {
    var self = this, L = this.layout;
    if (!L) return;
    L.trackers.forEach(function (t) {
      if (self.view === "tracker") {
        var r = self.trkRects[t.id];
        if (!r) return;
        var on = !self.marked || t.strings.some(function (x) { return self.marked.has(x); });
        r.classList.toggle("faded", !on);
      } else {
        t.strings.forEach(function (sid) {
          var r = self.rects[sid];
          if (!r) return;
          r.classList.toggle("faded", !!self.marked && !self.marked.has(sid));
        });
      }
    });
  };

  /* ---------------------------------------------------------------- detail */
  P.renderDetail = function () {
    var st = this.state || {}, L = this.layout, self = this;
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
      this.detail.appendChild(el("div", "svm-hint", this.serialMode
        ? "Modalità seriali attiva. Clicca una stringa per vederne i 25 seriali."
        : "Clicca un elemento sulla mappa o un problema qui sotto."));
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
      kv.appendChild(el("span", "svm-k", k)); kv.appendChild(el("span", "svm-v", v));
    }
    var ss = this.stringsFor(sel), s0 = ss[0];
    if (s0) {
      add("Inverter", s0.inverter); add("TX", s0.tx); add("Area", s0.area);
      add("Tracker", s0.tracker); add("TCU", s0.tcu); add("NCU", s0.ncu);
      if (sel.kind !== "string") add("Stringhe", ss.length);
    }
    if (sel.kind === "string") add("MPPT", s0 && s0.mppt);
    if (sel.kind === "mppt" && st.mppts && st.mppts[sel.id]) {
      var mm = st.mppts[sel.id];
      add("Stato", SEV_LABEL[mm.status] || mm.status);
      add("Corrente", mm.v != null ? mm.v + " A" : null);
      add("Attesa", mm.exp != null ? mm.exp + " A" : null);
    }
    if (sel.kind === "tracker" && st.trackers && st.trackers[sel.id]) {
      var tt = st.trackers[sel.id];
      add("Stato", SEV_LABEL[tt.status] || tt.status);
      add("Angolo target", tt.target_angle); add("Angolo attuale", tt.actual_angle);
      add("Scarto", tt.deviation != null ? tt.deviation + " deg" : null);
      add("Modo", tt.mode); add("Allarme", tt.alarm); add("Nota", tt.reason);
    }
    if (sel.kind === "inverter") { this.detail.appendChild(kv); this.renderInverter(sel.id); return; }
    var trk = s0 && this.byTracker[s0.tracker];
    if (trk && (sel.kind === "string" || sel.kind === "tracker")) {
      add("Moduli", trk.modules); add("Pali", trk.piles);
      add("Lunghezza", trk.len + " m"); add("Quota", trk.alt + " m");
      add("Coordinate", trk.lat + ", " + trk.lon);
    }
    this.detail.appendChild(kv);

    if (this.panelSel && sel.kind === "string" && this.panelSel.string === sel.id) {
      var ps = this.panelSel;
      var cached = (this.panelCache || {})[ps.tracker + "#" + ps.n];
      var pb = el("div", "svm-panelsel");
      pb.appendChild(el("span", "svm-k", "Pannello " + ps.n + " del tracker"));
      pb.appendChild(el("span", "svm-serial-code", cached || "seriale non caricato"));
      this.detail.appendChild(pb);
    }
    if (this.serialMode && (sel.kind === "string" || sel.kind === "tracker")) {
      this.renderSerials(sel);
    }
    if (sel.kind !== "string" && ss.length > 1 && !this.serialMode) {
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

  /* Everything under one inverter: what it is producing now, then every
     tracker, TCU, MPPT and string beneath it with the status each holds. */
  P.renderInverter = function (invId) {
    var self = this;
    var box = el("div", "svm-sub");
    box.appendChild(el("div", "svm-sub-head", "Produzione e catena"));
    var body = el("div", null, "Caricamento…");
    box.appendChild(body);
    this.detail.appendChild(box);
    fetch(API + "/inverter?id=" + encodeURIComponent(invId), { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        body.innerHTML = "";
        if (!j || j.error) { body.appendChild(el("div", "svm-none", "Dati non disponibili.")); return; }
        var p = j.production || {};
        var prod = el("div", "svm-prod");
        [["Potenza AC", p.ac_w != null ? Math.round(p.ac_w / 1000 * 10) / 10 + " kW" : "-"],
         ["Corrente DC", p.dc_a != null ? Math.round(p.dc_a * 100) / 100 + " A" : "-"],
         ["PR", p.pr_pct != null ? p.pr_pct + " %" : "-"],
         ["Temperatura", p.temp_c != null ? p.temp_c + " °C" : "-"]].forEach(function (r2) {
          var c = el("div", "svm-prod-cell");
          c.appendChild(el("span", "svm-prod-label", r2[0]));
          c.appendChild(el("span", "svm-prod-val", r2[1]));
          prod.appendChild(c);
        });
        body.appendChild(prod);
        if (p.comms_lost) body.appendChild(el("div", "svm-warn", "Comunicazione persa."));
        if (p.data_time) body.appendChild(el("div", "svm-serial-head", "Dato delle " + p.data_time));

        var c = j.counts || {};
        var row = el("div", "svm-count-row");
        ["red", "yellow", "green", "grey"].forEach(function (k) {
          if (!c[k]) return;
          var chip = el("span", "svm-chip s-" + k, c[k]);
          chip.title = k;
          row.appendChild(chip);
        });
        var ch = el("div", "svm-sub");
        ch.appendChild(el("div", "svm-sub-head", "Stringhe per stato"));
        ch.appendChild(row);
        body.appendChild(ch);

        var th = j.thresholds || {};
        var noon = (j.hour != null && j.hour <= 12);
        body.appendChild(el("div", "svm-serial-head",
          "Corrente per stringa = corrente MPPT / numero stringhe. Lo stato usa la "
          + "corrente MPPT normalizzata su base 2 stringhe, come le soglie del watchdog. "
          + "Soglie "
          + (noon ? "mattino" : "pomeriggio") + ": verde ≥ "
          + (noon ? th.morning_green : th.afternoon_green) + " A, giallo ≥ "
          + (noon ? th.morning_yellow : th.afternoon_yellow) + " A."));

        var list = el("div", "svm-chain");
        (j.trackers || []).forEach(function (t) {
          var tb = el("div", "svm-chain-trk");
          var hd = el("button", "svm-chain-head");
          hd.appendChild(el("span", "svm-sw s-" + t.status));
          hd.appendChild(el("span", "svm-chain-name", t.tracker));
          hd.appendChild(el("span", "svm-chain-tcu", t.tcu));
          hd.onclick = function () { self.select({ kind: "tracker", id: t.tracker }); };
          tb.appendChild(hd);
          (t.strings || []).forEach(function (s2) {
            var b = el("button", "svm-chain-str");
            b.appendChild(el("span", "svm-sw s-" + s2.status));
            b.appendChild(el("span", "svm-chain-sid", s2.string.slice(-5)));
            b.appendChild(el("span", "svm-chain-mppt", s2.mppt.slice(-6)));
            b.appendChild(el("span", "svm-chain-a",
              s2.per_string_a != null ? s2.per_string_a + " A" : "-"));
            if (s2.note) b.title = s2.note;
            b.onclick = function () { self.select({ kind: "string", id: s2.string }); };
            tb.appendChild(b);
          });
          list.appendChild(tb);
        });
        body.appendChild(list);
      })
      .catch(function () { body.textContent = "Dati non disponibili."; });
  };

  P.renderSerials = function (sel) {
    var box = el("div", "svm-sub svm-serials");
    box.appendChild(el("div", "svm-sub-head", "Seriali pannelli"));
    var body = el("div", "svm-serial-body", "Caricamento…");
    box.appendChild(body);
    this.detail.appendChild(box);
    var q = sel.kind === "string" ? "string=" + encodeURIComponent(sel.id)
                                  : "tracker=" + encodeURIComponent(sel.id);
    fetch(API + "/serials?" + q, { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        body.innerHTML = "";
        if (!j || !j.serials || !j.serials.length) {
          body.appendChild(el("div", "svm-none", "Nessun seriale registrato."));
          return;
        }
        var head = el("div", "svm-serial-head", j.count + " pannelli · ordine da nord a sud");
        body.appendChild(head);
        var list = el("div", "svm-serial-list");
        var last = null;
        j.serials.forEach(function (s) {
          if (s.string && s.string !== last) {
            list.appendChild(el("div", "svm-serial-group", s.string));
            last = s.string;
          }
          var row = el("div", "svm-serial");
          row.appendChild(el("span", "svm-serial-n", s.n));
          var code = el("span", "svm-serial-code", s.serial);
          code.title = "Clicca per copiare";
          code.onclick = function () {
            if (navigator.clipboard) navigator.clipboard.writeText(s.serial);
            code.classList.add("copied");
            setTimeout(function () { code.classList.remove("copied"); }, 900);
          };
          row.appendChild(code);
          list.appendChild(row);
        });
        body.appendChild(list);
        if (j.unassigned && j.unassigned.length) {
          body.appendChild(el("div", "svm-warn",
            j.unassigned.length + " seriale/i in eccesso rispetto ai moduli: "
            + j.unassigned.join(", ")));
        }
      })
      .catch(function () { body.textContent = "Seriali non disponibili."; });
  };

  /* ---------------------------------------------------------------- search */
  P.runSearch = function () {
    var q = this.search.value.trim().toUpperCase(), self = this, L = this.layout;
    this.results.innerHTML = "";
    if (q.length < 2 || !L) { this.results.classList.remove("on"); return; }
    var hits = [];
    L.trackers.forEach(function (t) { if (t.id.indexOf(q) >= 0) hits.push({ kind: "tracker", id: t.id }); });
    L.strings.forEach(function (s) { if (s.id.indexOf(q) >= 0) hits.push({ kind: "string", id: s.id }); });
    L.mppts.forEach(function (m) { if (m.id.indexOf(q) >= 0) hits.push({ kind: "mppt", id: m.id }); });
    (L.inverters || []).forEach(function (i) { if (i.id.indexOf(q) >= 0) hits.push({ kind: "inverter", id: i.id }); });
    hits = hits.slice(0, 40);
    if (!hits.length) { this.results.classList.remove("on"); return; }
    hits.forEach(function (h) {
      var b = el("button", "svm-res");
      b.appendChild(el("span", null, h.id));
      b.appendChild(el("span", "svm-res-kind", h.kind));
      b.onclick = function () { self.select(h); self.results.classList.remove("on"); self.search.blur(); };
      self.results.appendChild(b);
    });
    this.results.classList.add("on");
  };

  /* ---------------------------------------------------------------- view */
  P.measure = function () {
    var r = this.svg.getBoundingClientRect();
    this.fit = Math.min(r.width / this.VW, r.height / this.VH) || 1;
  };
  /* One rect per module once they are large enough to see, for the trackers
     actually on screen. 20 200 panels cannot all be in the DOM at once. */
  P.PANEL_PX = 5.0;
  P.updatePanels = function () {
    var self = this, L = this.layout;
    if (!L || !this.gPan) return;
    cancelAnimationFrame(this._panFrame);
    this._panFrame = requestAnimationFrame(function () {
      while (self.gPan.firstChild) self.gPan.removeChild(self.gPan.firstChild);
      var pxPerM = self.fit * self.k;
      self.panelZoom = false;
      if (pxPerM * 1.08 < self.PANEL_PX) return;
      var r = self.svg.getBoundingClientRect();
      var x0 = (-self.ox * self.fit - (r.width - self.VW * self.fit) / 2) / pxPerM;
      var y0 = (-self.oy * self.fit - (r.height - self.VH * self.fit) / 2) / pxPerM;
      var x1 = x0 + r.width / pxPerM, y1 = y0 + r.height / pxPerM;
      var frag = document.createDocumentFragment(), n = 0;
      L.trackers.forEach(function (t) {
        if (n > 4000) return;
        var cx = self.px(t.x), ty0 = self.py(t.y0), ty1 = self.py(t.y1);
        if (cx < x0 - 3 || cx > x1 + 3 || ty1 < y0 || ty0 > y1) return;
        var pitch = (t.y0 - t.y1) / t.mod;
        if (pitch * pxPerM < self.PANEL_PX) return;
        var perString = t.mod / (t.strings.length || 1);
        for (var i = 0; i < t.mod; i++) {
          var y = ty0 + i * pitch;
          if (y + pitch < y0 || y > y1) continue;
          var cell = sv("rect", { class: "svm-panel", x: cx - t.w / 2 + 0.06,
            y: y + 0.06, width: t.w - 0.12, height: Math.max(pitch - 0.12, 0.05) });
          cell.dataset.panel = i + 1;
          cell.dataset.trk = t.id;
          cell.dataset.str = t.strings[Math.floor(i / perString)] || "";
          frag.appendChild(cell);
          n++;
        }
      });
      self.gPan.appendChild(frag);
      self.panelZoom = true;
    });
  };

  P.apply = function () {
    if (!this.scene) return;
    this.scene.setAttribute("transform", "translate(" + this.ox + " " + this.oy + ") scale(" + this.k + ")");
    var s = 1 / (this.fit * this.k);
    (this.txLabels || []).forEach(function (l) {
      l.setAttribute("transform", "translate(" + l.dataset.x + " " + l.dataset.y + ") scale(" + s + ")");
    });
    var pxPerM = this.fit * this.k;
    (this.invShapes || []).forEach(function (o) {
      // 20% larger than the surveyed footprint so it is comfortable to hit
      var w = Math.max(o.w * 1.2, 8.4 / pxPerM), d = Math.max(o.d * 1.2, 4.2 / pxPerM);
      var pad = 3 / pxPerM;
      o.shape.setAttribute("x", o.cx - w / 2); o.shape.setAttribute("y", o.cy - d / 2);
      o.shape.setAttribute("width", w); o.shape.setAttribute("height", d);
      o.hit.setAttribute("x", o.cx - w / 2 - pad); o.hit.setAttribute("y", o.cy - d / 2 - pad);
      o.hit.setAttribute("width", w + pad * 2); o.hit.setAttribute("height", d + pad * 2);
    });
    this.updatePanels();
  };
  P.clampPan = function () {
    var lx = this.VW * (this.k - 1), ly = this.VH * (this.k - 1);
    this.ox = Math.min(0, Math.max(-lx, this.ox));
    this.oy = Math.min(0, Math.max(-ly, this.oy));
  };
  P.fitAll = function () { this.k = 1; this.ox = 0; this.oy = 0; this.apply(); };
  P.zoomTo = function (nk, cx, cy) {
    nk = Math.max(1, Math.min(60, nk));
    var r = this.svg.getBoundingClientRect();
    cx = cx == null ? r.width / 2 : cx; cy = cy == null ? r.height / 2 : cy;
    var gx = (cx - this.ox * this.fit - (r.width - this.VW * this.fit) / 2) / (this.fit * this.k);
    var gy = (cy - this.oy * this.fit - (r.height - this.VH * this.fit) / 2) / (this.fit * this.k);
    this.ox += gx * (this.k - nk); this.oy += gy * (this.k - nk);
    this.k = nk; this.clampPan(); this.apply();
  };
  P.zoomToSel = function (sel) {
    var ss = this.stringsFor(sel), self = this;
    if (!ss.length) return;
    var x0 = 1e18, y0 = 1e18, x1 = -1e18, y1 = -1e18, seen = {};
    ss.forEach(function (s) {
      var t = self.byTracker[s.tracker];
      if (!t || seen[t.id]) return;
      seen[t.id] = 1;
      x0 = Math.min(x0, self.px(t.x) - t.w / 2); x1 = Math.max(x1, self.px(t.x) + t.w / 2);
      y0 = Math.min(y0, self.py(t.y0)); y1 = Math.max(y1, self.py(t.y1));
    });
    var m = (sel.kind === "string" || sel.kind === "tracker") ? 16 : 26;
    var w = x1 - x0 + m * 2, h = y1 - y0 + m * 2;
    this.k = Math.max(1, Math.min(60, Math.min(this.VW / w, this.VH / h)));
    this.ox = -(x0 - m) * this.k; this.oy = -(y0 - m) * this.k;
    this.clampPan(); this.apply();
  };

  /* ---------------------------------------------------------------- input */
  P.onDown = function (e) {
    if (e.button !== 0) return;
    this.drag = { x: e.clientX, y: e.clientY, ox: this.ox, oy: this.oy, moved: false, hit: e.target };
    this.svg.setPointerCapture(e.pointerId);
  };
  P.onMove = function (e) {
    var d = e.target.dataset || {};
    if (d.panel) {
      var key = d.trk + "#" + d.panel;
      var cached = (this.panelCache || {})[key];
      this.tip.textContent = "Pannello " + d.panel + " · " + d.trk
        + (d.str ? " · " + d.str : "") + (cached ? " · " + cached : "");
      this.tip.classList.add("on");
      this.tip.style.left = Math.min(e.clientX + 14, window.innerWidth - 300) + "px";
      this.tip.style.top = (e.clientY + 14) + "px";
      this.wantSerial(d.trk);
      if (this.drag) { this.panDrag(e); }
      return;
    }
    if (d.str || d.trk || d.inv || d.tx) {
      var txt;
      if (d.str) {
        txt = d.str + " · " + d.trk + " · " + (SEV_LABEL[this.statusOf(d.str)] || "");
      } else if (d.trk) {
        var t = this.byTracker[d.trk];
        txt = d.trk + " · " + (t ? t.modules + " moduli · " + t.strings.length + " stringhe" : "");
      } else txt = (d.inv || d.tx) + (d.inv ? " · inverter" : " · cabina");
      this.tip.textContent = txt;
      this.tip.classList.add("on");
      this.tip.style.left = Math.min(e.clientX + 14, window.innerWidth - 260) + "px";
      this.tip.style.top = (e.clientY + 14) + "px";
    } else this.tip.classList.remove("on");
    if (!this.drag) return;
    this.panDrag(e);
  };

  P.panDrag = function (e) {
    var dx = e.clientX - this.drag.x, dy = e.clientY - this.drag.y;
    if (Math.abs(dx) + Math.abs(dy) > 3) this.drag.moved = true;
    this.ox = this.drag.ox + dx / this.fit; this.oy = this.drag.oy + dy / this.fit;
    this.clampPan(); this.apply();
  };

  /* serials for a tracker, fetched once and reused for every panel on it */
  P.wantSerial = function (trk) {
    if (!trk) return;
    this.panelCache = this.panelCache || {};
    this.panelPending = this.panelPending || {};
    if (this.panelPending[trk]) return;
    this.panelPending[trk] = 1;
    var self = this;
    fetch(API + "/serials?tracker=" + encodeURIComponent(trk), { credentials: "same-origin" })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        if (!j || !j.serials) return;
        j.serials.forEach(function (s2, i) { self.panelCache[trk + "#" + (i + 1)] = s2.serial; });
      })
      .catch(function () {});
  };
  P.onUp = function () {
    var d = this.drag; this.drag = null;
    if (!d || d.moved) return;
    var ds = (d.hit && d.hit.dataset) || {};
    if (ds.panel) {
      this.panelSel = { tracker: ds.trk, n: +ds.panel, string: ds.str };
      if (ds.str) this.select({ kind: "string", id: ds.str }, false);
      return;
    }
    if (ds.str) this.select({ kind: "string", id: ds.str }, false);
    else if (ds.trk) this.select({ kind: "tracker", id: ds.trk }, false);
    else if (ds.inv) this.select({ kind: "inverter", id: ds.inv });
    else if (ds.tx) this.select({ kind: "tx", id: ds.tx });
  };

  window.PlantMapSurveyed = PlantMapSurveyed;
  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("surveyed-map-root");
    if (root) window.__plantMapSurveyed = new PlantMapSurveyed(root);
  });
})();
