/**
 * Soup Mini Experiment Tracker — Live SSE Client & UI State Engine.
 *
 * Uses `fetch` + `ReadableStream` with `X-API-Key` and `Last-Event-ID` headers
 * so API keys are never exposed in URL query strings or server access logs.
 * Deduplicates by `(step, seq)`, detects sequence gaps, and sorts by `step ASC`.
 */
(function (root) {
  "use strict";

  const globalWin = typeof window !== "undefined" ? window : root;

  function createInitialState(runId = null) {
    return {
      runId: runId,
      connected: false,
      authRevoked: false,
      reconnectCount: 0,
      lastSeq: 0,
      seqGapsDetected: 0,
      overflowEvents: 0,
      metricsByStep: {},
      orderedMetrics: [],
      renderedRows: [],
    };
  }

  globalWin.__trackerState = createInitialState();

  class SoupTrackerClient {
    constructor(options = {}) {
      this.baseUrl = (options.baseUrl || "").replace(/\/$/, "");
      this.apiKey = options.apiKey || "";
      this.reconnectDelayMs = options.reconnectDelayMs || 120;
      this.state = createInitialState();
      globalWin.__trackerState = this.state;
      this._abortController = null;
      this._stopped = true;
      this._generation = 0;
    }

    setApiKey(apiKey) {
      this.apiKey = (apiKey || "").trim();
      if (typeof localStorage !== "undefined") {
        try {
          if (this.apiKey) {
            localStorage.setItem("soup_api_key", this.apiKey);
          }
        } catch (_) {}
      }
    }

    async fetchRuns() {
      const resp = await fetch(this.baseUrl + "/runs", {
        headers: { "X-API-Key": this.apiKey },
      });
      if (!resp.ok) {
        throw new Error("Failed to fetch runs: HTTP " + resp.status);
      }
      const data = await resp.json();
      this._renderRunList(data.runs || []);
      return data.runs || [];
    }

    applyMetricEvent(metric) {
      const seq = Number(metric.seq);
      const step = Number(metric.step);

      if (seq > 0) {
        if (seq > this.state.lastSeq + 1) {
          this.state.seqGapsDetected += seq - (this.state.lastSeq + 1);
        }
        if (seq > this.state.lastSeq) {
          this.state.lastSeq = seq;
        }
      }

      this.state.metricsByStep[step] = {
        step: step,
        seq: seq,
        loss: Number(metric.loss),
        lr: metric.lr !== null && metric.lr !== undefined ? Number(metric.lr) : null,
        grad_norm:
          metric.grad_norm !== null && metric.grad_norm !== undefined
            ? Number(metric.grad_norm)
            : null,
        custom_metrics: metric.custom_metrics || {},
        client_ts: metric.client_ts || null,
      };

      this.state.orderedMetrics = Object.values(this.state.metricsByStep).sort(
        (a, b) => a.step - b.step
      );
      this._renderRunState();
    }

    stopStream() {
      this._generation += 1;
      this._stopped = true;
      this.state.connected = false;
      if (this._abortController) {
        try {
          this._abortController.abort();
        } catch (_) {}
        this._abortController = null;
      }
      this._updateStatusBadge("disconnected", "Disconnected");
    }

    async startStream(runId, options = {}) {
      this.stopStream();
      const currentGen = this._generation;
      this._stopped = false;
      if (this.state.runId !== runId || options.resetState) {
        this.state = createInitialState(runId);
        globalWin.__trackerState = this.state;
      } else {
        this.state.runId = runId;
      }
      this._renderRunState();

      let isFirstAttempt = true;
      while (!this._stopped && !this.state.authRevoked && this._generation === currentGen) {
        if (!isFirstAttempt) {
          this.state.reconnectCount += 1;
          this._updateStatusBadge("reconnecting", "Reconnecting...");
          this._renderRunState();
          await new Promise((r) => setTimeout(r, this.reconnectDelayMs));
          if (this._stopped || this._generation !== currentGen) break;
        }
        isFirstAttempt = false;

        this._abortController = new AbortController();
        const querySuffix = options.queueSize ? "?queue_size=" + options.queueSize : "";
        const url = this.baseUrl + "/runs/" + encodeURIComponent(runId) + "/stream" + querySuffix;

        try {
          const resp = await fetch(url, {
            method: "GET",
            headers: {
              "X-API-Key": this.apiKey,
              "Last-Event-ID": String(this.state.lastSeq),
              Accept: "text/event-stream",
            },
            signal: this._abortController.signal,
          });

          if (this._generation !== currentGen) break;

          if (resp.status === 401 || resp.status === 403 || resp.status === 404) {
            this.state.authRevoked = resp.status === 401;
            this.state.connected = false;
            this._updateStatusBadge("disconnected", "Auth/Run Error (" + resp.status + ")");
            break;
          }

          if (!resp.ok || !resp.body) {
            this.state.connected = false;
            continue;
          }

          this.state.connected = true;
          this._updateStatusBadge("connected", "Live (SSE)");
          this._renderRunState();

          const reader = resp.body.getReader();
          const decoder = new TextDecoder("utf-8");
          let buffer = "";

          while (!this._stopped && this._generation === currentGen) {
            const { value, done } = await reader.read();
            if (done || this._generation !== currentGen) break;
            buffer += decoder.decode(value, { stream: true });

            let sepIdx;
            while ((sepIdx = buffer.indexOf("\n\n")) !== -1) {
              const rawBlock = buffer.slice(0, sepIdx);
              buffer = buffer.slice(sepIdx + 2);
              this._processSseFrame(rawBlock);
              if (this.state.authRevoked) {
                this.stopStream();
                return;
              }
            }
          }
        } catch (err) {
          if (this._stopped || this._generation !== currentGen) break;
        } finally {
          if (this._generation === currentGen) {
            this.state.connected = false;
          }
        }
      }
    }

    _processSseFrame(rawBlock) {
      const lines = rawBlock.split(/\r?\n/);
      let eventType = "message";
      let eventId = null;
      const dataLines = [];

      for (const line of lines) {
        if (!line || line.startsWith(":")) continue;
        if (line.startsWith("event:")) {
          eventType = line.slice(6).trim();
        } else if (line.startsWith("id:")) {
          eventId = line.slice(3).trim();
        } else if (line.startsWith("data:")) {
          dataLines.push(line.slice(5).trim());
        }
      }

      if (dataLines.length === 0) return;
      const rawData = dataLines.join("\n");
      let parsed;
      try {
        parsed = JSON.parse(rawData);
      } catch (_) {
        return;
      }

      if (eventType === "metric") {
        if (eventId !== null && parsed.seq === undefined) {
          parsed.seq = Number(eventId);
        }
        this.applyMetricEvent(parsed);
      } else if (eventType === "overflow") {
        this.state.overflowEvents += 1;
      } else if (eventType === "error") {
        if (parsed && parsed.reason === "api_key_revoked") {
          this.state.authRevoked = true;
          this.state.connected = false;
          this._updateStatusBadge("disconnected", "API Key Revoked");
        }
      }
    }

    _updateStatusBadge(statusClass, text) {
      if (typeof document === "undefined") return;
      const el = document.getElementById("stream-status");
      if (el) {
        el.className = "badge " + statusClass;
        el.textContent = text;
      }
    }

    _renderRunList(runs) {
      if (typeof document === "undefined") return;
      const listEl = document.getElementById("run-list");
      if (!listEl) return;
      listEl.innerHTML = "";
      for (const r of runs) {
        const li = document.createElement("li");
        li.className = "run-item" + (this.state.runId === r.run_id ? " active" : "");
        li.innerHTML =
          "<div>" +
          "<div class='run-title'>" + r.name + "</div>" +
          "<div class='run-sub'>" + r.run_id.slice(0, 8) + "…</div>" +
          "</div>" +
          "<span class='badge'>" + r.last_seq + " pts</span>";
        li.addEventListener("click", () => {
          const titleEl = document.getElementById("active-run-title");
          if (titleEl) titleEl.textContent = r.name;
          const items = listEl.querySelectorAll(".run-item");
          items.forEach((it) => it.classList.remove("active"));
          li.classList.add("active");
          this.startStream(r.run_id, { resetState: true });
        });
        listEl.appendChild(li);
      }
    }

    _renderRunState() {
      this.state.renderedRows = this.state.orderedMetrics.map((m) => ({
        step: m.step,
        seq: m.seq,
        loss: m.loss,
        lr: m.lr,
        grad_norm: m.grad_norm,
      }));

      if (typeof document === "undefined") return;

      const setText = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = String(val);
      };

      setText("meta-run-id", this.state.runId || "—");
      setText("meta-last-seq", this.state.lastSeq);
      setText("meta-step-count", this.state.orderedMetrics.length);
      setText("meta-reconnects", this.state.reconnectCount);
      setText("meta-seq-gaps", this.state.seqGapsDetected);

      const tbody = document.getElementById("metrics-tbody");
      if (tbody) {
        tbody.innerHTML = this.state.orderedMetrics
          .map(
            (m) =>
              `<tr data-step="${m.step}" data-seq="${m.seq}">` +
              `<td>${m.step}</td>` +
              `<td>${m.seq}</td>` +
              `<td>${m.loss.toFixed(6)}</td>` +
              `<td>${m.lr !== null ? m.lr.toExponential(2) : "—"}</td>` +
              `<td>${m.grad_norm !== null ? m.grad_norm.toFixed(4) : "—"}</td>` +
              `</tr>`
          )
          .join("");
      }

      const svg = document.getElementById("loss-chart");
      if (svg) {
        const pts = this.state.orderedMetrics;
        const totalW = (svg.clientWidth && svg.clientWidth > 100) ? svg.clientWidth : 760;
        const totalH = (svg.clientHeight && svg.clientHeight > 80) ? svg.clientHeight : 240;

        if (typeof svg.setAttribute === "function") {
          svg.setAttribute("viewBox", `0 0 ${totalW} ${totalH}`);
        }
        if (typeof svg.removeAttribute === "function") {
          svg.removeAttribute("preserveAspectRatio");
        }

        if (pts.length === 0) {
          svg.innerHTML = `
            <text x="${totalW / 2}" y="${totalH / 2}" fill="#8b949e" font-size="12" text-anchor="middle" font-family="ui-monospace, monospace">
              No metrics in this run yet. Click "Send 10 Steps" above.
            </text>
          `;
          return;
        }

        const minStep = pts[0].step;
        const maxStep = Math.max(pts[pts.length - 1].step, minStep + 1);
        const losses = pts.map((p) => p.loss);
        const minLoss = Math.min(...losses);
        const maxLoss = Math.max(...losses, minLoss + 1e-4);

        const padLeft = 56;
        const padRight = 24;
        const padTop = 28;
        const padBottom = 28;
        const plotW = Math.max(50, totalW - padLeft - padRight);
        const plotH = Math.max(40, totalH - padTop - padBottom);

        const coords = pts
          .map((p) => {
            const x = padLeft + ((p.step - minStep) / (maxStep - minStep)) * plotW;
            const y = padTop + (1 - (p.loss - minLoss) / (maxLoss - minLoss)) * plotH;
            return `${x.toFixed(1)},${y.toFixed(1)}`;
          })
          .join(" ");

        const firstPt = `${padLeft},${padTop + plotH}`;
        const lastPt = `${padLeft + plotW},${padTop + plotH}`;
        const polyArea = `${firstPt} ${coords} ${lastPt}`;
        const latestVal = pts[pts.length - 1].loss.toFixed(4);

        svg.innerHTML = `
          <!-- Grid lines -->
          <line x1="${padLeft}" y1="${padTop}" x2="${padLeft + plotW}" y2="${padTop}" stroke="#21262d" stroke-dasharray="3,3" />
          <line x1="${padLeft}" y1="${padTop + plotH / 2}" x2="${padLeft + plotW}" y2="${padTop + plotH / 2}" stroke="#21262d" stroke-dasharray="3,3" />
          <line x1="${padLeft}" y1="${padTop + plotH}" x2="${padLeft + plotW}" y2="${padTop + plotH}" stroke="#30363d" />

          <!-- Shaded Area -->
          <polygon fill="rgba(88, 166, 255, 0.08)" points="${polyArea}" />

          <!-- Polyline -->
          <polyline fill="none" stroke="#58a6ff" stroke-width="2" points="${coords}" />

          <!-- Axis Labels -->
          <text x="${padLeft - 10}" y="${padTop + 4}" fill="#8b949e" font-size="11" text-anchor="end" font-family="ui-monospace, monospace">${maxLoss.toFixed(3)}</text>
          <text x="${padLeft - 10}" y="${padTop + plotH + 3}" fill="#8b949e" font-size="11" text-anchor="end" font-family="ui-monospace, monospace">${minLoss.toFixed(3)}</text>
          <text x="${padLeft}" y="${padTop + plotH + 18}" fill="#8b949e" font-size="11" font-family="ui-monospace, monospace">step ${minStep}</text>
          <text x="${padLeft + plotW}" y="${padTop + plotH + 18}" fill="#8b949e" font-size="11" text-anchor="end" font-family="ui-monospace, monospace">step ${pts[pts.length - 1].step}</text>

          <!-- Metric Header -->
          <text x="${padLeft + plotW}" y="${padTop - 10}" fill="#58a6ff" font-size="12" font-weight="600" text-anchor="end" font-family="ui-monospace, monospace">Latest Loss: ${latestVal}</text>
        `;
      }
    }
  }

  globalWin.SoupTrackerClient = SoupTrackerClient;

  if (typeof document !== "undefined") {
    document.addEventListener("DOMContentLoaded", () => {
      const client = new SoupTrackerClient();
      globalWin.__trackerClient = client;

      const keyInput = document.getElementById("api-key-input");
      const btnLoad = document.getElementById("btn-load-runs");
      const btnBootstrap = document.getElementById("btn-create-project");
      const btnNewRun = document.getElementById("btn-new-run");
      const btnSendMetrics = document.getElementById("btn-send-metrics");
      const btnLiveStream = document.getElementById("btn-live-stream");

      try {
        const savedKey = localStorage.getItem("soup_api_key");
        if (savedKey && keyInput) {
          keyInput.value = savedKey;
          client.setApiKey(savedKey);
        }
      } catch (_) {}

      let liveFeedInterval = null;

      if (keyInput) {
        keyInput.addEventListener("input", () => {
          client.setApiKey(keyInput.value);
        });
      }

      if (btnLoad) {
        btnLoad.addEventListener("click", async () => {
          client.setApiKey(keyInput ? keyInput.value : "");
          try {
            const runs = await client.fetchRuns();
            if (runs.length > 0 && !client.state.runId) {
              const titleEl = document.getElementById("active-run-title");
              if (titleEl) titleEl.textContent = runs[0].name;
              client.startStream(runs[0].run_id, { resetState: true });
            }
          } catch (err) {
            alert(err.message);
          }
        });
      }

      async function sendBatch(runId, count = 10) {
        if (!client.apiKey || !runId) return;
        const currentSteps = client.state.orderedMetrics.length;
        const startStep = currentSteps > 0 ? client.state.orderedMetrics[currentSteps - 1].step + 1 : 0;
        const batch = [];
        for (let i = 0; i < count; i++) {
          const step = startStep + i;
          const loss = 2.4 * Math.exp(-0.06 * step) + 0.35 + (Math.random() * 0.08 - 0.04);
          batch.push({
            step: step,
            loss: Math.max(0.05, loss),
            lr: 1e-4 * Math.pow(0.99, step),
            grad_norm: 0.4 + Math.random() * 0.2,
          });
        }
        await fetch("/runs/" + encodeURIComponent(runId) + "/metrics", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-API-Key": client.apiKey,
          },
          body: JSON.stringify({
            batch_id: "batch-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2, 6),
            metrics: batch,
          }),
        });
        await client.fetchRuns();
      }

      if (btnBootstrap) {
        btnBootstrap.addEventListener("click", async () => {
          btnBootstrap.disabled = true;
          btnBootstrap.textContent = "Bootstrapping...";
          try {
            const resp = await fetch("/projects", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ name: "demo-project" }),
            });
            const proj = await resp.json();
            if (keyInput) keyInput.value = proj.api_key;
            client.setApiKey(proj.api_key);

            const runResp = await fetch("/runs", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "X-API-Key": proj.api_key,
              },
              body: JSON.stringify({ name: "llama3-8b-sft", config: { lr: 1e-4, batch_size: 16 } }),
            });
            const run = await runResp.json();

            const initialMetrics = [];
            for (let s = 0; s < 15; s++) {
              const loss = 2.4 * Math.exp(-0.12 * s) + 0.45 + (Math.random() * 0.06 - 0.03);
              initialMetrics.push({
                step: s,
                loss: Math.max(0.1, loss),
                lr: 1e-4 * Math.pow(0.98, s),
                grad_norm: 0.5 + Math.random() * 0.25,
              });
            }
            await fetch("/runs/" + run.run_id + "/metrics", {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "X-API-Key": proj.api_key,
              },
              body: JSON.stringify({ batch_id: "init-demo-batch", metrics: initialMetrics }),
            });

            await client.fetchRuns();
            const titleEl = document.getElementById("active-run-title");
            if (titleEl) titleEl.textContent = "llama3-8b-sft";
            client.startStream(run.run_id, { resetState: true });
          } catch (err) {
            alert("Bootstrap error: " + err.message);
          } finally {
            btnBootstrap.disabled = false;
            btnBootstrap.textContent = "Bootstrap Demo Project";
          }
        });
      }

      if (btnNewRun) {
        btnNewRun.addEventListener("click", async () => {
          if (!client.apiKey) {
            alert("Please paste or generate an API key first.");
            return;
          }
          const runResp = await fetch("/runs", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-API-Key": client.apiKey,
            },
            body: JSON.stringify({ name: "run-" + Date.now().toString(36) }),
          });
          const run = await runResp.json();
          await client.fetchRuns();
          const titleEl = document.getElementById("active-run-title");
          if (titleEl) titleEl.textContent = run.name;
          client.startStream(run.run_id, { resetState: true });
        });
      }

      if (btnSendMetrics) {
        btnSendMetrics.addEventListener("click", async () => {
          if (!client.state.runId) {
            alert("Select or create a run first!");
            return;
          }
          btnSendMetrics.disabled = true;
          try {
            await sendBatch(client.state.runId, 10);
          } finally {
            btnSendMetrics.disabled = false;
          }
        });
      }

      if (btnLiveStream) {
        btnLiveStream.addEventListener("click", () => {
          if (!client.state.runId) {
            alert("Select or create a run first!");
            return;
          }
          if (liveFeedInterval) {
            clearInterval(liveFeedInterval);
            liveFeedInterval = null;
            btnLiveStream.textContent = "Live Feed: Off";
            btnLiveStream.style.background = "#21262d";
          } else {
            btnLiveStream.textContent = "Live Feed: On";
            btnLiveStream.style.background = "#b62324";
            liveFeedInterval = setInterval(async () => {
              if (!client.state.runId) return;
              await sendBatch(client.state.runId, 1);
            }, 750);
          }
        });
      }

      window.addEventListener("resize", () => {
        if (client.state.runId) {
          client._renderRunState();
        }
      });
    });
  }
})(typeof globalThis !== "undefined" ? globalThis : this);
