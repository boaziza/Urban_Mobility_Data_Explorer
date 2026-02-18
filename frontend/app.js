const API = "https://urban-mobility-data-explorer-hqlv.onrender.com/api";

const state = {
  sort: "pickup_datetime",
  order: "desc",
  charts: {
    hourly: null,
    zones: null
  }
};

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) {
    throw new Error(`HTTP ${r.status} for ${url}`);
  }
  return await r.json();
}

function kpiCard(title, value) {
  return `<div class="card"><h3>${title}</h3><p>${value}</p></div>`;
}

function insightCard(title, stat, detail) {
  return `
    <article class="card insight-card">
      <h3>${title}</h3>
      <p class="insight-stat">${stat}</p>
      <p class="insight-detail">${detail}</p>
    </article>
  `;
}

function formatInt(n) {
  if (typeof n !== "number" || Number.isNaN(n)) return "N/A";
  return n.toLocaleString();
}

function formatPct(x) {
  if (typeof x !== "number" || Number.isNaN(x)) return "N/A";
  return `${x.toFixed(2)}%`;
}

function paymentTypeLabel(id) {
  const map = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
  };
  return map[id] || `Type ${id}`;
}

function queryFromFilters() {
  const params = new URLSearchParams();
  const mappings = [
    ["start_date", "startDate"],
    ["end_date", "endDate"],
    ["borough", "borough"],
    ["payment_type", "paymentType"],
    ["min_distance", "minDistance"],
    ["max_distance", "maxDistance"],
    ["min_fare", "minFare"],
    ["max_fare", "maxFare"]
  ];

  mappings.forEach(([key, id]) => {
    const value = document.getElementById(id).value;
    if (value !== "") params.set(key, value);
  });
  return params.toString();
}

function endpoint(path) {
  const qs = queryFromFilters();
  return qs ? `${API}${path}?${qs}` : `${API}${path}`;
}

async function loadFilterOptions() {
  const data = await getJSON(`${API}/filter-options`);
  const borough = document.getElementById("borough");
  data.boroughs.forEach((b) => {
    const opt = document.createElement("option");
    opt.value = b;
    opt.textContent = b;
    borough.appendChild(opt);
  });

  const payment = document.getElementById("paymentType");
  data.payment_types.forEach((p) => {
    const opt = document.createElement("option");
    opt.value = String(p.id);
    opt.textContent = p.label;
    payment.appendChild(opt);
  });

  document.getElementById("startDate").value = data.min_date || "";
  document.getElementById("endDate").value = data.max_date || "";
}

async function loadKPIs() {
  const s = await getJSON(endpoint("/summary"));
  document.getElementById("kpis").innerHTML =
    kpiCard("Total Trips", s.trips) +
    kpiCard("Total Revenue", `$${s.revenue}`) +
    kpiCard("Average Distance", `${s.avg_distance} mi`) +
    kpiCard("Average Speed", `${s.avg_speed} mph`);
}

async function loadHourlyChart() {
  const rows = await getJSON(endpoint("/hourly-trips"));
  if (state.charts.hourly) state.charts.hourly.destroy();
  state.charts.hourly = new Chart(document.getElementById("hourlyChart"), {
    type: "line",
    data: {
      labels: rows.map((x) => x.hour),
      datasets: [{ label: "Trips by Hour", data: rows.map((x) => x.trips) }]
    }
  });
}

async function loadZoneChart() {
  const rows = await getJSON(`${endpoint("/top-zones")}${endpoint("/top-zones").includes("?") ? "&" : "?"}k=10`);
  if (state.charts.zones) state.charts.zones.destroy();
  state.charts.zones = new Chart(document.getElementById("zonesChart"), {
    type: "bar",
    data: {
      labels: rows.map((x) => `${x.zone} (${x.borough})`),
      datasets: [{ label: "Top Pickup Zones", data: rows.map((x) => x.trips) }]
    }
  });
}

async function loadRoutes() {
  const rows = await getJSON(`${endpoint("/top-routes")}${endpoint("/top-routes").includes("?") ? "&" : "?"}k=10`);
  const tbody = document.querySelector("#routesTable tbody");
  tbody.innerHTML = rows
    .map((r) => `<tr><td>${r.pu_location_id}</td><td>${r.do_location_id}</td><td>${r.trip_count}</td></tr>`)
    .join("");
}

async function loadTrips() {
  const base = endpoint("/trips");
  const joiner = base.includes("?") ? "&" : "?";
  const url = `${base}${joiner}limit=50&offset=0&sort=${state.sort}&order=${state.order}`;
  const rows = await getJSON(url);
  const tbody = document.querySelector("#tripsTable tbody");
  tbody.innerHTML = rows
    .map((r) => {
      return `<tr>
        <td>${r.pickup_datetime}</td>
        <td>${Number(r.trip_distance).toFixed(2)}</td>
        <td>$${Number(r.fare_amount).toFixed(2)}</td>
        <td>$${Number(r.total_amount).toFixed(2)}</td>
        <td>${Number(r.duration_min).toFixed(1)}</td>
        <td>${r.pu_zone} (${r.pu_borough})</td>
        <td>${r.do_zone} (${r.do_borough})</td>
      </tr>`;
    })
    .join("");
}

async function loadInsights() {
  const [insights, summary] = await Promise.all([getJSON(endpoint("/insights")), getJSON(endpoint("/summary"))]);

  const topBoro = insights.top_pickup_borough;
  const peak = insights.peak_hour;
  const tipRows = (insights.tip_behavior_by_payment || []).slice();
  const tripsTotal = Number(summary.trips || 0);

  let boroCard = insightCard("Pickup Demand Concentration", "No data", "Could not compute top pickup borough from current dataset.");
  if (topBoro && typeof topBoro.trips === "number") {
    const share = tripsTotal > 0 ? (100 * topBoro.trips) / tripsTotal : NaN;
    boroCard = insightCard(
      "Pickup Demand Concentration",
      `${topBoro.borough}: ${formatInt(topBoro.trips)} trips`,
      `Using PULocationID mapped to borough, ${topBoro.borough} contributes ${formatPct(share)} of all filtered trips.`
    );
  }

  let peakCard = insightCard("Peak Activity Hour", "No data", "Could not compute peak pickup hour from current dataset.");
  if (peak && typeof peak.trips === "number" && typeof peak.pickup_hour === "number") {
    const share = tripsTotal > 0 ? (100 * peak.trips) / tripsTotal : NaN;
    peakCard = insightCard(
      "Peak Activity Hour",
      `${String(peak.pickup_hour).padStart(2, "0")}:00 with ${formatInt(peak.trips)} trips`,
      `From pickup_hour, this hour accounts for ${formatPct(share)} of the currently filtered trips.`
    );
  }

  let tipCard = insightCard("Tip Behavior by Payment Type", "No data", "Could not compute tip behavior from payment_type and tip_pct.");
  if (tipRows.length > 0) {
    const credit = tipRows.find((r) => r.payment_type === 1);
    const cash = tipRows.find((r) => r.payment_type === 2);
    let detail = tipRows
      .slice(0, 3)
      .map((r) => `${paymentTypeLabel(r.payment_type)}: ${formatPct(r.avg_tip_pct)}`)
      .join(" | ");
    let stat = "Top tip pattern";
    if (credit && cash) {
      const gap = Number(credit.avg_tip_pct) - Number(cash.avg_tip_pct);
      stat = `Credit vs Cash gap: ${formatPct(gap)}`;
      detail = `payment_type shows ${formatPct(credit.avg_tip_pct)} average tip for Credit card versus ${formatPct(cash.avg_tip_pct)} for Cash.`;
    }
    tipCard = insightCard("Tip Behavior by Payment Type", stat, detail);
  }

  document.getElementById("insights").innerHTML = `${boroCard}${peakCard}${tipCard}`;
}

async function refreshDashboard() {
  await loadKPIs();
  await Promise.all([loadHourlyChart(), loadZoneChart(), loadRoutes(), loadTrips(), loadInsights()]);
}

function wireEvents() {
  document.getElementById("applyFilters").addEventListener("click", refreshDashboard);
  document.getElementById("resetFilters").addEventListener("click", () => {
    [
      "startDate",
      "endDate",
      "borough",
      "paymentType",
      "minDistance",
      "maxDistance",
      "minFare",
      "maxFare"
    ].forEach((id) => (document.getElementById(id).value = ""));
    refreshDashboard();
  });

  document.querySelectorAll("#tripsTable th[data-sort]").forEach((th) => {
    th.addEventListener("click", async () => {
      const clicked = th.getAttribute("data-sort");
      if (state.sort === clicked) {
        state.order = state.order === "asc" ? "desc" : "asc";
      } else {
        state.sort = clicked;
        state.order = "desc";
      }
      await loadTrips();
    });
  });
}

async function init() {
  try {
    await loadFilterOptions();
    wireEvents();
    await refreshDashboard();
  } catch (err) {
    console.error(err);
    alert(`Dashboard failed to load: ${err.message}`);
  }
}

init();
