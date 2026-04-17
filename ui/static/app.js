const state = {
  projects: [],
};

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function formatNumber(value) {
  return Intl.NumberFormat("en-US").format(value);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function statCard(label, value) {
  return `
    <article class="stat-card">
      <span>${escapeHtml(label)}</span>
      <strong>${formatNumber(value)}</strong>
    </article>
  `;
}

function renderOverview(data) {
  const stats = Object.entries(data.stats)
    .map(([label, value]) => statCard(label.replaceAll("_", " "), value))
    .join("");
  document.getElementById("stat-grid").innerHTML = stats;
  document.getElementById("db-path").textContent = data.db_path;
  document.getElementById("project-root").textContent = data.project_root;

  state.projects = data.projects;
  const maxCount = Math.max(...data.projects.map((item) => item.count), 1);
  document.getElementById("project-bars").innerHTML = data.projects.map((item) => `
    <div class="bar-row">
      <div class="bar-label">${escapeHtml(item.project_id)}</div>
      <div class="bar-track"><span style="width:${(item.count / maxCount) * 100}%"></span></div>
      <strong>${formatNumber(item.count)}</strong>
    </div>
  `).join("");

  const projectSelect = document.getElementById("event-project");
  projectSelect.innerHTML = `<option value="all">All projects</option>` + data.projects.map((item) => (
    `<option value="${escapeHtml(item.project_id)}">${escapeHtml(item.project_id)}</option>`
  )).join("");

  renderEntityList("intervention-list", data.recent_interventions, "alert");
}

function renderEntityList(targetId, items, tone = "neutral") {
  const target = document.getElementById(targetId);
  if (!items.length) {
    target.innerHTML = `<div class="empty">No items.</div>`;
    return;
  }
  target.innerHTML = items.map((item) => `
    <article class="list-item ${tone}">
      <div class="list-meta">
        <span class="pill">${escapeHtml(item.project_id)}</span>
        <time>${escapeHtml(item.created_at)}</time>
      </div>
      <h3>${escapeHtml(item.summary || item.title)}</h3>
      <p>${escapeHtml(item.details?.message || item.details?.choice_made || item.symptom || "")}</p>
    </article>
  `).join("");
}

function renderEvents(items) {
  const target = document.getElementById("event-list");
  if (!items.length) {
    target.innerHTML = `<div class="empty">No events matched.</div>`;
    return;
  }
  target.innerHTML = items.map((item) => `
    <article class="list-item">
      <div class="list-meta">
        <span class="pill">${escapeHtml(item.project_id)}</span>
        <span class="pill subtle">${escapeHtml(item.event_kind)}</span>
        <time>${escapeHtml(item.created_at)}</time>
      </div>
      <h3>${escapeHtml(item.summary)}</h3>
      <p>${escapeHtml(item.snippet)}</p>
    </article>
  `).join("");
}

function renderCases(items) {
  const target = document.getElementById("case-list");
  if (!items.length) {
    target.innerHTML = `<div class="empty">No cases matched.</div>`;
    return;
  }
  target.innerHTML = items.map((item) => `
    <article class="list-item">
      <div class="list-meta">
        <span class="pill">${escapeHtml(item.project_id)}</span>
        <span class="pill subtle">${escapeHtml(item.case_kind)}</span>
        <time>${escapeHtml(item.created_at)}</time>
      </div>
      <h3>${escapeHtml(item.title)}</h3>
      <p>${escapeHtml(item.symptom)}</p>
    </article>
  `).join("");
}

function renderRecall(query, results) {
  const target = document.getElementById("recall-results");
  if (!results.length) {
    target.innerHTML = `<div class="empty">No results for "${escapeHtml(query)}".</div>`;
    return;
  }
  target.classList.remove("empty");
  target.innerHTML = results.map((item) => `
    <article class="list-item">
      <div class="list-meta">
        <span class="pill">${escapeHtml(item.project_id || "unknown")}</span>
        <span class="pill subtle">${escapeHtml(item.type)}</span>
        <strong>${escapeHtml((item.score ?? 0).toFixed(4))}</strong>
      </div>
      <h3>${escapeHtml(item.title)}</h3>
      <p>${escapeHtml(item.symptom || "")}</p>
    </article>
  `).join("");
}

async function loadEvents() {
  const project = document.getElementById("event-project").value;
  const query = document.getElementById("event-query").value.trim();
  const params = new URLSearchParams({ limit: "80" });
  if (project && project !== "all") params.set("project", project);
  if (query) params.set("q", query);
  const data = await fetchJson(`/api/events?${params.toString()}`);
  renderEvents(data.items);
}

async function loadCases() {
  const kind = document.getElementById("case-kind").value;
  const params = new URLSearchParams({ limit: "40" });
  if (kind && kind !== "all") params.set("kind", kind);
  const data = await fetchJson(`/api/cases?${params.toString()}`);
  renderCases(data.items);
}

async function loadSupplemental() {
  const [decisions, interventions] = await Promise.all([
    fetchJson("/api/decisions?limit=12"),
    fetchJson("/api/interventions?limit=12"),
  ]);
  renderEntityList("decision-list", decisions.items, "decision");
  renderEntityList("intervention-list", interventions.items, "alert");
}

async function boot() {
  const overview = await fetchJson("/api/overview");
  renderOverview(overview);
  renderEvents(overview.recent_events);
  renderCases(overview.recent_cases);
  await loadSupplemental();

  document.getElementById("event-filter").addEventListener("submit", async (event) => {
    event.preventDefault();
    await loadEvents();
  });

  document.getElementById("case-filter").addEventListener("submit", async (event) => {
    event.preventDefault();
    await loadCases();
  });

  document.getElementById("recall-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const query = document.getElementById("recall-input").value.trim();
    if (!query) return;
    const data = await fetchJson(`/api/recall?q=${encodeURIComponent(query)}&limit=6`);
    renderRecall(query, data.results);
  });
}

boot().catch((error) => {
  document.body.innerHTML = `<pre class="fatal">${escapeHtml(error.message)}</pre>`;
});
