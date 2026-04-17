const metricsEl = document.getElementById("metrics");
const projectsEl = document.getElementById("projects");
const sessionsEl = document.getElementById("sessions");
const recallResultsEl = document.getElementById("recall-results");
const projectFilterEl = document.getElementById("project-filter");
const eventSearchEl = document.getElementById("event-search");
const eventsEl = document.getElementById("events");
const casesEl = document.getElementById("cases");
const decisionsEl = document.getElementById("decisions");
const interventionsEl = document.getElementById("interventions");

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function createItem(title, meta, body) {
  const article = document.createElement("article");
  article.className = "item";
  article.innerHTML = `
    <div class="item-meta">${meta}</div>
    <h3>${title}</h3>
    <p>${body}</p>
  `;
  return article;
}

function renderMetrics(counts) {
  const labels = [
    ["events", "Events"],
    ["unknown_events", "Unknown"],
    ["decisions", "Decisions"],
    ["interventions", "Alerts"],
    ["sessions", "Sessions"],
    ["cases", "Cases"],
    ["embeddings", "Embeddings"],
    ["heuristics", "Heuristics"],
  ];
  metricsEl.innerHTML = "";
  for (const [key, label] of labels) {
    const card = document.createElement("article");
    card.className = "metric";
    card.innerHTML = `<span>${label}</span><strong>${counts[key] ?? 0}</strong>`;
    metricsEl.appendChild(card);
  }
}

function renderProjects(projects) {
  projectsEl.innerHTML = "";
  projects.forEach((project) => {
    const row = document.createElement("div");
    row.className = "bar-row";
    const width = Math.max(8, Math.min(100, project.count));
    row.innerHTML = `
      <div class="bar-label">
        <span>${project.project_id}</span>
        <strong>${project.count}</strong>
      </div>
      <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
    `;
    projectsEl.appendChild(row);
  });
}

function renderSessions(items) {
  sessionsEl.innerHTML = "";
  items.forEach((item) => {
    sessionsEl.appendChild(
      createItem(
        item.project_id,
        `load ${(item.context_load_score * 100).toFixed(0)}% | frag ${(item.fragmentation_score * 100).toFixed(0)}% | switches ${item.tool_switch_count}`,
        item.summary,
      ),
    );
  });
}

function renderRecall(results) {
  recallResultsEl.innerHTML = "";
  if (!results.length) {
    recallResultsEl.textContent = "No recall results yet.";
    return;
  }
  results.forEach((result) => {
    recallResultsEl.appendChild(
      createItem(
        result.title || result.id,
        `${result.project_id || "unknown"} | ${result.type} | ${(result.score * 100).toFixed(1)}%`,
        result.symptom || result.future_pattern || (result.reusable_fix || []).join(" -> ") || "No detail available.",
      ),
    );
  });
}

function renderCollection(target, items, mapper) {
  target.innerHTML = "";
  if (!items.length) {
    target.innerHTML = `<p class="muted">No items found for this filter.</p>`;
    return;
  }
  items.forEach((item) => target.appendChild(mapper(item)));
}

function eventCard(item) {
  return createItem(
    item.summary,
    `${item.project_id} | ${item.event_kind} | ${item.created_at}`,
    item.raw_content || "No raw content captured.",
  );
}

function caseCard(item) {
  const fix = item.reusable_fix?.length ? item.reusable_fix.join(" -> ") : item.future_pattern || item.symptom || "No fix path yet.";
  return createItem(
    item.title,
    `${item.project_id} | ${item.case_kind} | recur ${item.recurrence} | conf ${Number(item.confidence || 0).toFixed(2)}`,
    fix,
  );
}

function decisionCard(item) {
  return createItem(
    item.choice_made || item.summary,
    `${item.project_id} | ${item.decision_kind} | ${item.created_at}`,
    item.context || "No context captured.",
  );
}

function interventionCard(item) {
  return createItem(
    item.summary,
    `${item.project_id} | collapse ${(item.collapse_score * 100).toFixed(0)}% | ${item.created_at}`,
    item.suggested_action || item.message || "No action captured.",
  );
}

async function loadProjects() {
  const projects = await fetchJson("/api/projects");
  projectFilterEl.innerHTML = `
    <option value="">All projects</option>
    <option value="unknown">unknown</option>
  `;
  projects.forEach((project) => {
    const option = document.createElement("option");
    option.value = project.id;
    option.textContent = project.name;
    projectFilterEl.appendChild(option);
  });
}

async function loadOverview() {
  const overview = await fetchJson("/api/overview");
  renderMetrics(overview.counts);
  renderProjects(overview.projects);
  renderSessions(overview.hottest_sessions);
}

async function loadCollections() {
  const params = new URLSearchParams();
  if (projectFilterEl.value) {
    params.set("project", projectFilterEl.value);
  }
  if (eventSearchEl.value.trim()) {
    params.set("q", eventSearchEl.value.trim());
  }
  const suffix = params.toString() ? `?${params.toString()}` : "";

  const [events, cases, decisions, interventions] = await Promise.all([
    fetchJson(`/api/events${suffix}`),
    fetchJson(`/api/cases${suffix}`),
    fetchJson(`/api/decisions${suffix}`),
    fetchJson(`/api/interventions${suffix}`),
  ]);

  renderCollection(eventsEl, events, eventCard);
  renderCollection(casesEl, cases, caseCard);
  renderCollection(decisionsEl, decisions, decisionCard);
  renderCollection(interventionsEl, interventions, interventionCard);
}

async function runRecall(query) {
  const payload = await fetchJson(`/api/recall?q=${encodeURIComponent(query)}`);
  renderRecall(payload.results || []);
}

document.getElementById("recall-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const query = document.getElementById("recall-query").value.trim();
  if (!query) {
    return;
  }
  await runRecall(query);
});

document.getElementById("refresh-data").addEventListener("click", loadCollections);
projectFilterEl.addEventListener("change", loadCollections);

async function boot() {
  await loadProjects();
  await loadOverview();
  await loadCollections();
  await runRecall("IronClad revenue blocked");
}

boot().catch((error) => {
  recallResultsEl.textContent = error.message;
});
