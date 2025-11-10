import {
  getState,
  subscribeState,
  updateSlice,
  type AppState,
} from "@scripts/core/state";
import { buildMediumTableHTML } from "@scripts/features/shared/mediumTable";
import { escapeHtml } from "@scripts/core/utils";
import { renderCalculationSnapshot } from "@scripts/features/shared/calculationSnapshot";
import { initVirtualList } from "@scripts/core/virtualList";
import { printHtml } from "@scripts/core/print";
import { printEntriesChunked } from "@scripts/features/shared/printing";
import { getDatabaseSnapshot } from "@scripts/core/database";
import { getActiveDriverKey, saveDatabase } from "@scripts/core/storage";
import type { CalculationItem } from "@scripts/features/calculation";

interface Services {
  state: {
    getState: typeof getState;
    subscribe: typeof subscribeState;
    updateSlice: typeof updateSlice;
  };
}

interface HistoryEntry {
  datum?: string;
  date?: string;
  ersteller?: string;
  standort?: string;
  kultur?: string;
  kisten?: number;
  items: CalculationItem[];
  savedAt?: string;
  [key: string]: unknown;
}

const USE_VIRTUAL_SCROLLING = true;
const INITIAL_LOAD_LIMIT = 200;

let initialized = false;
let virtualListInstance: ReturnType<typeof initVirtualList> | null = null;
const selectedIndexes = new Set<number>();

async function persistHistoryChanges(): Promise<void> {
  const driverKey = getActiveDriverKey();
  if (!driverKey || driverKey === "memory") {
    return;
  }
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
  } catch (err) {
    console.error("Fehler beim Persistieren der Historie", err);
    window.alert(
      "Historie konnte nicht dauerhaft gespeichert werden. Bitte erneut versuchen."
    );
  }
}

function createSection(): HTMLElement {
  const wrapper = document.createElement("div");
  wrapper.className = "section-inner";
  wrapper.innerHTML = `
    <h2 class="text-center mb-4">Historie – Frühere Einträge</h2>
    <div class="card card-dark">
      <div class="card-header d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 no-print">
        <div class="small text-muted" data-role="selection-info">Keine Einträge ausgewählt.</div>
        <button class="btn btn-outline-light btn-sm" data-action="print-selected" disabled>Ausgewählte drucken</button>
      </div>
      <div class="card-body">
        <div data-role="history-list" class="history-list"></div>
      </div>
    </div>
    <div class="card card-dark mt-4 d-none" id="history-detail">
      <div class="card-header bg-info text-white">
        <h5 class="mb-0">Details</h5>
      </div>
      <div class="card-body" id="history-detail-body"></div>
    </div>
  `;
  return wrapper;
}

function updateCardSelection(
  listContainer: Element | null,
  index: number,
  selected: boolean
): void {
  const card = listContainer?.querySelector<HTMLElement>(
    `.calc-snapshot-card[data-index="${index}"]`
  );
  if (!card) {
    return;
  }
  card.classList.toggle("calc-snapshot-card--selected", selected);
  const checkbox = card.querySelector<HTMLInputElement>(
    '[data-action="toggle-select"]'
  );
  if (checkbox) {
    checkbox.checked = selected;
  }
}

function renderCardsList(
  state: AppState,
  listContainer: HTMLElement,
  labels: AppState["fieldLabels"]
): void {
  const entries: HistoryEntry[] = state.history || [];
  const resolvedLabels = labels || getState().fieldLabels;

  for (const idx of Array.from(selectedIndexes)) {
    if (!entries[idx]) {
      selectedIndexes.delete(idx);
    }
  }

  if (USE_VIRTUAL_SCROLLING && entries.length > INITIAL_LOAD_LIMIT) {
    if (!virtualListInstance) {
      virtualListInstance = initVirtualList(listContainer, {
        itemCount: entries.length,
        estimatedItemHeight: 250,
        overscan: 6,
        renderItem: (node, index) => {
          const entry = entries[index];
          const selected = selectedIndexes.has(index);
          node.innerHTML = renderCalculationSnapshot(entry, resolvedLabels, {
            showActions: true,
            includeCheckbox: true,
            index,
            selected,
          });
        },
      });
    } else {
      virtualListInstance.updateItemCount(entries.length);
    }
  } else {
    if (virtualListInstance) {
      virtualListInstance.destroy();
      virtualListInstance = null;
    }

    const limit = Math.min(entries.length, INITIAL_LOAD_LIMIT);
    const fragment = document.createDocumentFragment();

    for (let i = 0; i < limit; i += 1) {
      const entry = entries[i];
      const selected = selectedIndexes.has(i);
      const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
        showActions: true,
        includeCheckbox: true,
        index: i,
        selected,
      });
      const wrapper = document.createElement("div");
      wrapper.innerHTML = cardHtml;
      if (wrapper.firstElementChild) {
        fragment.appendChild(wrapper.firstElementChild);
      }
    }

    listContainer.innerHTML = "";
    listContainer.appendChild(fragment);

    if (entries.length > limit) {
      const loadMoreBtn = document.createElement("button");
      loadMoreBtn.className = "btn btn-secondary w-100 mt-3";
      loadMoreBtn.textContent = `Mehr laden (${entries.length - limit} weitere)`;
      loadMoreBtn.dataset.action = "load-more";
      loadMoreBtn.dataset.currentLimit = String(limit);
      listContainer.appendChild(loadMoreBtn);
    }
  }
}

function renderHistoryTable(section: HTMLElement, state: AppState): void {
  const listContainer = section.querySelector<HTMLElement>(
    '[data-role="history-list"]'
  );
  if (!listContainer) {
    return;
  }
  renderCardsList(state, listContainer, state.fieldLabels);
}

function renderDetail(
  entry: HistoryEntry | null,
  section: HTMLElement,
  index: number | null,
  labels: AppState["fieldLabels"]
): void {
  const detailCard = section.querySelector<HTMLElement>("#history-detail");
  const detailBody = section.querySelector<HTMLElement>("#history-detail-body");
  if (!detailCard || !detailBody) {
    return;
  }

  if (!entry) {
    detailCard.classList.add("d-none");
    detailBody.innerHTML = "";
    detailCard.dataset.index = "";
    return;
  }

  detailCard.dataset.index = index !== null ? String(index) : "";
  const resolvedLabels = labels || getState().fieldLabels;
  const tableLabels = resolvedLabels.history?.tableColumns ?? {};
  const detailLabels = resolvedLabels.history?.detail ?? {};
  const snapshotTable = buildMediumTableHTML(
    entry.items,
    resolvedLabels,
    "detail"
  );

  detailBody.innerHTML = `
    <p>
      <strong>${escapeHtml(tableLabels.date || "Datum")}:</strong> ${escapeHtml(entry.datum || entry.date || "")}<br />
      <strong>${escapeHtml(detailLabels.creator || "Erstellt von")}:</strong> ${escapeHtml(entry.ersteller || "")}<br />
      <strong>${escapeHtml(detailLabels.location || "Standort")}:</strong> ${escapeHtml(entry.standort || "")}<br />
      <strong>${escapeHtml(detailLabels.crop || "Kultur")}:</strong> ${escapeHtml(entry.kultur || "")}<br />
      <strong>${escapeHtml(detailLabels.quantity || "Kisten")}:</strong> ${escapeHtml(
        entry.kisten !== undefined && entry.kisten !== null
          ? String(entry.kisten)
          : ""
      )}
    </p>
    <div class="table-responsive">
      ${snapshotTable}
    </div>
    <button class="btn btn-outline-secondary no-print" data-action="detail-print">Drucken / PDF</button>
  `;
  detailCard.classList.remove("d-none");
}

const HISTORY_SUMMARY_STYLES = `
  .history-summary {
    margin-top: 1.5rem;
  }
  .history-summary table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .history-summary th,
  .history-summary td {
    border: 1px solid #555;
    padding: 6px 8px;
    vertical-align: top;
  }
  .history-summary th {
    background: #f2f2f2;
  }
  .history-summary td div + div {
    margin-top: 0.25rem;
  }
  .history-detail h2 {
    margin-top: 1.5rem;
  }
  .history-detail table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .history-detail th,
  .history-detail td {
    border: 1px solid #555;
    padding: 6px 8px;
    text-align: left;
  }
  .nowrap {
    white-space: nowrap;
  }
`;

function buildCompanyHeader(company: AppState["company"]): string {
  const hasContent = Boolean(
    company?.name ||
      company?.headline ||
      company?.address ||
      company?.contactEmail
  );
  if (!hasContent) {
    return "";
  }
  const address = company?.address
    ? escapeHtml(company.address).replace(/\n/g, "<br />")
    : "";
  const email = company?.contactEmail
    ? `<p>${escapeHtml(company.contactEmail)}</p>`
    : "";
  return `
    <div class="print-meta">
      ${company?.name ? `<h1>${escapeHtml(company.name)}</h1>` : ""}
      ${company?.headline ? `<p>${escapeHtml(company.headline)}</p>` : ""}
      ${address ? `<p>${address}</p>` : ""}
      ${email}
    </div>
  `;
}

async function printSummary(
  entries: HistoryEntry[],
  labels: AppState["fieldLabels"]
): Promise<void> {
  if (!entries.length) {
    window.alert("Keine Einträge zum Drucken ausgewählt.");
    return;
  }
  const company = getState().company || {};
  const headerHtml = buildCompanyHeader(company);
  try {
    await printEntriesChunked(entries, labels, {
      title: "Historie – Übersicht",
      headerHtml,
      chunkSize: 50,
    });
  } catch (err) {
    console.error("Printing failed", err);
    window.alert("Fehler beim Drucken. Bitte erneut versuchen.");
  }
}

function printDetail(
  entry: HistoryEntry | null,
  labels: AppState["fieldLabels"]
): void {
  if (!entry) {
    window.alert("Kein Eintrag zum Drucken vorhanden.");
    return;
  }
  const company = getState().company || {};
  const resolvedLabels = labels || getState().fieldLabels;
  const detailLabels = resolvedLabels.history?.detail ?? {};
  const snapshotTable = buildMediumTableHTML(
    entry.items || [],
    resolvedLabels,
    "detail",
    {
      classes: "history-detail-table",
    }
  );

  const content = `${buildCompanyHeader(company)}
    <section class="history-detail">
      <h2>${escapeHtml(detailLabels.title || "Historieneintrag")} – ${escapeHtml(
        entry.datum || entry.date || ""
      )}</h2>
      <p>
        <strong>${escapeHtml(detailLabels.creator || "Erstellt von")}:</strong> ${escapeHtml(
          entry.ersteller || ""
        )}<br />
        <strong>${escapeHtml(detailLabels.location || "Standort")}:</strong> ${escapeHtml(
          entry.standort || ""
        )}<br />
        <strong>${escapeHtml(detailLabels.crop || "Kultur")}:</strong> ${escapeHtml(
          entry.kultur || ""
        )}<br />
        <strong>${escapeHtml(detailLabels.quantity || "Kisten")}:</strong> ${escapeHtml(
          entry.kisten !== undefined && entry.kisten !== null
            ? String(entry.kisten)
            : ""
        )}
      </p>
      ${snapshotTable}
    </section>
  `;

  printHtml({
    title: `Historie – ${entry.datum || entry.date || ""}`,
    styles: HISTORY_SUMMARY_STYLES,
    content,
  });
}

function updateSelectionUI(section: HTMLElement): void {
  const info = section.querySelector<HTMLElement>(
    '[data-role="selection-info"]'
  );
  const printButton = section.querySelector<HTMLButtonElement>(
    '[data-action="print-selected"]'
  );
  if (info) {
    info.textContent = selectedIndexes.size
      ? `${selectedIndexes.size} Eintrag(e) ausgewählt.`
      : "Keine Einträge ausgewählt.";
  }
  if (printButton) {
    printButton.disabled = !selectedIndexes.size;
  }
}

function toggleSectionAvailability(
  section: HTMLElement,
  state: AppState
): void {
  const hasDatabase = Boolean(state.app?.hasDatabase);
  section.classList.toggle("d-none", !hasDatabase);
}

export function initHistory(
  container: Element | null,
  services: Services
): void {
  if (!container || initialized) {
    return;
  }

  const host = container as HTMLElement;
  host.innerHTML = "";
  const section = createSection();
  host.appendChild(section);

  const handleStateChange = (nextState: AppState) => {
    toggleSectionAvailability(section, nextState);
    renderHistoryTable(section, nextState);
    const detailCard = section.querySelector<HTMLElement>("#history-detail");
    if (detailCard && !detailCard.classList.contains("d-none")) {
      const detailIndex = Number(detailCard.dataset.index);
      if (!Number.isNaN(detailIndex) && nextState.history[detailIndex]) {
        renderDetail(
          nextState.history[detailIndex] as HistoryEntry,
          section,
          detailIndex,
          nextState.fieldLabels
        );
      } else {
        renderDetail(null, section, null, nextState.fieldLabels);
      }
    }
    updateSelectionUI(section);
  };

  services.state.subscribe(handleStateChange);
  handleStateChange(services.state.getState());

  section.addEventListener("click", (event) => {
    const target = event.target as HTMLElement | null;
    if (!target) {
      return;
    }
    const action = target.dataset.action;
    if (!action) {
      return;
    }

    if (action === "detail-print") {
      const detailCard = target.closest<HTMLElement>("#history-detail");
      const indexAttr = detailCard?.dataset.index;
      const index =
        typeof indexAttr === "string" && indexAttr !== ""
          ? Number(indexAttr)
          : NaN;
      const state = services.state.getState();
      const entry = Number.isInteger(index)
        ? (state.history[index] as HistoryEntry)
        : null;
      printDetail(entry, state.fieldLabels);
      return;
    }

    if (action === "print-selected") {
      const state = services.state.getState();
      const entries = Array.from(selectedIndexes)
        .sort((a, b) => a - b)
        .map((idx) => state.history[idx] as HistoryEntry)
        .filter(Boolean);
      void printSummary(entries, state.fieldLabels);
      return;
    }

    if (action === "load-more") {
      const btn = target as HTMLButtonElement;
      const currentLimit = parseInt(btn.dataset.currentLimit ?? "0", 10);
      const state = services.state.getState();
      const newLimit = Math.min(
        state.history.length,
        currentLimit + INITIAL_LOAD_LIMIT
      );
      const listContainer = section.querySelector<HTMLElement>(
        '[data-role="history-list"]'
      );
      if (!listContainer) {
        return;
      }
      const resolvedLabels = state.fieldLabels;

      const fragment = document.createDocumentFragment();
      for (let i = currentLimit; i < newLimit; i += 1) {
        const entry = state.history[i] as HistoryEntry;
        const selected = selectedIndexes.has(i);
        const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
          showActions: true,
          includeCheckbox: true,
          index: i,
          selected,
        });
        const wrapper = document.createElement("div");
        wrapper.innerHTML = cardHtml;
        if (wrapper.firstElementChild) {
          fragment.appendChild(wrapper.firstElementChild);
        }
      }

      btn.remove();
      listContainer.appendChild(fragment);

      if (newLimit < state.history.length) {
        const newBtn = document.createElement("button");
        newBtn.className = "btn btn-secondary w-100 mt-3";
        newBtn.textContent = `Mehr laden (${state.history.length - newLimit} weitere)`;
        newBtn.dataset.action = "load-more";
        newBtn.dataset.currentLimit = String(newLimit);
        listContainer.appendChild(newBtn);
      }
      return;
    }

    const index = Number(target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }

    const state = services.state.getState();

    if (action === "view") {
      const entry = state.history[index] as HistoryEntry | undefined;
      renderDetail(entry ?? null, section, index, state.fieldLabels);
    } else if (action === "delete") {
      if (!window.confirm("Wirklich löschen?")) {
        return;
      }
      services.state.updateSlice("history", (history) => {
        const copy = [...history];
        copy.splice(index, 1);
        return copy;
      });
      selectedIndexes.clear();
      updateSelectionUI(section);
      renderDetail(null, section, null, state.fieldLabels);
      void persistHistoryChanges().catch((err) => {
        console.error("Persist delete history error", err);
      });
    }
  });

  section.addEventListener("change", (event) => {
    const target = event.target as HTMLInputElement | null;
    if (!target || target.dataset.action !== "toggle-select") {
      return;
    }
    const index = Number(target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    const listContainer = section.querySelector('[data-role="history-list"]');
    if (target.checked) {
      selectedIndexes.add(index);
      updateCardSelection(listContainer, index, true);
    } else {
      selectedIndexes.delete(index);
      updateCardSelection(listContainer, index, false);
    }
    updateSelectionUI(section);
  });

  initialized = true;
}
