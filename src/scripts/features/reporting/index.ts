import { getState, subscribeState, type AppState } from "@scripts/core/state";
import { renderCalculationSnapshot } from "@scripts/features/shared/calculationSnapshot";
import { initVirtualList } from "@scripts/core/virtualList";
import { escapeHtml } from "@scripts/core/utils";
import { printEntriesChunked } from "@scripts/features/shared/printing";
import type { CalculationItem } from "@scripts/features/calculation";

interface Services {
  state: {
    getState: typeof getState;
    subscribe: typeof subscribeState;
  };
}

type DateFilter = {
  start: Date;
  end: Date;
  startLabel: string;
  endLabel: string;
};

type ReportingEntry = {
  datum?: string;
  date?: string;
  ersteller?: string;
  standort?: string;
  kultur?: string;
  kisten?: number;
  items?: CalculationItem[];
  [key: string]: unknown;
};

const USE_VIRTUAL_SCROLLING = true;
const INITIAL_LOAD_LIMIT = 200;

let initialized = false;
let currentEntries: ReportingEntry[] = [];
let activeFilter: DateFilter | null = null;
let virtualListInstance: ReturnType<typeof initVirtualList> | null = null;

function createSection(): HTMLElement {
  const section = document.createElement("div");
  section.className = "section-inner";
  section.innerHTML = `
    <h2 class="text-center mb-4">Auswertung nach Datum</h2>
    <div class="card card-dark no-print mb-4">
      <div class="card-body">
        <form id="report-filter" class="row g-3">
          <div class="col-md-4">
            <label class="form-label" for="report-start">Startdatum</label>
            <input type="date" class="form-control" id="report-start" name="report-start" required />
          </div>
          <div class="col-md-4">
            <label class="form-label" for="report-end">Enddatum</label>
            <input type="date" class="form-control" id="report-end" name="report-end" required />
          </div>
          <div class="col-md-4 d-flex align-items-end">
            <button class="btn btn-success w-100" type="submit">Anzeigen</button>
          </div>
        </form>
      </div>
    </div>
    <div class="card card-dark">
      <div class="card-header d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 no-print">
        <div class="small text-muted" data-role="report-info">Alle Einträge</div>
        <button class="btn btn-outline-light btn-sm" data-action="print-report" disabled>Drucken</button>
      </div>
      <div class="card-body">
        <div data-role="report-list" class="report-list"></div>
      </div>
    </div>
  `;
  return section;
}

function parseDate(value: FormDataEntryValue | null): Date | null {
  if (typeof value !== "string" || !value) {
    return null;
  }
  const parts = value.split("-");
  if (parts.length !== 3) {
    return null;
  }
  const [year, month, day] = parts.map(Number);
  if (Number.isNaN(year) || Number.isNaN(month) || Number.isNaN(day)) {
    return null;
  }
  return new Date(year, month - 1, day);
}

function germanDateToIso(value: string | null | undefined): Date | null {
  if (!value) {
    return null;
  }
  const parts = value.split(".");
  if (parts.length !== 3) {
    return null;
  }
  const [day, month, year] = parts.map(Number);
  if (Number.isNaN(day) || Number.isNaN(month) || Number.isNaN(year)) {
    return null;
  }
  return new Date(year, month - 1, day);
}

function resolveReportingLabels(labels: AppState["fieldLabels"]): any {
  const resolvedLabels = labels || getState().fieldLabels;
  return resolvedLabels?.reporting || {};
}

function describeFilter(
  entryCount: number,
  labels: AppState["fieldLabels"]
): string {
  const reportingLabels = resolveReportingLabels(labels);
  const infoEmpty = reportingLabels.infoEmpty || "Keine Einträge";
  const infoAll = reportingLabels.infoAll || "Alle Einträge";
  const infoPrefix = reportingLabels.infoPrefix || "Auswahl";

  if (!activeFilter) {
    if (!entryCount) {
      return infoEmpty;
    }
    return `${infoAll} (${entryCount})`;
  }

  const prefix = `${infoPrefix} ${activeFilter.startLabel} – ${activeFilter.endLabel}`;
  if (!entryCount) {
    return `${prefix} (${infoEmpty})`;
  }
  return `${prefix} (${entryCount})`;
}

function renderCardsList(
  listContainer: HTMLElement,
  entries: ReportingEntry[],
  labels: AppState["fieldLabels"]
): void {
  const resolvedLabels = labels || getState().fieldLabels;

  if (USE_VIRTUAL_SCROLLING && entries.length > INITIAL_LOAD_LIMIT) {
    listContainer.innerHTML = "";
    if (!virtualListInstance) {
      virtualListInstance = initVirtualList(listContainer, {
        itemCount: entries.length,
        estimatedItemHeight: 200,
        overscan: 6,
        renderItem: (node, index) => {
          const entry = currentEntries[index];
          node.innerHTML = renderCalculationSnapshot(entry, resolvedLabels, {
            showActions: false,
            includeCheckbox: false,
          });
        },
      });
    } else {
      virtualListInstance.updateItemCount(entries.length);
    }
    return;
  }

  if (virtualListInstance) {
    virtualListInstance.destroy();
    virtualListInstance = null;
  }

  const limit = Math.min(entries.length, INITIAL_LOAD_LIMIT);
  const fragment = document.createDocumentFragment();

  for (let i = 0; i < limit; i += 1) {
    const entry = entries[i];
    const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
      showActions: false,
      includeCheckbox: false,
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

function renderTable(
  section: HTMLElement,
  entries: ReportingEntry[],
  labels: AppState["fieldLabels"]
): void {
  currentEntries = entries.slice();

  const listContainer = section.querySelector<HTMLElement>(
    '[data-role="report-list"]'
  );
  if (listContainer) {
    renderCardsList(listContainer, entries, labels);
  }

  const info = section.querySelector<HTMLElement>('[data-role="report-info"]');
  if (info) {
    info.textContent = describeFilter(entries.length, labels);
  }

  const printButton = section.querySelector<HTMLButtonElement>(
    '[data-action="print-report"]'
  );
  if (printButton) {
    printButton.disabled = entries.length === 0;
  }
}

function applyFilter(
  section: HTMLElement,
  state: AppState,
  filter: DateFilter | null
): void {
  const source = Array.isArray(state.history)
    ? (state.history as ReportingEntry[])
    : [];
  if (!filter) {
    renderTable(section, source, state.fieldLabels);
    return;
  }

  const filtered = source.filter((entry) => {
    const isoDate = germanDateToIso(
      (entry.datum as string) || (entry.date as string)
    );
    if (!isoDate) {
      return false;
    }
    return isoDate >= filter.start && isoDate <= filter.end;
  });

  renderTable(section, filtered, state.fieldLabels);
}

function toggleSection(section: HTMLElement, state: AppState): void {
  const ready = Boolean(state.app?.hasDatabase);
  const active = state.app?.activeSection === "report";
  section.classList.toggle("d-none", !(ready && active));
  if (ready) {
    applyFilter(section, state, activeFilter);
  }
}

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

function buildFilterInfo(
  filter: DateFilter | null,
  labels: AppState["fieldLabels"]
): string {
  const reportingLabels = resolveReportingLabels(labels);
  const prefix = escapeHtml(reportingLabels.infoPrefix || "Auswahl");
  if (!filter) {
    const infoAll = escapeHtml(reportingLabels.infoAll || "Alle Einträge");
    return `<p>${prefix}: ${infoAll}</p>`;
  }
  return `<p>${prefix}: ${escapeHtml(filter.startLabel)} – ${escapeHtml(filter.endLabel)}</p>`;
}

async function printReport(
  entries: ReportingEntry[],
  filter: DateFilter | null,
  labels: AppState["fieldLabels"]
): Promise<void> {
  if (!entries.length) {
    window.alert("Keine Daten für den Druck vorhanden.");
    return;
  }
  const resolvedLabels = labels || getState().fieldLabels;
  const reportingLabels = resolveReportingLabels(resolvedLabels);
  const company = getState().company || {};
  const headerHtml =
    buildCompanyHeader(company) + buildFilterInfo(filter, resolvedLabels);
  try {
    await printEntriesChunked(entries, resolvedLabels, {
      title: reportingLabels.printTitle || "Bericht",
      headerHtml,
      chunkSize: 50,
    });
  } catch (err) {
    console.error("Printing failed", err);
    window.alert("Fehler beim Drucken. Bitte erneut versuchen.");
  }
}

export function initReporting(
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

  const filterForm = section.querySelector<HTMLFormElement>("#report-filter");
  filterForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(filterForm);
    const start = parseDate(formData.get("report-start"));
    const end = parseDate(formData.get("report-end"));
    if (!start || !end) {
      window.alert("Bitte gültige Daten auswählen!");
      return;
    }
    if (start > end) {
      window.alert("Das Startdatum muss vor dem Enddatum liegen.");
      return;
    }
    activeFilter = {
      start,
      end,
      startLabel: new Intl.DateTimeFormat("de-DE").format(start),
      endLabel: new Intl.DateTimeFormat("de-DE").format(end),
    };
    applyFilter(section, services.state.getState(), activeFilter);
  });

  services.state.subscribe((nextState) => {
    toggleSection(section, nextState);
  });

  toggleSection(section, services.state.getState());

  section.addEventListener("click", (event) => {
    const target = event.target as HTMLElement | null;
    if (!target) {
      return;
    }

    if (target.dataset.action === "load-more") {
      const btn = target as HTMLButtonElement;
      const currentLimit = parseInt(btn.dataset.currentLimit || "0", 10);
      const listContainer = section.querySelector<HTMLElement>(
        '[data-role="report-list"]'
      );
      if (!listContainer) {
        return;
      }
      const newLimit = Math.min(
        currentEntries.length,
        currentLimit + INITIAL_LOAD_LIMIT
      );
      const labels = services.state.getState().fieldLabels;
      const fragment = document.createDocumentFragment();

      for (let i = currentLimit; i < newLimit; i += 1) {
        const entry = currentEntries[i];
        const cardHtml = renderCalculationSnapshot(entry, labels, {
          showActions: false,
          includeCheckbox: false,
        });
        const wrapper = document.createElement("div");
        wrapper.innerHTML = cardHtml;
        if (wrapper.firstElementChild) {
          fragment.appendChild(wrapper.firstElementChild);
        }
      }

      btn.remove();
      listContainer.appendChild(fragment);

      if (newLimit < currentEntries.length) {
        const newBtn = document.createElement("button");
        newBtn.className = "btn btn-secondary w-100 mt-3";
        newBtn.textContent = `Mehr laden (${currentEntries.length - newLimit} weitere)`;
        newBtn.dataset.action = "load-more";
        newBtn.dataset.currentLimit = String(newLimit);
        listContainer.appendChild(newBtn);
      }
      return;
    }

    const printTrigger = target.closest<HTMLButtonElement>(
      '[data-action="print-report"]'
    );
    if (printTrigger) {
      void printReport(
        currentEntries,
        activeFilter,
        services.state.getState().fieldLabels
      );
    }
  });

  initialized = true;
}
