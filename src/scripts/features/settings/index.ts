import {
  getState,
  subscribeState,
  updateSlice,
  type AppState,
} from "@scripts/core/state";
import { getDatabaseSnapshot } from "@scripts/core/database";
import { saveDatabase } from "@scripts/core/storage";
import { escapeHtml } from "@scripts/core/utils";

interface Services {
  state: {
    getState: typeof getState;
    subscribe: typeof subscribeState;
    updateSlice: typeof updateSlice;
  };
}

let initialized = false;
let mediumsTableBody: HTMLTableSectionElement | null = null;
let methodInput: HTMLInputElement | null = null;
let methodDatalist: HTMLDataListElement | null = null;
let addForm: HTMLFormElement | null = null;

function createSection(): HTMLElement {
  const section = document.createElement("div");
  section.className = "section-inner";
  section.innerHTML = `
    <h2 class="text-center mb-4">Mittel-Verwaltung</h2>
    <div class="card card-dark mb-4">
      <div class="card-body">
        <p class="mb-2"><strong>Was kann ich hier tun?</strong></p>
        <p class="text-muted mb-0">
          Erfasse, bearbeite oder lösche deine Mittel. Trage Name, Einheit, Methode und den Faktor ein und speichere
          die Änderungen anschließend in der Datenbank. Tippe bei der Methode einfach einen bestehenden Namen oder
          vergib einen neuen – neu erfasste Methoden stehen beim nächsten Mal automatisch zur Auswahl.
        </p>
      </div>
    </div>
    <div class="card card-dark mb-4">
      <div class="card-body">
        <div class="table-responsive">
          <table class="table table-dark table-bordered" id="settings-mediums-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Einheit</th>
                <th>Methode</th>
                <th>Wert</th>
                <th>Aktion</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="card card-dark">
      <div class="card-body">
        <h3 class="h5 text-success mb-3">Neues Mittel hinzufügen</h3>
        <p class="text-muted">Trage alle Felder aus. Der Wert beschreibt den Faktor, der bei der Berechnung angewendet wird.</p>
        <form id="settings-medium-form" class="row g-3">
          <div class="col-md-3">
            <input class="form-control" name="medium-name" placeholder="Name (z. B. Elot-Vis)" required />
          </div>
          <div class="col-md-2">
            <input class="form-control" name="medium-unit" placeholder="Einheit (z. B. ml, %)" required />
          </div>
          <div class="col-md-3">
            <input class="form-control" name="medium-method" placeholder="Methode (z. B. perKiste)" list="settings-method-options" required />
            <datalist id="settings-method-options"></datalist>
          </div>
          <div class="col-md-2">
            <input type="number" step="any" class="form-control" name="medium-value" placeholder="Wert" required />
          </div>
          <div class="col-md-2 d-grid">
            <button class="btn btn-success" type="submit">Hinzufügen</button>
          </div>
        </form>
        <div class="mt-3 small text-muted">
          Nach dem Hinzufügen kannst du Mittel jederzeit löschen. Änderungen werden erst mit dem Button unten dauerhaft gespeichert.
        </div>
      </div>
    </div>
    <div class="mt-4 no-print">
      <button class="btn btn-success" data-action="persist">Änderungen speichern</button>
    </div>
  `;
  return section;
}

function renderMediumRows(state: AppState): void {
  if (!mediumsTableBody) {
    return;
  }
  const methodsById = new Map<string, any>(
    state.measurementMethods.map((method: any) => [method.id, method])
  );

  if (!state.mediums.length) {
    mediumsTableBody.innerHTML = `
      <tr>
        <td colspan="5" class="text-center text-muted">Noch keine Mittel gespeichert.</td>
      </tr>
    `;
    return;
  }

  mediumsTableBody.innerHTML = "";
  state.mediums.forEach((medium: any, index: number) => {
    const row = document.createElement("tr");
    const method = methodsById.get(medium.methodId);
    row.innerHTML = `
      <td>${escapeHtml(medium.name)}</td>
      <td>${escapeHtml(medium.unit)}</td>
      <td>${escapeHtml(method ? method.label : medium.methodId)}</td>
      <td>${escapeHtml(medium.value != null ? String(medium.value) : "")}</td>
      <td>
        <button class="btn btn-sm btn-danger" data-action="delete" data-index="${index}">Löschen</button>
      </td>
    `;
    mediumsTableBody?.appendChild(row);
  });
}

function renderMethodSuggestions(state: AppState): void {
  if (!methodDatalist) {
    return;
  }
  const seen = new Set<string>();
  methodDatalist.innerHTML = "";
  state.measurementMethods.forEach((method: any) => {
    const labelKey = (method.label ?? "").toLowerCase();
    const idKey = (method.id ?? "").toLowerCase();
    if (labelKey && !seen.has(labelKey)) {
      seen.add(labelKey);
      const labelOption = document.createElement("option");
      labelOption.value = method.label;
      methodDatalist!.appendChild(labelOption);
    }
    if (idKey && !seen.has(idKey)) {
      seen.add(idKey);
      const idOption = document.createElement("option");
      idOption.value = method.id;
      methodDatalist!.appendChild(idOption);
    }
  });
}

function createMethodId(label: string): string {
  const slug = label
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (slug) {
    return slug;
  }
  return `method-${Date.now()}-${Math.random().toString(16).slice(2, 6)}`;
}

function ensureMethodExists(
  state: AppState,
  services: Services
): string | null {
  if (!methodInput) {
    return null;
  }
  const rawValue = methodInput.value.trim();
  if (!rawValue) {
    window.alert("Bitte eine Methode angeben.");
    methodInput.focus();
    return null;
  }
  const existing = state.measurementMethods.find(
    (method: any) =>
      method.label?.toLowerCase() === rawValue.toLowerCase() ||
      method.id?.toLowerCase() === rawValue.toLowerCase()
  );
  if (existing) {
    return existing.id;
  }
  const id = createMethodId(rawValue);
  const defaultUnit =
    state.fieldLabels?.calculation?.fields?.quantity?.unit || "Kiste";
  const newMethod = {
    id,
    label: rawValue,
    type: "factor",
    unit: defaultUnit,
    requires: ["kisten"],
    config: { sourceField: "kisten" },
  };
  services.state.updateSlice("measurementMethods", (methods: any[]) => [
    ...methods,
    newMethod,
  ]);
  return id;
}

async function persistChanges(): Promise<void> {
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
    window.alert("Änderungen wurden gespeichert.");
  } catch (err) {
    console.error("Fehler beim Speichern", err);
    const message =
      err instanceof Error ? err.message : "Speichern fehlgeschlagen";
    window.alert(message);
  }
}

function toggleSectionVisibility(section: HTMLElement, state: AppState): void {
  const ready = Boolean(state.app?.hasDatabase);
  const active = state.app?.activeSection === "settings";
  section.classList.toggle("d-none", !(ready && active));
}

export function initSettings(
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

  mediumsTableBody = section.querySelector<HTMLTableSectionElement>(
    "#settings-mediums-table tbody"
  );
  methodInput = section.querySelector<HTMLInputElement>(
    'input[name="medium-method"]'
  );
  methodDatalist = section.querySelector<HTMLDataListElement>(
    "#settings-method-options"
  );
  addForm = section.querySelector<HTMLFormElement>("#settings-medium-form");

  addForm?.addEventListener("submit", (event) => {
    event.preventDefault();
    const state = services.state.getState();
    const formData = new FormData(addForm!);
    const name = (formData.get("medium-name") || "").toString().trim();
    const unit = (formData.get("medium-unit") || "").toString().trim();
    const valueRaw = formData.get("medium-value");
    const value = Number(valueRaw);
    if (!name || !unit || Number.isNaN(value)) {
      window.alert("Bitte alle Felder korrekt ausfüllen.");
      return;
    }
    const methodId = ensureMethodExists(state, services);
    if (!methodId) {
      return;
    }
    const id =
      typeof crypto !== "undefined" && typeof crypto.randomUUID === "function"
        ? crypto.randomUUID()
        : `medium-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
    const medium = {
      id,
      name,
      unit,
      methodId,
      value,
    };
    services.state.updateSlice("mediums", (mediums: any[]) => [
      ...mediums,
      medium,
    ]);
    addForm?.reset();
    renderMethodSuggestions(services.state.getState());
  });

  mediumsTableBody?.addEventListener("click", (event) => {
    const target = (
      event.target as HTMLElement | null
    )?.closest<HTMLButtonElement>('[data-action="delete"]');
    if (!target) {
      return;
    }
    const index = Number(target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    services.state.updateSlice("mediums", (mediums: any[]) => {
      const copy = mediums.slice();
      copy.splice(index, 1);
      return copy;
    });
  });

  section
    .querySelector<HTMLButtonElement>('[data-action="persist"]')
    ?.addEventListener("click", () => {
      void persistChanges();
    });

  const handleStateChange = (nextState: AppState) => {
    toggleSectionVisibility(section, nextState);
    if (nextState.app.activeSection === "settings") {
      renderMediumRows(nextState);
      renderMethodSuggestions(nextState);
    }
  };

  services.state.subscribe(handleStateChange);
  handleStateChange(services.state.getState());

  initialized = true;
}
