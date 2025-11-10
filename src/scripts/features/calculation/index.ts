import { escapeHtml } from "@scripts/core/utils";
import { getState } from "@scripts/core/state";
import { setFieldLabelByPath } from "@scripts/core/labels";
import { getDatabaseSnapshot } from "@scripts/core/database";
import { saveDatabase, getActiveDriverKey } from "@scripts/core/storage";
import {
  buildMediumTableHead,
  buildMediumTableRows,
} from "../shared/mediumTable";

interface Services {
  state: {
    getState: typeof getState;
    updateSlice: typeof import("@scripts/core/state").updateSlice;
    subscribe: typeof import("@scripts/core/state").subscribeState;
  };
  events: {
    emit: typeof import("@scripts/core/eventBus").emit;
  };
}

type Labels = ReturnType<typeof getState>["fieldLabels"];

type DefaultsState = ReturnType<typeof getState>["defaults"];

export type CalculationItem = {
  id: string;
  name: string;
  unit: string;
  methodLabel: string;
  methodId: string;
  value: number;
  total: number;
  inputs: Record<string, number>;
};

export type CalculationResult = {
  header: Record<string, any>;
  items: CalculationItem[];
};

let initialized = false;

function escapeAttr(value: unknown): string {
  return escapeHtml(value);
}

function createSection(
  labels: Labels,
  defaultsState: DefaultsState
): HTMLElement {
  const formDefaults = defaultsState?.form || {
    creator: "",
    location: "",
    crop: "",
    quantity: "",
  };
  const section = document.createElement("section");
  section.className = "section-inner";
  section.innerHTML = `
    <div class="card card-dark mb-4 no-print">
      <div class="card-body">
        <form id="calculationForm" class="row g-3 no-print">
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.creator.label" data-default-label="${escapeAttr(labels.calculation.fields.creator.label)}" value="${escapeAttr(labels.calculation.fields.creator.label)}" placeholder="${escapeAttr(labels.calculation.fields.creator.label)}" aria-label="Bezeichnung für Feld" />
            <input type="text" class="form-control" id="calc-ersteller" name="calc-ersteller" required data-placeholder-id="calc-form-creator" placeholder="${escapeAttr(labels.calculation.fields.creator.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.creator.label)}" value="${escapeAttr(formDefaults.creator || "")}" />
          </div>
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.location.label" data-default-label="${escapeAttr(labels.calculation.fields.location.label)}" value="${escapeAttr(labels.calculation.fields.location.label)}" placeholder="${escapeAttr(labels.calculation.fields.location.label)}" aria-label="Bezeichnung für Feld" />
            <input type="text" class="form-control" id="calc-standort" name="calc-standort" data-placeholder-id="calc-form-location" placeholder="${escapeAttr(labels.calculation.fields.location.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.location.label)}" value="${escapeAttr(formDefaults.location || "")}" />
          </div>
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.crop.label" data-default-label="${escapeAttr(labels.calculation.fields.crop.label)}" value="${escapeAttr(labels.calculation.fields.crop.label)}" placeholder="${escapeAttr(labels.calculation.fields.crop.label)}" aria-label="Bezeichnung für Feld" />
            <input type="text" class="form-control" id="calc-kultur" name="calc-kultur" data-placeholder-id="calc-form-crop" placeholder="${escapeAttr(labels.calculation.fields.crop.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.crop.label)}" value="${escapeAttr(formDefaults.crop || "")}" />
          </div>
          <div class="col-md-3">
            <input type="text" class="form-control form-control-sm label-editor mb-2" data-label-editor="calculation.fields.quantity.label" data-default-label="${escapeAttr(labels.calculation.fields.quantity.label)}" value="${escapeAttr(labels.calculation.fields.quantity.label)}" placeholder="${escapeAttr(labels.calculation.fields.quantity.label)}" aria-label="Bezeichnung für Feld" />
            <input type="number" min="0" step="any" class="form-control" id="calc-kisten" name="calc-kisten" required data-placeholder-id="calc-form-quantity" placeholder="${escapeAttr(labels.calculation.fields.quantity.placeholder)}" aria-label="${escapeAttr(labels.calculation.fields.quantity.label)}" value="${escapeAttr(formDefaults.quantity || "")}" />
          </div>
          <div class="col-12 text-center">
            <button type="submit" class="btn btn-success px-4">Berechnen</button>
          </div>
        </form>
      </div>
    </div>
    <div id="calc-result" class="card card-dark d-none">
      <div class="card-body">
        <div class="calc-summary mb-3">
          <div class="calc-summary-columns">
            <div class="calc-summary-column calc-summary-main">
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-creator">${escapeHtml(labels.calculation.fields.creator.label)}</span>
                <span class="calc-summary-value" data-field="ersteller"></span>
              </div>
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-location">${escapeHtml(labels.calculation.fields.location.label)}</span>
                <span class="calc-summary-value" data-field="standort"></span>
              </div>
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-crop">${escapeHtml(labels.calculation.fields.crop.label)}</span>
                <span class="calc-summary-value" data-field="kultur"></span>
              </div>
              <div class="calc-summary-row">
                <span class="calc-summary-label" data-label-id="calc-summary-date">${escapeHtml(labels.calculation.summary.dateLabel || "Datum")}</span>
                <span class="calc-summary-value" data-field="datum"></span>
              </div>
            </div>
            <div class="calc-summary-column calc-summary-company">
              <div class="calc-summary-row d-none" data-company-row="headline">
                <span class="calc-summary-label">Claim</span>
                <span class="calc-summary-value" data-field="company-headline"></span>
              </div>
              <div class="calc-summary-row d-none" data-company-row="address">
                <span class="calc-summary-label">Anschrift</span>
                <span class="calc-summary-value calc-summary-value--multiline" data-field="company-address"></span>
              </div>
              <div class="calc-summary-row d-none" data-company-row="email">
                <span class="calc-summary-label">E-Mail</span>
                <a class="calc-summary-value" data-field="company-email"></a>
              </div>
            </div>
          </div>
        </div>
        <div class="calc-table-wrapper">
          <table class="table table-dark table-striped align-middle calc-medium-table" id="calc-results-table">
            <thead></thead>
            <tbody></tbody>
          </table>
        </div>
        <div class="mt-3 no-print">
          <button class="btn btn-outline-secondary" data-action="print">Drucken / PDF</button>
          <button class="btn btn-primary ms-2" data-action="save">Aktuelle Berechnung speichern</button>
        </div>
      </div>
    </div>
  `;
  return section;
}

function applyFieldLabels(section: HTMLElement, labels: Labels): void {
  const labelMap: Record<string, string> = {
    "calc-form-creator": labels.calculation.fields.creator.label,
    "calc-form-location": labels.calculation.fields.location.label,
    "calc-form-crop": labels.calculation.fields.crop.label,
    "calc-form-quantity": labels.calculation.fields.quantity.label,
    "calc-summary-creator": labels.calculation.fields.creator.label,
    "calc-summary-location": labels.calculation.fields.location.label,
    "calc-summary-crop": labels.calculation.fields.crop.label,
    "calc-summary-date": labels.calculation.summary.dateLabel || "Datum",
  };

  Object.entries(labelMap).forEach(([key, text]) => {
    const element = section.querySelector(`[data-label-id="${key}"]`);
    if (element) {
      element.textContent = typeof text === "string" ? text : "";
    }
  });

  section
    .querySelectorAll<HTMLInputElement>(".label-editor")
    .forEach((input) => {
      const path = input.dataset.labelEditor;
      if (!path) {
        return;
      }
      const value = path
        .split(".")
        .reduce<any>(
          (acc, segment) =>
            acc && acc[segment] !== undefined ? acc[segment] : null,
          labels
        );
      if (typeof value === "string") {
        input.placeholder = value;
        input.dataset.defaultLabel = value;
        if (!input.matches(":focus")) {
          input.value = value;
        }
      }
    });

  const placeholderMap: Record<string, string> = {
    "calc-form-creator": labels.calculation.fields.creator.placeholder,
    "calc-form-location": labels.calculation.fields.location.placeholder,
    "calc-form-crop": labels.calculation.fields.crop.placeholder,
    "calc-form-quantity": labels.calculation.fields.quantity.placeholder,
  };

  Object.entries(placeholderMap).forEach(([key, text]) => {
    const element = section.querySelector<HTMLInputElement>(
      `[data-placeholder-id="${key}"]`
    );
    if (element) {
      element.setAttribute("placeholder", typeof text === "string" ? text : "");
    }
  });

  const tableHead = section.querySelector("#calc-results-table thead");
  if (tableHead) {
    tableHead.innerHTML = buildMediumTableHead(labels, "calculation");
  }
}

async function persistHistory(services: Services): Promise<void> {
  const driverKey = getActiveDriverKey();
  if (!driverKey || driverKey === "memory") {
    return;
  }
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
    services.events.emit("database:saved", {
      scope: "history",
      driver: driverKey,
    });
  } catch (err) {
    console.error("Automatisches Speichern der Historie fehlgeschlagen", err);
    services.events.emit("database:error", { scope: "history", error: err });
    window.alert(
      "Berechnung gespeichert, aber die Datei konnte nicht aktualisiert werden. Bitte manuell sichern."
    );
  }
}

function getWaterVolume(kisten: number, defaults: DefaultsState): number {
  const arValue = kisten / (defaults.kistenProAr || 1);
  return arValue * (defaults.waterPerKisteL || 0);
}

function executeFormula(
  medium: any,
  method: any,
  inputs: Record<string, number>
): number {
  if (!method) {
    return 0;
  }
  const value = Number(medium.value) || 0;
  switch (method.type) {
    case "factor": {
      const base = inputs[method.config?.sourceField || "kisten"] || 0;
      return value * base;
    }
    case "percentOf": {
      const base = inputs[method.config?.baseField || "waterVolume"] || 0;
      return (base * value) / 100;
    }
    case "fixed":
      return value;
    default:
      console.warn("Unbekannter Methodentyp", method.type);
      return value;
  }
}

function renderResults(
  section: HTMLElement,
  calculation: CalculationResult | null,
  labels: Labels
): void {
  const resultCard = section.querySelector<HTMLDivElement>("#calc-result");
  if (!resultCard) {
    return;
  }
  const resultsTable = resultCard.querySelector<HTMLTableElement>(
    "#calc-results-table"
  );
  const resultsHead = resultsTable?.querySelector("thead");
  const resultsBody = resultsTable?.querySelector("tbody");
  const stateSnapshot = getState();
  const companyData = stateSnapshot.company || {};

  if (!calculation) {
    resultCard.classList.add("d-none");
    if (resultsBody) {
      resultsBody.innerHTML = "";
    }
    return;
  }

  const { header, items } = calculation;

  const setFieldText = (key: string, value: string): void => {
    const el = resultCard.querySelector<HTMLElement>(`[data-field="${key}"]`);
    if (el) {
      el.textContent = value ?? "";
    }
  };

  setFieldText("ersteller", header.ersteller);
  setFieldText("standort", header.standort);
  setFieldText("kultur", header.kultur);
  setFieldText("datum", header.datum);

  const updateCompanyRowVisibility = (rowKey: string, visible: boolean) => {
    const row = resultCard.querySelector<HTMLElement>(
      `[data-company-row="${rowKey}"]`
    );
    if (row) {
      row.classList.toggle("d-none", !visible);
    }
  };

  const headlineValue = companyData.headline?.trim() || "";
  setFieldText("company-headline", headlineValue);
  updateCompanyRowVisibility("headline", Boolean(headlineValue));

  const addressValue = companyData.address?.trim() || "";
  const addressEl = resultCard.querySelector<HTMLElement>(
    '[data-field="company-address"]'
  );
  if (addressEl) {
    addressEl.textContent = addressValue;
  }
  updateCompanyRowVisibility("address", Boolean(addressValue));

  const emailEl = resultCard.querySelector<HTMLAnchorElement>(
    '[data-field="company-email"]'
  );
  const emailValue = companyData.contactEmail?.trim() || "";
  if (emailEl) {
    if (emailValue) {
      emailEl.textContent = emailValue;
      emailEl.setAttribute("href", `mailto:${emailValue}`);
    } else {
      emailEl.textContent = "";
      emailEl.removeAttribute("href");
    }
  }
  updateCompanyRowVisibility("email", Boolean(emailValue));

  const companyColumn = resultCard.querySelector<HTMLElement>(
    ".calc-summary-company"
  );
  if (companyColumn) {
    const hasVisibleRow = Array.from(
      companyColumn.querySelectorAll<HTMLElement>("[data-company-row]")
    ).some((row) => !row.classList.contains("d-none"));
    companyColumn.classList.toggle("d-none", !hasVisibleRow);
  }

  if (resultsHead) {
    resultsHead.innerHTML = buildMediumTableHead(labels, "calculation");
  }
  if (resultsBody) {
    resultsBody.innerHTML = buildMediumTableRows(items, "calculation");
  }

  resultCard.classList.remove("d-none");
}

export function initCalculation(
  container: Element | null,
  services: Services
): void {
  if (!container || initialized) {
    return;
  }

  const host = container as HTMLElement;
  const initialState = services.state.getState();

  host.innerHTML = "";
  const section = createSection(
    initialState.fieldLabels,
    initialState.defaults
  );
  host.appendChild(section);

  applyFieldLabels(section, initialState.fieldLabels);

  const form = section.querySelector<HTMLFormElement>("#calculationForm");
  const resultCard = section.querySelector<HTMLDivElement>("#calc-result");
  const resultsTable = section.querySelector<HTMLTableElement>(
    "#calc-results-table"
  );
  const resultsHead = resultsTable?.querySelector("thead");
  const resultsBody = resultsTable?.querySelector("tbody");

  if (!form || !resultCard || !resultsTable || !resultsHead || !resultsBody) {
    console.warn("Berechnungsbereich konnte nicht initialisiert werden");
    return;
  }

  const setCalcContext = (calculation: CalculationResult | null) => {
    renderResults(section, calculation, services.state.getState().fieldLabels);
  };

  services.state.subscribe((nextState) => {
    applyFieldLabels(section, nextState.fieldLabels);
    setCalcContext(nextState.calcContext as CalculationResult | null);
  });

  section
    .querySelectorAll<HTMLInputElement>(".label-editor")
    .forEach((input) => {
      input.addEventListener("change", (event) => {
        const target = event.currentTarget as HTMLInputElement;
        const path = target.dataset.labelEditor;
        if (!path) {
          return;
        }
        const trimmed = target.value.trim();
        const fallback =
          target.dataset.defaultLabel ||
          target.getAttribute("placeholder") ||
          target.value;
        const nextValue = trimmed || fallback || "";
        if (!trimmed) {
          target.value = nextValue;
        }
        services.state.updateSlice("fieldLabels", (currentLabels) =>
          setFieldLabelByPath(currentLabels, path, nextValue)
        );
      });
    });

  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const formData = new FormData(form);
    const rawErsteller = (formData.get("calc-ersteller") || "")
      .toString()
      .trim();
    const rawStandort = (formData.get("calc-standort") || "").toString().trim();
    const rawKultur = (formData.get("calc-kultur") || "").toString().trim();
    const rawKisten = (formData.get("calc-kisten") || "").toString().trim();
    const ersteller = rawErsteller;
    const standort = rawStandort || "-";
    const kultur = rawKultur || "-";
    const kisten = Number(rawKisten);
    if (!ersteller || Number.isNaN(kisten)) {
      window.alert("Bitte Felder korrekt ausfüllen!");
      return;
    }

    const state = services.state.getState();
    const defaults = state.defaults;
    const measurementMethods = state.measurementMethods;
    const waterVolume = getWaterVolume(kisten, defaults);
    const areaAr = defaults.kistenProAr ? kisten / defaults.kistenProAr : 0;
    const areaSqm = areaAr * 100;

    const inputs = {
      kisten,
      waterVolume,
      areaAr,
      areaSqm,
    };

    const items = state.mediums.map((medium: any) => {
      const method = measurementMethods.find(
        (m: any) => m.id === medium.methodId
      );
      const total = executeFormula(medium, method, inputs);
      return {
        id: medium.id,
        name: medium.name,
        unit: medium.unit,
        methodLabel: method ? method.label : medium.methodId,
        methodId: medium.methodId,
        value: medium.value,
        total,
        inputs,
      };
    });

    const header = {
      ersteller,
      standort,
      kultur,
      kisten,
      datum: new Date().toLocaleDateString("de-DE", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      }),
      waterVolume,
      areaAr,
      areaSqm,
    };

    const calculation: CalculationResult = {
      header,
      items,
    };

    services.state.updateSlice("defaults", (defaultsState) => ({
      ...defaultsState,
      form: {
        ...(defaultsState.form || {
          creator: "",
          location: "",
          crop: "",
          quantity: "",
        }),
        creator: rawErsteller,
        location: rawStandort,
        crop: rawKultur,
        quantity: rawKisten,
      },
    }));

    services.state.updateSlice("calcContext", () => calculation);
  });

  resultCard.addEventListener("click", (event) => {
    const target = event.target as HTMLElement;
    const action = target?.dataset?.action;
    if (!action) {
      return;
    }
    if (action === "print") {
      window.print();
    } else if (action === "save") {
      const calc = services.state.getState()
        .calcContext as CalculationResult | null;
      if (!calc) {
        window.alert("Keine Berechnung vorhanden.");
        return;
      }
      services.state.updateSlice("history", (history) => {
        const entry = {
          ...calc.header,
          items: calc.items,
          savedAt: new Date().toISOString(),
        };
        return [...history, entry];
      });
      void persistHistory(services).catch((err) => {
        console.error("Persist history promise error", err);
      });
      window.alert("Berechnung gespeichert! (Siehe Historie)");
    }
  });

  setCalcContext(initialState.calcContext as CalculationResult | null);

  initialized = true;
}
