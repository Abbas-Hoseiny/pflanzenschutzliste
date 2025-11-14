import type { FieldType } from "./types";

export interface TemplateEditorView {
  root: HTMLElement;
  toolbar: HTMLElement;
  paletteList: HTMLElement;
  paperWrapper: HTMLElement;
  paper: HTMLElement;
  fieldLayer: HTMLElement;
  selectionLayer: HTMLElement;
  dropIndicator: HTMLElement | null;
  grid: HTMLElement;
  emptyState: HTMLElement | null;
  templateNameLabel: HTMLElement | null;
  templateVersionLabel: HTMLElement | null;
  templateUpdatedLabel: HTMLElement | null;
  templateSummaryLabel: HTMLElement | null;
  propertyPanel: HTMLElement;
  propertyGroups: HTMLElement | null;
  propertyAdvancedSections: HTMLElement[];
  propertyInputs: {
    name: HTMLInputElement | null;
    label: HTMLInputElement | null;
    placeholder: HTMLInputElement | null;
    required: HTMLInputElement | null;
    width: HTMLInputElement | null;
    height: HTMLInputElement | null;
    x: HTMLInputElement | null;
    y: HTMLInputElement | null;
  };
  propertyMultiPanel: HTMLElement | null;
  propertyMultiSummary: HTMLElement | null;
  propertyMultiInputs: {
    label: HTMLInputElement | null;
    placeholder: HTMLInputElement | null;
    width: HTMLInputElement | null;
    height: HTMLInputElement | null;
    x: HTMLInputElement | null;
    y: HTMLInputElement | null;
  };
  propertyMultiAdvancedSections: HTMLElement[];
  propertyMultiRequiredButtons: HTMLButtonElement[];
  templateSelect: HTMLSelectElement | null;
  templateSearch: HTMLInputElement | null;
  templateDeleteButton: HTMLButtonElement | null;
  templateRefreshButton: HTMLButtonElement | null;
  emptyPropertyState: HTMLElement | null;
  contextMenu: HTMLElement;
  notifications: HTMLElement;
  zoomControls: HTMLElement | null;
  zoomIndicator: HTMLElement | null;
  zoomInButton: HTMLButtonElement | null;
  zoomOutButton: HTMLButtonElement | null;
  revisionPanel: HTMLElement | null;
  revisionList: HTMLElement | null;
  revisionEmpty: HTMLElement | null;
  revisionRevertButton: HTMLButtonElement | null;
}

type PaletteItemDefinition = {
  type: FieldType;
  label: string;
  description: string;
  icon: string;
};

const PALETTE_ITEMS: readonly PaletteItemDefinition[] = [
  {
    type: "label",
    label: "Beschriftung",
    description: "Reiner Text zur Strukturierung, keine Eingabe.",
    icon: "bi-type",
  },
  {
    type: "text",
    label: "Textfeld",
    description: "Freitext-Eingabe für Notizen oder Werte.",
    icon: "bi-input-cursor-text",
  },
  {
    type: "number",
    label: "Zahlenfeld",
    description: "Numerische Eingabe, optional mit Begrenzungen.",
    icon: "bi-123",
  },
] as const;

function createToolbar(): HTMLElement {
  const toolbar = document.createElement("header");
  toolbar.className = "template-editor__toolbar";
  toolbar.innerHTML = /* html */ `
    <div class="btn-group btn-group-sm" role="group" aria-label="Vorlagenverwaltung">
      <button type="button" class="btn btn-outline-light" data-action="template-new">
        <i class="bi bi-file-earmark-plus"></i>
        <span class="ms-1">Neu</span>
      </button>
      <button type="button" class="btn btn-outline-light" data-action="template-save">
        <i class="bi bi-save"></i>
        <span class="ms-1">Speichern</span>
      </button>
      <button type="button" class="btn btn-outline-light" data-action="template-save-as">
        <i class="bi bi-save2"></i>
        <span class="ms-1">Speichern als</span>
      </button>
      <button type="button" class="btn btn-outline-light" data-action="template-duplicate">
        <i class="bi bi-files"></i>
        <span class="ms-1">Duplizieren</span>
      </button>
    </div>
    <div class="btn-group btn-group-sm" role="group" aria-label="Ausgabe">
      <button type="button" class="btn btn-outline-light" data-action="template-preview">
        <i class="bi bi-eye"></i>
        <span class="ms-1">Vorschau</span>
      </button>
    </div>
    <div class="ms-auto d-flex align-items-center gap-2">
      <div class="btn-group btn-group-sm" role="group" aria-label="Rasteroptionen">
        <button type="button" class="btn btn-outline-light" data-action="toggle-grid" aria-pressed="false">
          <i class="bi bi-grid"></i>
          <span class="ms-1">Raster</span>
        </button>
        <button type="button" class="btn btn-outline-light" data-action="toggle-snap" aria-pressed="false">
          <i class="bi bi-magnet"></i>
          <span class="ms-1">Einrasten</span>
        </button>
      </div>
      <div class="btn-group btn-group-sm" role="group" aria-label="Verlauf">
        <button type="button" class="btn btn-outline-light" data-action="template-undo" disabled aria-disabled="true">
          <i class="bi bi-arrow-counterclockwise"></i>
          <span class="ms-1">Rückgängig</span>
        </button>
        <button type="button" class="btn btn-outline-light" data-action="template-redo" disabled aria-disabled="true">
          <i class="bi bi-arrow-clockwise"></i>
          <span class="ms-1">Wiederholen</span>
        </button>
      </div>
    </div>
  `;
  return toolbar;
}

function buildPaletteList(): HTMLElement {
  const list = document.createElement("div");
  list.className = "template-editor__palette-list";

  PALETTE_ITEMS.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "template-editor__palette-item text-start";
    button.dataset.paletteItem = item.type;
    button.setAttribute("data-palette-item", item.type);
    button.draggable = true;
    button.innerHTML = /* html */ `
      <div class="d-flex gap-3 align-items-start">
        <span class="fs-4"><i class="bi ${item.icon}"></i></span>
        <div>
          <h4 class="mb-1">${item.label}</h4>
          <p class="mb-0">${item.description}</p>
        </div>
      </div>
    `;
    list.appendChild(button);
  });

  return list;
}

function buildPropertyPanel(): HTMLElement {
  const panel = document.createElement("div");
  panel.className = "d-flex flex-column gap-3";
  panel.innerHTML = /* html */ `
    <div class="template-editor__property-empty" data-role="property-empty">
      <p class="mb-0">Wähle ein Feld, um die Eigenschaften zu bearbeiten.</p>
    </div>
    <div class="d-flex flex-column gap-3 d-none" data-role="property-groups" aria-disabled="true" data-field-id="">
      <section class="template-editor__property-group">
        <div class="row g-2">
          <div class="col-12">
            <label for="property-name" class="form-label">Technischer Name</label>
            <input type="text" class="form-control form-control-sm" id="property-name" placeholder="Feldname" autocomplete="off" />
          </div>
          <div class="col-12">
            <label for="property-label" class="form-label">Bezeichnung</label>
            <input type="text" class="form-control form-control-sm" id="property-label" placeholder="Beschriftung" />
          </div>
          <div class="col-6">
            <div class="form-check mt-2">
              <input class="form-check-input" type="checkbox" value="1" id="property-required" />
              <label class="form-check-label" for="property-required">Pflichtfeld</label>
            </div>
          </div>
        </div>
      </section>
      <section class="template-editor__property-group" data-property-advanced="text">
        <label for="property-placeholder" class="form-label">Platzhalter (nur Text)</label>
        <input type="text" class="form-control form-control-sm" id="property-placeholder" placeholder="Platzhalter" data-default-placeholder="Platzhalter" />
      </section>
      <section class="template-editor__property-group">
        <div class="row g-2">
          <div class="col-6">
            <label for="property-width" class="form-label">Breite (Raster)</label>
            <input type="number" min="1" max="16" class="form-control form-control-sm" name="property-width" id="property-width" placeholder="Breite" />
          </div>
          <div class="col-6">
            <label for="property-height" class="form-label">Höhe (Raster)</label>
            <input type="number" min="1" max="16" class="form-control form-control-sm" name="property-height" id="property-height" placeholder="Höhe" />
          </div>
          <div class="col-6">
            <label for="property-x" class="form-label">Spalte</label>
            <input type="number" min="0" class="form-control form-control-sm" name="property-x" id="property-x" placeholder="Spalte" />
          </div>
          <div class="col-6">
            <label for="property-y" class="form-label">Zeile</label>
            <input type="number" min="0" class="form-control form-control-sm" name="property-y" id="property-y" placeholder="Zeile" />
          </div>
        </div>
      </section>
    </div>
    <section class="template-editor__property-group template-editor__property-multi d-none" data-role="property-multi">
      <div class="template-editor__property-multi-summary" data-role="property-multi-summary">
        <p class="mb-0">Mehrere Felder ausgewählt.</p>
      </div>
      <div class="row g-2">
        <div class="col-12">
          <label class="form-label" for="property-multi-label">Bezeichnung</label>
          <input type="text" class="form-control form-control-sm" data-role="property-multi-label" id="property-multi-label" placeholder="Beschriftung" data-default-placeholder="Beschriftung" data-mixed-placeholder="-" />
        </div>
        <div class="col-12" data-property-multi-advanced="text">
          <label class="form-label" for="property-multi-placeholder">Platzhalter (Textfelder)</label>
          <input type="text" class="form-control form-control-sm" data-role="property-multi-placeholder" id="property-multi-placeholder" placeholder="Platzhalter" data-default-placeholder="Platzhalter" data-mixed-placeholder="-" />
        </div>
        <div class="col-12">
          <label class="form-label">Pflichtfeld</label>
          <div class="btn-group btn-group-sm w-100" role="group">
            <button type="button" class="btn btn-outline-light" data-role="property-multi-required" data-required="false" aria-pressed="false">Optional</button>
            <button type="button" class="btn btn-outline-light" data-role="property-multi-required" data-required="true" aria-pressed="false">Pflicht</button>
          </div>
        </div>
        <div class="col-6">
          <label class="form-label" for="property-multi-width">Breite (Raster)</label>
          <input type="number" min="1" max="16" class="form-control form-control-sm" data-role="property-multi-width" id="property-multi-width" placeholder="Breite" data-default-placeholder="Breite" data-mixed-placeholder="-" />
        </div>
        <div class="col-6">
          <label class="form-label" for="property-multi-height">Höhe (Raster)</label>
          <input type="number" min="1" max="16" class="form-control form-control-sm" data-role="property-multi-height" id="property-multi-height" placeholder="Höhe" data-default-placeholder="Höhe" data-mixed-placeholder="-" />
        </div>
        <div class="col-6">
          <label class="form-label" for="property-multi-x">Spalte</label>
          <input type="number" min="0" class="form-control form-control-sm" data-role="property-multi-x" id="property-multi-x" placeholder="Spalte" data-default-placeholder="Spalte" data-mixed-placeholder="-" />
        </div>
        <div class="col-6">
          <label class="form-label" for="property-multi-y">Zeile</label>
          <input type="number" min="0" class="form-control form-control-sm" data-role="property-multi-y" id="property-multi-y" placeholder="Zeile" data-default-placeholder="Zeile" data-mixed-placeholder="-" />
        </div>
      </div>
    </section>
    <section class="template-editor__property-group" data-role="revision-panel">
      <header class="d-flex justify-content-between align-items-center mb-2">
        <h4 class="h6 mb-0">Versionen</h4>
        <button class="btn btn-outline-light btn-sm" data-action="revision-revert" disabled aria-disabled="true">
          <i class="bi bi-arrow-counterclockwise"></i>
          <span class="ms-1">Version laden</span>
        </button>
      </header>
      <div class="template-editor__property-empty" data-role="revision-empty">
        <p class="mb-0">Noch keine gespeicherten Versionen.</p>
      </div>
      <div class="template-editor__revision-list" data-role="revision-list" aria-live="polite"></div>
    </section>
  `;
  return panel;
}

export function createTemplateEditorView(): TemplateEditorView {
  const root = document.createElement("section");
  root.className = "template-editor";

  const toolbar = createToolbar();
  const body = document.createElement("div");
  body.className = "template-editor__body";

  const leftSidebar = document.createElement("aside");
  leftSidebar.className =
    "template-editor__sidebar template-editor__sidebar--left";
  leftSidebar.innerHTML = /* html */ `
    <div class="template-editor__sidebar-header">
      <h3 class="template-editor__sidebar-title">Vorlagen</h3>
      <div class="btn-group btn-group-sm" role="group" aria-label="Vorlagenverwaltung">
        <button class="btn btn-outline-light" data-action="template-list-refresh" title="Aktualisieren">
          <i class="bi bi-arrow-repeat"></i>
        </button>
        <button class="btn btn-outline-light" data-action="template-list-delete" title="Löschen">
          <i class="bi bi-trash"></i>
        </button>
      </div>
    </div>
    <select class="form-select form-select-sm template-editor__dropdown" data-role="template-select">
      <option value="">Neue Vorlage erstellen…</option>
    </select>
    <div class="input-group input-group-sm mb-3">
      <span class="input-group-text"><i class="bi bi-search"></i></span>
      <input type="search" class="form-control" placeholder="Vorlagen suchen" data-role="template-search" />
    </div>
    <h4 class="template-editor__sidebar-title">Bausteine</h4>
  `;

  const paletteScroll = document.createElement("div");
  paletteScroll.className = "template-editor__sidebar-scroll";
  const paletteList = buildPaletteList();
  paletteScroll.appendChild(paletteList);
  leftSidebar.appendChild(paletteScroll);

  const canvas = document.createElement("main");
  canvas.className = "template-editor__canvas";
  canvas.innerHTML = /* html */ `
    <div class="template-editor__paper-wrapper" data-role="paper-wrapper" data-zoom="0.85">
      <div class="template-editor__paper-meta">
        <span>Vorlage: <strong data-role="template-name">Unbenannt</strong></span>
        <span>Version: <strong data-role="template-version">Entwurf</strong></span>
        <span data-role="template-updated">Noch nicht gespeichert</span>
        <span data-role="template-dimensions">A4 - Raster 12 x 16</span>
        <span class="template-editor__paper-meta-summary d-none" data-role="template-summary"></span>
      </div>
      <div class="template-editor__paper" data-role="template-paper">
        <div class="template-editor__grid" data-role="grid"></div>
        <div class="template-editor__field-layer" data-role="field-layer"></div>
        <div class="template-editor__selection-layer" data-role="selection-layer" aria-hidden="true"></div>
        <div class="template-editor__drop-indicator d-none" data-role="drop-indicator" aria-hidden="true"></div>
        <div class="template-editor__empty-state" data-role="empty-state">
          <i class="bi bi-bounding-box fs-3 mb-2"></i>
          <p class="mb-0">Ziehe Bausteine hierher oder nutze die Palette, um Felder zu platzieren.</p>
        </div>
        <div class="template-editor__zoom-controls" data-role="zoom-controls" aria-label="Zoomsteuerung">
          <button type="button" data-action="zoom-out" title="Zoom verkleinern" aria-label="Zoom verkleinern">&minus;</button>
          <span data-role="zoom-indicator" aria-live="polite">85%</span>
          <button type="button" data-action="zoom-in" title="Zoom vergrößern" aria-label="Zoom vergrößern">+</button>
        </div>
      </div>
    </div>
  `;

  const rightSidebar = document.createElement("aside");
  rightSidebar.className =
    "template-editor__sidebar template-editor__sidebar--right";
  rightSidebar.innerHTML = /* html */ `
    <div class="template-editor__sidebar-header">
      <h3 class="template-editor__sidebar-title">Eigenschaften</h3>
      <div class="btn-group btn-group-sm" role="group" aria-label="Eigenschaften">
        <button class="btn btn-outline-light" data-action="collapse-properties" title="Einklappen">
          <i class="bi bi-chevron-bar-right"></i>
        </button>
      </div>
    </div>
  `;

  const propertyScroll = document.createElement("div");
  propertyScroll.className = "template-editor__sidebar-scroll";
  const propertyPanel = buildPropertyPanel();
  propertyScroll.appendChild(propertyPanel);
  rightSidebar.appendChild(propertyScroll);

  body.append(leftSidebar, canvas, rightSidebar);
  root.append(toolbar, body);

  const contextMenu = document.createElement("div");
  contextMenu.className = "template-editor__context-menu d-none";
  contextMenu.setAttribute("role", "menu");
  contextMenu.innerHTML = /* html */ `
    <button type="button" class="template-editor__context-menu-item" data-menu-action="duplicate" role="menuitem">
      <i class="bi bi-files"></i>
      <span>Duplizieren</span>
    </button>
    <button type="button" class="template-editor__context-menu-item" data-menu-action="delete" role="menuitem">
      <i class="bi bi-trash"></i>
      <span>Löschen</span>
    </button>
    <hr class="template-editor__context-menu-separator" />
    <button type="button" class="template-editor__context-menu-item" data-menu-action="bring-front" role="menuitem">
      <i class="bi bi-bring-front"></i>
      <span>Nach vorne</span>
    </button>
    <button type="button" class="template-editor__context-menu-item" data-menu-action="send-back" role="menuitem">
      <i class="bi bi-send-back"></i>
      <span>Nach hinten</span>
    </button>
  `;

  const notifications = document.createElement("div");
  notifications.className = "template-editor__notifications";
  notifications.setAttribute("aria-live", "polite");
  notifications.setAttribute("aria-atomic", "true");

  root.append(notifications, contextMenu);

  const paperWrapper = canvas.querySelector<HTMLElement>(
    "[data-role='paper-wrapper']"
  );
  const paper = canvas.querySelector<HTMLElement>(
    "[data-role='template-paper']"
  );
  const fieldLayer = canvas.querySelector<HTMLElement>(
    "[data-role='field-layer']"
  );
  const selectionLayer = canvas.querySelector<HTMLElement>(
    "[data-role='selection-layer']"
  );
  const grid = canvas.querySelector<HTMLElement>("[data-role='grid']");

  if (!paperWrapper || !paper || !fieldLayer || !selectionLayer || !grid) {
    throw new Error("Template editor markup missing required layers.");
  }

  paperWrapper.style.transform = "scale(0.85)";
  if (!paperWrapper.dataset.zoom) {
    paperWrapper.dataset.zoom = "0.85";
  }

  const emptyState = canvas.querySelector<HTMLElement>(
    "[data-role='empty-state']"
  );
  if (emptyState && !emptyState.dataset.defaultMessage) {
    emptyState.dataset.defaultMessage = emptyState.innerHTML;
  }

  const emptyPropertyState = propertyPanel.querySelector<HTMLElement>(
    "[data-role='property-empty']"
  );
  if (emptyPropertyState && !emptyPropertyState.dataset.defaultMessage) {
    emptyPropertyState.dataset.defaultMessage = emptyPropertyState.innerHTML;
  }

  const propertyMultiPanel = propertyPanel.querySelector<HTMLElement>(
    "[data-role='property-multi']"
  );
  const propertyMultiSummary = propertyPanel.querySelector<HTMLElement>(
    "[data-role='property-multi-summary']"
  );
  const propertyMultiInputs = {
    label: propertyPanel.querySelector<HTMLInputElement>(
      "[data-role='property-multi-label']"
    ),
    placeholder: propertyPanel.querySelector<HTMLInputElement>(
      "[data-role='property-multi-placeholder']"
    ),
    width: propertyPanel.querySelector<HTMLInputElement>(
      "[data-role='property-multi-width']"
    ),
    height: propertyPanel.querySelector<HTMLInputElement>(
      "[data-role='property-multi-height']"
    ),
    x: propertyPanel.querySelector<HTMLInputElement>(
      "[data-role='property-multi-x']"
    ),
    y: propertyPanel.querySelector<HTMLInputElement>(
      "[data-role='property-multi-y']"
    ),
  } as const;

  return {
    root,
    toolbar,
    paletteList,
    paperWrapper,
    paper,
    fieldLayer,
    selectionLayer,
    dropIndicator: canvas.querySelector<HTMLElement>(
      "[data-role='drop-indicator']"
    ),
    grid,
    emptyState,
    templateNameLabel: canvas.querySelector<HTMLElement>(
      "[data-role='template-name']"
    ),
    templateVersionLabel: canvas.querySelector<HTMLElement>(
      "[data-role='template-version']"
    ),
    templateUpdatedLabel: canvas.querySelector<HTMLElement>(
      "[data-role='template-updated']"
    ),
    templateSummaryLabel: canvas.querySelector<HTMLElement>(
      "[data-role='template-summary']"
    ),
    propertyPanel,
    propertyGroups: propertyPanel.querySelector<HTMLElement>(
      "[data-role='property-groups']"
    ),
    propertyAdvancedSections: Array.from(
      propertyPanel.querySelectorAll<HTMLElement>("[data-property-advanced]")
    ),
    propertyInputs: {
      name: propertyPanel.querySelector<HTMLInputElement>("#property-name"),
      label: propertyPanel.querySelector<HTMLInputElement>("#property-label"),
      placeholder: propertyPanel.querySelector<HTMLInputElement>(
        "#property-placeholder"
      ),
      required:
        propertyPanel.querySelector<HTMLInputElement>("#property-required"),
      width: propertyPanel.querySelector<HTMLInputElement>(
        "input[name='property-width']"
      ),
      height: propertyPanel.querySelector<HTMLInputElement>(
        "input[name='property-height']"
      ),
      x: propertyPanel.querySelector<HTMLInputElement>(
        "input[name='property-x']"
      ),
      y: propertyPanel.querySelector<HTMLInputElement>(
        "input[name='property-y']"
      ),
    },
    propertyMultiPanel,
    propertyMultiSummary,
    propertyMultiInputs,
    propertyMultiAdvancedSections: Array.from(
      propertyPanel.querySelectorAll<HTMLElement>(
        "[data-property-multi-advanced]"
      )
    ),
    propertyMultiRequiredButtons: Array.from(
      propertyPanel.querySelectorAll<HTMLButtonElement>(
        "[data-role='property-multi-required']"
      )
    ),
    templateSelect: leftSidebar.querySelector<HTMLSelectElement>(
      "[data-role='template-select']"
    ),
    templateSearch: leftSidebar.querySelector<HTMLInputElement>(
      "[data-role='template-search']"
    ),
    templateDeleteButton: leftSidebar.querySelector<HTMLButtonElement>(
      "[data-action='template-list-delete']"
    ),
    templateRefreshButton: leftSidebar.querySelector<HTMLButtonElement>(
      "[data-action='template-list-refresh']"
    ),
    emptyPropertyState,
    contextMenu,
    notifications,
    zoomControls: canvas.querySelector<HTMLElement>(
      "[data-role='zoom-controls']"
    ),
    zoomIndicator: canvas.querySelector<HTMLElement>(
      "[data-role='zoom-indicator']"
    ),
    zoomInButton: canvas.querySelector<HTMLButtonElement>(
      "[data-action='zoom-in']"
    ),
    zoomOutButton: canvas.querySelector<HTMLButtonElement>(
      "[data-action='zoom-out']"
    ),
    revisionPanel: propertyPanel.querySelector<HTMLElement>(
      "[data-role='revision-panel']"
    ),
    revisionList: propertyPanel.querySelector<HTMLElement>(
      "[data-role='revision-list']"
    ),
    revisionEmpty: propertyPanel.querySelector<HTMLElement>(
      "[data-role='revision-empty']"
    ),
    revisionRevertButton: propertyPanel.querySelector<HTMLButtonElement>(
      "[data-action='revision-revert']"
    ),
  };
}
