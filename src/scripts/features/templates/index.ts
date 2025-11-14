import type { getState, subscribeState } from "@scripts/core/state";
import { updateSlice } from "@scripts/core/state";
import type { emit as emitEvent } from "@scripts/core/eventBus";
import { escapeHtml } from "@scripts/core/utils";
import { getDatabaseSnapshot } from "@scripts/core/database";
import {
  getActiveDriverKey,
  loadTemplateRevisionDocument,
  saveDatabase,
} from "@scripts/core/storage";
import { createTemplateEditorView } from "./editorView";
import type { TemplateEditorView } from "./editorView";
import {
  DEFAULT_FIELD_SIZE,
  DEFAULT_ZOOM,
  MAX_ZOOM,
  MIN_ZOOM,
  clampZoom,
  createEditorStore,
  type EditorStore,
} from "./editorState";
import type {
  EditorField,
  EditorState,
  FieldLayout,
  FieldType,
  TemplateDocument,
  TemplateRevision,
} from "./types";
import {
  cloneTemplateDocument,
  createRevisionSnapshotFromState,
  createTemplateDocumentFromState,
  ensureUniqueTemplateName,
  generateTemplateId,
  normalizeRevisionSnapshotFromDocument,
} from "./persistence";

// eslint-disable-next-line import/extensions
import { loadEditorPreferences, saveEditorPreferences } from "./preferences.js";
import {
  GRID_CELL_SIZE,
  GRID_COLUMNS,
  GRID_ROWS,
  GRID_PADDING,
  clampLayout,
  layoutToStyle,
  pixelRectFromElement,
  pixelsToGrid,
} from "./utils/grid";

interface Services {
  state: {
    getState: typeof getState;
    subscribe: typeof subscribeState;
  };
  events: {
    emit: typeof emitEvent;
  };
}

let initialized = false;
let host: HTMLElement | null = null;
const pendingRevisionSnapshotLoads = new Map<string, Promise<void>>();
let activeView: TemplateEditorView | null = null;

type NotificationKind = "info" | "success" | "warning" | "error";

interface NotificationOptions {
  kind?: NotificationKind;
  duration?: number;
  id?: string;
}

interface TemplateSaveOptions {
  silent?: boolean;
  summary?: string;
}

const notificationTimers = new WeakMap<HTMLElement, number>();
const notificationRegistry = new Map<string, HTMLElement>();

const AUTO_SAVE_DELAY_MS = 5000;
let autoSaveTimer: number | null = null;
let autoSaveInFlight: Promise<void> | null = null;
type AutoSaveStatus = "idle" | "pending" | "saving" | "error";
let autoSaveStatus: AutoSaveStatus = "idle";
let activeStore: EditorStore | null = null;
let lastRenderedState: EditorState | null = null;
let lastRenderedFieldsRef: EditorField[] | null = null;

const ZOOM_STEP = 0.05;
const ZOOM_EPSILON = 0.0001;

const FIELD_TYPE_LABELS: Record<FieldType, string> = {
  label: "Label",
  text: "Text",
  number: "Zahl",
};

const PALETTE_DRAG_MIME = "application/x-template-field";
const DROP_TARGET_CLASS = "is-drop-target";

function adjustZoomValue(current: number, stepDelta: number): number {
  const raw = current + stepDelta * ZOOM_STEP;
  const stepped = Math.round(raw / ZOOM_STEP) * ZOOM_STEP;
  return clampZoom(Number(stepped.toFixed(2)));
}

function isValidFieldType(value: unknown): value is FieldType {
  return value === "label" || value === "text" || value === "number";
}

function deriveRevisionContext(state: EditorState): {
  viewingVersion: number;
  activeRevision: TemplateRevision | null;
  isViewingCurrent: boolean;
} {
  const viewingVersion =
    typeof state.selectedRevisionVersion === "number"
      ? state.selectedRevisionVersion
      : state.version;
  const activeRevision =
    state.revisions.find(
      (revision) => revision && revision.version === viewingVersion
    ) ?? null;
  const isViewingCurrent =
    !state.templateId || viewingVersion === state.version;
  return { viewingVersion, activeRevision, isViewingCurrent };
}

function ensureUpdatedLabelParts(label: HTMLElement): {
  textSpan: HTMLSpanElement;
  indicator: HTMLSpanElement;
} {
  let textSpan = label.querySelector<HTMLSpanElement>(
    ".template-editor__updated-text"
  );
  if (!textSpan) {
    textSpan = document.createElement("span");
    textSpan.className = "template-editor__updated-text";
    textSpan.textContent = label.textContent ?? "";
    label.textContent = "";
    label.appendChild(textSpan);
  }

  let indicator = label.querySelector<HTMLSpanElement>(
    ".template-editor__autosave-indicator"
  );
  if (!indicator) {
    indicator = document.createElement("span");
    indicator.className = "template-editor__autosave-indicator";
    indicator.setAttribute("aria-hidden", "true");
    label.appendChild(indicator);
  }

  return { textSpan, indicator };
}

function applyAutoSaveVisual(
  view: TemplateEditorView,
  state: EditorState
): void {
  const label = view.templateUpdatedLabel;
  if (!label) {
    lastRenderedState = state;
    return;
  }

  const { viewingVersion, activeRevision, isViewingCurrent } =
    deriveRevisionContext(state);
  const { textSpan, indicator } = ensureUpdatedLabelParts(label);

  label.classList.remove("text-warning", "text-info", "text-danger");

  let message = "";
  let title = "";
  let indicatorStatus: AutoSaveStatus = "idle";
  const autoSaveEligible = shouldAutoSave(state);

  if (state.dirty) {
    if (!state.templateId) {
      message = "Änderungen nicht gespeichert";
      title = "Die Vorlage wurde noch nicht gespeichert.";
      label.classList.add("text-warning");
    } else if (!isViewingCurrent) {
      message = "Entwurf geändert (Revision angezeigt)";
      title =
        "Es liegen Änderungen vor, während eine ältere Revision angezeigt wird.";
      label.classList.add("text-warning");
    } else {
      switch (autoSaveStatus) {
        case "saving":
          message = "Automatisches Speichern…";
          title = "Die Änderungen werden gerade automatisch gespeichert.";
          label.classList.add("text-info");
          indicatorStatus = "saving";
          break;
        case "pending":
          message = "Automatisches Speichern in Kürze…";
          title =
            "Die Änderungen werden in wenigen Sekunden automatisch gespeichert.";
          label.classList.add("text-info");
          indicatorStatus = "pending";
          break;
        case "error":
          message = "Auto-Speichern fehlgeschlagen";
          title =
            "Das automatische Speichern ist fehlgeschlagen. Bitte manuell sichern.";
          label.classList.add("text-danger");
          indicatorStatus = "error";
          break;
        default:
          if (autoSaveEligible) {
            message = "Automatisches Speichern in Kürze…";
            title =
              "Die Änderungen werden in wenigen Sekunden automatisch gespeichert.";
            label.classList.add("text-info");
            indicatorStatus = "pending";
          } else {
            message = "Änderungen nicht gespeichert";
            title = "Es liegen ungesicherte Änderungen vor.";
            label.classList.add("text-warning");
          }
          break;
      }
    }
  } else if (!isViewingCurrent && activeRevision) {
    const timestamp =
      buildUpdatedTitle(activeRevision.updatedAt ?? "") ||
      activeRevision.updatedAt ||
      "";
    message = `Revision vom ${timestamp}`;
    title = `Revision v${viewingVersion} gespeichert am ${timestamp}`;
  } else if (state.updatedAt) {
    const formatted = buildUpdatedTitle(state.updatedAt);
    message = `Gespeichert: ${formatted}`;
    title = `Zuletzt gespeichert am ${formatted}`;
  } else {
    message = "Noch nicht gespeichert";
    title = "Diese Vorlage wurde noch nicht gespeichert.";
  }

  textSpan.textContent = message;
  label.title = title;

  indicator.dataset.status = indicatorStatus;
  if (indicatorStatus === "pending") {
    indicator.setAttribute("aria-label", "Automatisches Speichern geplant");
  } else if (indicatorStatus === "saving") {
    indicator.setAttribute("aria-label", "Automatisches Speichern aktiv");
  } else if (indicatorStatus === "error") {
    indicator.setAttribute(
      "aria-label",
      "Automatisches Speichern fehlgeschlagen"
    );
  } else {
    indicator.removeAttribute("aria-label");
  }

  lastRenderedState = state;
}

function setAutoSaveStatus(
  status: AutoSaveStatus,
  view?: TemplateEditorView | null,
  state?: EditorState | null
): void {
  autoSaveStatus = status;
  const targetView = view ?? activeView;
  const sourceState =
    state ?? lastRenderedState ?? activeStore?.getState() ?? null;
  if (targetView && sourceState) {
    applyAutoSaveVisual(targetView, sourceState);
  }
}

function hideTemplateNotification(toast: HTMLElement): void {
  if (toast.classList.contains("is-dismissing")) {
    return;
  }
  toast.classList.add("is-dismissing");
  const timer = notificationTimers.get(toast);
  if (typeof timer === "number") {
    window.clearTimeout(timer);
    notificationTimers.delete(toast);
  }
  const notificationId = toast.dataset.notificationId;
  if (notificationId) {
    notificationRegistry.delete(notificationId);
  }
  toast.classList.remove("is-visible");
  const remove = () => {
    toast.removeEventListener("transitionend", remove);
    toast.remove();
  };
  toast.addEventListener("transitionend", remove);
  window.setTimeout(remove, 400);
}

function showTemplateNotification(
  view: TemplateEditorView,
  message: string,
  options: NotificationOptions = {}
): void {
  const hostNode = view.notifications;
  if (!hostNode) {
    window.alert(message);
    return;
  }

  const kind = options.kind ?? "info";
  const duration = Number.isFinite(options.duration ?? 5000)
    ? Math.max(0, Number(options.duration ?? 5000))
    : 5000;

  if (options.id) {
    const existing = notificationRegistry.get(options.id);
    if (existing && existing.isConnected) {
      hideTemplateNotification(existing);
    }
  }

  while (hostNode.childElementCount >= 4) {
    const first = hostNode.firstElementChild as HTMLElement | null;
    if (!first) {
      break;
    }
    hideTemplateNotification(first);
    break;
  }

  const toast = document.createElement("div");
  toast.className = `template-editor__toast template-editor__toast--${kind}`;
  toast.setAttribute(
    "role",
    kind === "error" || kind === "warning" ? "alert" : "status"
  );
  toast.tabIndex = 0;
  toast.dataset.kind = kind;
  if (options.id) {
    toast.dataset.notificationId = options.id;
    notificationRegistry.set(options.id, toast);
  }

  const messageNode = document.createElement("span");
  messageNode.className = "template-editor__toast-message";
  messageNode.textContent = message;
  toast.appendChild(messageNode);

  toast.addEventListener("click", () => hideTemplateNotification(toast));
  toast.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      hideTemplateNotification(toast);
    }
  });

  hostNode.appendChild(toast);
  requestAnimationFrame(() => {
    toast.classList.add("is-visible");
  });

  if (duration > 0) {
    const timer = window.setTimeout(() => {
      hideTemplateNotification(toast);
    }, duration);
    notificationTimers.set(toast, timer);
  }
}

function emitTemplateNotification(
  message: string,
  options?: NotificationOptions
): void {
  if (activeView) {
    showTemplateNotification(activeView, message, options);
    return;
  }
  window.alert(message);
}

function cancelAutoSaveTimer(): void {
  if (autoSaveTimer !== null) {
    window.clearTimeout(autoSaveTimer);
    autoSaveTimer = null;
  }
}

function shouldAutoSave(state: EditorState): boolean {
  if (!state.templateId || !state.dirty) {
    return false;
  }
  if (
    typeof state.selectedRevisionVersion === "number" &&
    state.selectedRevisionVersion !== state.version
  ) {
    return false;
  }
  return true;
}

function scheduleAutoSave(
  view: TemplateEditorView,
  store: EditorStore,
  services: Services
): void {
  cancelAutoSaveTimer();
  setAutoSaveStatus("pending", view, store.getState());
  autoSaveTimer = window.setTimeout(() => {
    autoSaveTimer = null;
    const latest = store.getState();
    if (!shouldAutoSave(latest)) {
      setAutoSaveStatus("idle", view, latest);
      return;
    }
    setAutoSaveStatus("saving", view, latest);
    autoSaveInFlight = handleTemplateSave("save", store, services, view, {
      silent: true,
      summary: "Automatisch gespeichert",
    })
      .catch((error: unknown) => {
        console.error("Automatisches Speichern fehlgeschlagen", error);
        setAutoSaveStatus("error", view, store.getState());
        showTemplateNotification(
          view,
          "Automatisches Speichern fehlgeschlagen. Bitte manuell sichern.",
          {
            kind: "error",
            id: "auto-save-error",
            duration: 6000,
          }
        );
      })
      .finally(() => {
        autoSaveInFlight = null;
        const next = store.getState();
        if (shouldAutoSave(next)) {
          scheduleAutoSave(view, store, services);
        } else {
          setAutoSaveStatus("idle", view, next);
        }
      });
  }, AUTO_SAVE_DELAY_MS);
}

function evaluateAutoSave(
  view: TemplateEditorView,
  store: EditorStore,
  services: Services,
  state: EditorState
): void {
  if (!shouldAutoSave(state)) {
    cancelAutoSaveTimer();
    setAutoSaveStatus("idle", view, state);
    return;
  }
  if (autoSaveInFlight) {
    setAutoSaveStatus("saving", view, state);
    return;
  }
  scheduleAutoSave(view, store, services);
}

export function initTemplateEditor(
  container: Element | null,
  services: Services
): void {
  if (!container || initialized) {
    return;
  }

  host = container as HTMLElement;
  host.classList.add("template-editor-host");
  host.innerHTML = "";

  const view = createTemplateEditorView();
  host.appendChild(view.root);
  host.dataset.featureReady = "template-editor";
  activeView = view;

  const storedPreferences = loadEditorPreferences();
  const store = createEditorStore({
    initialPreferences: storedPreferences ?? undefined,
    onPreferencesChange: saveEditorPreferences,
  });
  activeStore = store;

  const unsubscribeStore = store.subscribe((state) => {
    renderEditor(view, store, state);
    if (view.templateSelect) {
      view.templateSelect.value = state.templateId ?? "";
    }
    evaluateAutoSave(view, store, services, state);
  });

  const syncTemplateList = setupTemplateList(view, store, services);

  setupPalette(view, store);
  setupPaletteDragAndDrop(view, store);
  setupToolbar(view, store, services);
  setupZoomControls(view, store);
  setupCanvasSelection(view, store);
  setupPropertyPanel(view, store);
  setupRevisionPanel(view, store);
  setupContextMenu(view, store);
  setupKeyboardShortcuts(view, store);

  const unsubscribeAppState = services.state.subscribe((nextState) => {
    const hasDatabase = Boolean(nextState.app?.hasDatabase);
    if (host) {
      host.classList.toggle("d-none", !hasDatabase);
    }
    syncTemplateList(nextState.templates ?? []);
  });
  host.classList.toggle("d-none", !services.state.getState().app?.hasDatabase);
  syncTemplateList(services.state.getState().templates ?? []);

  initialized = true;

  // Clean up if the host is removed later.
  const observer = new MutationObserver(() => {
    if (host && !document.body.contains(host)) {
      unsubscribeStore();
      unsubscribeAppState();
      if (activeView === view) {
        activeView = null;
      }
      if (activeStore === store) {
        activeStore = null;
      }
      cancelAutoSaveTimer();
      observer.disconnect();
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });
}

function renderEditor(
  view: TemplateEditorView,
  store: EditorStore,
  state: EditorState
): void {
  const { viewingVersion, activeRevision, isViewingCurrent } =
    deriveRevisionContext(state);

  if (view.templateNameLabel) {
    view.templateNameLabel.textContent = state.name || "Unbenannt";
  }
  if (view.templateVersionLabel) {
    if (!state.templateId) {
      const suffix = state.dirty ? " · geändert" : "";
      view.templateVersionLabel.textContent = `Entwurf${suffix}`;
      view.templateVersionLabel.title = "Entwurfsversion";
    } else if (!isViewingCurrent && activeRevision) {
      const suffix = state.dirty ? " · geändert" : "";
      view.templateVersionLabel.textContent = `v${viewingVersion} (aktuell v${state.version})${suffix}`;
      view.templateVersionLabel.title = `Revision v${viewingVersion} angezeigt · Aktuelle Version v${state.version}`;
    } else {
      const suffix = state.dirty ? " · geändert" : "";
      view.templateVersionLabel.textContent = `v${state.version}${suffix}`;
      view.templateVersionLabel.title = `Aktuelle Version v${state.version}`;
    }
  }
  applyAutoSaveVisual(view, state);
  if (view.templateSummaryLabel) {
    const activeSummary =
      activeRevision && activeRevision.summary
        ? activeRevision.summary.trim()
        : "";
    if (activeSummary) {
      const prefix = isViewingCurrent
        ? "Letzte Änderung:"
        : `Revision v${viewingVersion}:`;
      view.templateSummaryLabel.textContent = `${prefix} ${activeSummary}`;
      view.templateSummaryLabel.classList.remove("d-none");
      view.templateSummaryLabel.title = activeSummary;
    } else if (state.lastSummary) {
      view.templateSummaryLabel.textContent = `Letzte Änderung: ${state.lastSummary}`;
      view.templateSummaryLabel.classList.remove("d-none");
      view.templateSummaryLabel.title = state.lastSummary;
    } else {
      view.templateSummaryLabel.textContent = "";
      view.templateSummaryLabel.classList.add("d-none");
      view.templateSummaryLabel.title = "";
    }
  }
  view.grid.style.opacity = state.gridVisible ? "1" : "0";
  view.grid.style.visibility = state.gridVisible ? "visible" : "hidden";

  updateZoomControls(view, state);

  const empty = view.emptyState;
  if (empty) {
    empty.classList.toggle("d-none", state.fields.length > 0);
  }

  const layer = view.fieldLayer;
  const fieldsChanged = lastRenderedFieldsRef !== state.fields;
  if (fieldsChanged) {
    layer.innerHTML = "";

    const fragment = document.createDocumentFragment();
    for (const field of state.fields) {
      fragment.appendChild(createFieldElement(field, store, view, state));
    }
    layer.appendChild(fragment);
  } else {
    const selection = new Set(state.selectedFieldIds);
    layer
      .querySelectorAll<HTMLElement>(".template-editor__field")
      .forEach((element) => {
        const fieldId = element.dataset.fieldId;
        if (!fieldId) {
          return;
        }
        if (selection.has(fieldId)) {
          element.classList.add("is-selected");
          element.setAttribute("aria-selected", "true");
        } else {
          element.classList.remove("is-selected");
          element.removeAttribute("aria-selected");
        }
      });
  }

  updateSelectionOutline(view, state);

  updatePropertyPanel(view, state);
  updateToolbarToggles(view, state);
  updateRevisionPanel(view, state);

  lastRenderedFieldsRef = state.fields;
}

function createFieldElement(
  field: EditorField,
  store: EditorStore,
  view: TemplateEditorView,
  state: EditorState
): HTMLElement {
  const element = document.createElement("div");
  element.className = "template-editor__field";
  element.dataset.fieldId = field.id;

  const style = layoutToStyle(field.layout);
  element.style.left = style.left;
  element.style.top = style.top;
  element.style.width = style.width;
  element.style.height = style.height;
  element.style.zIndex = `${10 + (field.layout.layer ?? 0)}`;
  element.dataset.left = style.left.replace("px", "");
  element.dataset.top = style.top.replace("px", "");
  element.dataset.width = style.width.replace("px", "");
  element.dataset.height = style.height.replace("px", "");
  element.dataset.layer = String(field.layout.layer ?? 0);

  element.innerHTML = `
    <div class="template-editor__field-content">
      <span class="template-editor__field-label">${escapeHtml(
        field.label || field.name
      )}</span>
      <span class="template-editor__field-meta">${field.type.toUpperCase()}</span>
    </div>
    <div class="template-editor__field-handles" aria-hidden="true">
      <span class="template-editor__field-handle template-editor__field-handle--nw" data-handle="nw" data-edges="n w"></span>
      <span class="template-editor__field-handle template-editor__field-handle--n" data-handle="n" data-edges="n"></span>
      <span class="template-editor__field-handle template-editor__field-handle--ne" data-handle="ne" data-edges="n e"></span>
      <span class="template-editor__field-handle template-editor__field-handle--e" data-handle="e" data-edges="e"></span>
      <span class="template-editor__field-handle template-editor__field-handle--se" data-handle="se" data-edges="s e"></span>
      <span class="template-editor__field-handle template-editor__field-handle--s" data-handle="s" data-edges="s"></span>
      <span class="template-editor__field-handle template-editor__field-handle--sw" data-handle="sw" data-edges="s w"></span>
      <span class="template-editor__field-handle template-editor__field-handle--w" data-handle="w" data-edges="w"></span>
    </div>
  `;

  if (state.selectedFieldIds.includes(field.id)) {
    element.classList.add("is-selected");
    element.setAttribute("aria-selected", "true");
  } else {
    element.classList.remove("is-selected");
    element.removeAttribute("aria-selected");
  }

  element.tabIndex = 0;
  element.addEventListener("mousedown", (event) => {
    if ((event as MouseEvent).button !== 0) {
      return;
    }
    const mouseEvent = event as MouseEvent;
    if (mouseEvent.shiftKey || mouseEvent.metaKey || mouseEvent.ctrlKey) {
      const current = new Set(store.getState().selectedFieldIds);
      if (current.has(field.id)) {
        current.delete(field.id);
      } else {
        current.add(field.id);
      }
      store.selectFields(Array.from(current));
    } else {
      store.selectField(field.id);
    }
  });
  element.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      if (event.shiftKey || event.metaKey || event.ctrlKey) {
        const current = new Set(store.getState().selectedFieldIds);
        if (current.has(field.id)) {
          current.delete(field.id);
        } else {
          current.add(field.id);
        }
        store.selectFields(Array.from(current));
      } else {
        store.selectField(field.id);
      }
    }
  });

  setupFieldInteractions(element, view, store);

  return element;
}

function setupFieldInteractions(
  element: HTMLElement,
  view: TemplateEditorView,
  store: EditorStore
): void {
  const fieldId = element.dataset.fieldId;
  if (!fieldId) {
    return;
  }

  const getLayerBounds = () => {
    const layer = view.fieldLayer;
    const width = layer?.clientWidth ?? GRID_COLUMNS * GRID_CELL_SIZE;
    const height = layer?.clientHeight ?? GRID_ROWS * GRID_CELL_SIZE;
    return { width, height };
  };

  const getCellDimensions = () => {
    const meta = store.getState().layoutMeta;
    return {
      cellWidth: meta?.cellWidth ?? GRID_CELL_SIZE,
      cellHeight: meta?.cellHeight ?? GRID_CELL_SIZE,
    };
  };

  const updatePosition = (
    target: HTMLElement,
    left: number,
    top: number,
    width?: number,
    height?: number
  ) => {
    const normalizedLeft = Number(left.toFixed(2));
    const normalizedTop = Number(top.toFixed(2));
    target.dataset.left = `${normalizedLeft}`;
    target.dataset.top = `${normalizedTop}`;
    target.style.left = `${normalizedLeft}px`;
    target.style.top = `${normalizedTop}px`;
    if (typeof width === "number") {
      const normalizedWidth = Number(width.toFixed(2));
      target.dataset.width = `${normalizedWidth}`;
      target.style.width = `${normalizedWidth}px`;
    }
    if (typeof height === "number") {
      const normalizedHeight = Number(height.toFixed(2));
      target.dataset.height = `${normalizedHeight}`;
      target.style.height = `${normalizedHeight}px`;
    }
  };

  const startDrag = (event: PointerEvent) => {
    if (event.button !== 0) {
      return;
    }
    if (
      (event.target as HTMLElement | null)?.closest(
        ".template-editor__field-handle"
      )
    ) {
      return;
    }
    event.preventDefault();
    const pointerId = event.pointerId;
    const bounds = getLayerBounds();
    const { cellWidth, cellHeight } = getCellDimensions();
    const startLeft = parseFloat(element.dataset.left ?? "0");
    const startTop = parseFloat(element.dataset.top ?? "0");
    const width = parseFloat(element.dataset.width ?? `${element.offsetWidth}`);
    const height = parseFloat(
      element.dataset.height ?? `${element.offsetHeight}`
    );
    let moved = false;

    const handleMove = (moveEvent: PointerEvent) => {
      if (moveEvent.pointerId !== pointerId) {
        return;
      }
      moveEvent.preventDefault();
      const zoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
      const dx = (moveEvent.clientX - event.clientX) / zoom;
      const dy = (moveEvent.clientY - event.clientY) / zoom;
      let nextLeft = startLeft + dx;
      let nextTop = startTop + dy;
      if (store.getState().snapping) {
        nextLeft = Math.round(nextLeft / cellWidth) * cellWidth;
        nextTop = Math.round(nextTop / cellHeight) * cellHeight;
      }
      const maxLeft = Math.max(0, bounds.width - width);
      const maxTop = Math.max(0, bounds.height - height);
      nextLeft = Math.min(Math.max(0, nextLeft), maxLeft);
      nextTop = Math.min(Math.max(0, nextTop), maxTop);
      if (
        Math.abs(nextLeft - startLeft) > 0.01 ||
        Math.abs(nextTop - startTop) > 0.01
      ) {
        moved = true;
      }
      updatePosition(element, nextLeft, nextTop);
    };

    const finishDrag = () => {
      document.removeEventListener("pointermove", handleMove);
      document.removeEventListener("pointerup", finishDrag);
      document.removeEventListener("pointercancel", finishDrag);
      if (element.hasPointerCapture?.(pointerId)) {
        try {
          element.releasePointerCapture(pointerId);
        } catch {
          /* noop */
        }
      }
      if (moved) {
        const rect = pixelRectFromElement(element);
        const layout = pixelsToGrid(rect);
        store.updateFieldLayout(fieldId, layout);
      } else {
        updatePosition(element, startLeft, startTop);
      }
    };

    element.setPointerCapture(pointerId);
    document.addEventListener("pointermove", handleMove);
    document.addEventListener("pointerup", finishDrag);
    document.addEventListener("pointercancel", finishDrag);
  };

  const startResize = (event: PointerEvent, handle: HTMLElement) => {
    if (event.button !== 0) {
      return;
    }
    event.preventDefault();
    const pointerId = event.pointerId;
    const edges = handle.dataset.edges?.split(/\s+/).filter(Boolean) ?? [];
    if (!edges.length) {
      return;
    }
    const bounds = getLayerBounds();
    const { cellWidth, cellHeight } = getCellDimensions();
    const minWidth = cellWidth;
    const minHeight = cellHeight;
    const startLeft = parseFloat(element.dataset.left ?? "0");
    const startTop = parseFloat(element.dataset.top ?? "0");
    const startWidth = parseFloat(
      element.dataset.width ?? `${element.offsetWidth}`
    );
    const startHeight = parseFloat(
      element.dataset.height ?? `${element.offsetHeight}`
    );
    const startRight = startLeft + startWidth;
    const startBottom = startTop + startHeight;
    let changed = false;

    const handleMove = (moveEvent: PointerEvent) => {
      if (moveEvent.pointerId !== pointerId) {
        return;
      }
      moveEvent.preventDefault();
      const zoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
      const deltaX = (moveEvent.clientX - event.clientX) / zoom;
      const deltaY = (moveEvent.clientY - event.clientY) / zoom;
      const movingWest = edges.includes("w");
      const movingEast = edges.includes("e");
      const movingNorth = edges.includes("n");
      const movingSouth = edges.includes("s");

      const layerWidth = Math.max(bounds.width, minWidth);
      const layerHeight = Math.max(bounds.height, minHeight);

      let leftEdge = movingWest ? startLeft + deltaX : startLeft;
      let rightEdge = movingEast ? startRight + deltaX : startRight;
      let topEdge = movingNorth ? startTop + deltaY : startTop;
      let bottomEdge = movingSouth ? startBottom + deltaY : startBottom;

      if (movingWest) {
        leftEdge = Math.min(Math.max(0, leftEdge), rightEdge - minWidth);
      } else {
        leftEdge = startLeft;
      }

      if (movingEast) {
        rightEdge = Math.max(
          Math.min(layerWidth, rightEdge),
          leftEdge + minWidth
        );
      } else {
        rightEdge = startRight;
      }

      if (movingNorth) {
        topEdge = Math.min(Math.max(0, topEdge), bottomEdge - minHeight);
      } else {
        topEdge = startTop;
      }

      if (movingSouth) {
        bottomEdge = Math.max(
          Math.min(layerHeight, bottomEdge),
          topEdge + minHeight
        );
      } else {
        bottomEdge = startBottom;
      }

      if (store.getState().snapping) {
        if (movingWest) {
          leftEdge = Math.round(leftEdge / cellWidth) * cellWidth;
          leftEdge = Math.min(Math.max(0, leftEdge), rightEdge - minWidth);
        }
        if (movingEast) {
          rightEdge = Math.round(rightEdge / cellWidth) * cellWidth;
          rightEdge = Math.max(
            Math.min(layerWidth, rightEdge),
            leftEdge + minWidth
          );
        }
        if (movingNorth) {
          topEdge = Math.round(topEdge / cellHeight) * cellHeight;
          topEdge = Math.min(Math.max(0, topEdge), bottomEdge - minHeight);
        }
        if (movingSouth) {
          bottomEdge = Math.round(bottomEdge / cellHeight) * cellHeight;
          bottomEdge = Math.max(
            Math.min(layerHeight, bottomEdge),
            topEdge + minHeight
          );
        }
      }

      const nextWidth = rightEdge - leftEdge;
      const nextHeight = bottomEdge - topEdge;
      if (
        Math.abs(nextWidth - startWidth) > 0.01 ||
        Math.abs(nextHeight - startHeight) > 0.01 ||
        Math.abs(leftEdge - startLeft) > 0.01 ||
        Math.abs(topEdge - startTop) > 0.01
      ) {
        changed = true;
      }
      updatePosition(element, leftEdge, topEdge, nextWidth, nextHeight);
    };

    const finishResize = () => {
      document.removeEventListener("pointermove", handleMove);
      document.removeEventListener("pointerup", finishResize);
      document.removeEventListener("pointercancel", finishResize);
      if (element.hasPointerCapture?.(pointerId)) {
        try {
          element.releasePointerCapture(pointerId);
        } catch {
          /* noop */
        }
      }
      if (changed) {
        const rect = pixelRectFromElement(element);
        const layout = pixelsToGrid(rect);
        store.updateFieldLayout(fieldId, layout);
      } else {
        updatePosition(element, startLeft, startTop, startWidth, startHeight);
      }
    };

    element.setPointerCapture(pointerId);
    document.addEventListener("pointermove", handleMove);
    document.addEventListener("pointerup", finishResize);
    document.addEventListener("pointercancel", finishResize);
  };

  element.addEventListener("pointerdown", startDrag);
  element
    .querySelectorAll<HTMLElement>(".template-editor__field-handle")
    .forEach((handle) => {
      handle.addEventListener("pointerdown", (event) =>
        startResize(event, handle)
      );
    });
}

function setupPalette(view: TemplateEditorView, store: EditorStore): void {
  view.paletteList.addEventListener("click", (event) => {
    const item = (event.target as HTMLElement | null)?.closest<HTMLElement>(
      "[data-palette-item]"
    );
    if (!item || item.classList.contains("disabled")) {
      return;
    }
    const type = item.dataset.paletteItem as FieldType | undefined;
    if (!type) {
      return;
    }
    store.addField(type);
  });
}

type PaletteDragPayload = {
  type: FieldType;
};

function setupPaletteDragAndDrop(
  view: TemplateEditorView,
  store: EditorStore
): void {
  const dropIndicator = view.dropIndicator;
  const paper = view.paper;
  const fieldLayer = view.fieldLayer;
  if (!fieldLayer) {
    return;
  }

  let activeDragType: FieldType | null = null;
  let dropPosition: { x: number; y: number } | null = null;

  const enableDropTarget = () => {
    if (paper) {
      paper.classList.add(DROP_TARGET_CLASS);
    }
  };

  const disableDropTarget = () => {
    if (paper) {
      paper.classList.remove(DROP_TARGET_CLASS);
    }
  };

  const clearDropIndicator = () => {
    dropPosition = null;
    if (dropIndicator) {
      dropIndicator.classList.add("d-none");
    }
  };

  const resetDragState = () => {
    activeDragType = null;
    clearDropIndicator();
    disableDropTarget();
  };

  const updateDropIndicator = (clientX: number, clientY: number): void => {
    if (!dropIndicator || !activeDragType) {
      return;
    }
    const layerRect = fieldLayer.getBoundingClientRect();
    const zoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
    const localX = (clientX - layerRect.left) / zoom;
    const localY = (clientY - layerRect.top) / zoom;
    if (Number.isNaN(localX) || Number.isNaN(localY)) {
      clearDropIndicator();
      return;
    }

    const layoutMeta = store.getState().layoutMeta;
    const columns = layoutMeta?.columns ?? GRID_COLUMNS;
    const rows = layoutMeta?.rows ?? GRID_ROWS;
    const cellWidth = layoutMeta?.cellWidth ?? GRID_CELL_SIZE;
    const cellHeight = layoutMeta?.cellHeight ?? GRID_CELL_SIZE;
    if (localX < 0 || localY < 0) {
      clearDropIndicator();
      return;
    }

    const cellX = Math.floor(localX / cellWidth);
    const cellY = Math.floor(localY / cellHeight);
    const maxX = columns - DEFAULT_FIELD_SIZE.w;
    const maxY = rows - DEFAULT_FIELD_SIZE.h;

    if (cellX < 0 || cellY < 0 || cellX > maxX || cellY > maxY) {
      clearDropIndicator();
      return;
    }

    dropIndicator.style.left = `${cellX * cellWidth}px`;
  };

  const readDragType = (dataTransfer: DataTransfer): FieldType | null => {
    const custom = dataTransfer.getData(PALETTE_DRAG_MIME);
    if (custom) {
      try {
        const parsed = JSON.parse(custom) as PaletteDragPayload;
        if (parsed && isValidFieldType(parsed.type)) {
          return parsed.type;
        }
      } catch (error) {
        console.warn("Vorlageneditor: Ungültige Drag-Daten", error);
      }
    }
    const fallback = dataTransfer.getData("text/plain");
    if (isValidFieldType(fallback)) {
      return fallback;
    }
    return null;
  };

  const handleDragStart = (event: DragEvent) => {
    const item = (event.target as HTMLElement | null)?.closest<HTMLElement>(
      "[data-palette-item]"
    );
    if (!item) {
      return;
    }
    const type = item.dataset.paletteItem as FieldType | undefined;
    if (!type) {
      return;
    }
    if (item.classList.contains("disabled")) {
      event.preventDefault();
      return;
    }
    activeDragType = type;
    dropPosition = null;
    clearDropIndicator();
    enableDropTarget();
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = "copy";
      const payload: PaletteDragPayload = { type };
      event.dataTransfer.setData(PALETTE_DRAG_MIME, JSON.stringify(payload));
      event.dataTransfer.setData("text/plain", type);
    }
  };

  view.paletteList.addEventListener(
    "dragstart",
    handleDragStart as EventListener
  );
  view.paletteList.addEventListener("dragend", () => {
    resetDragState();
  });

  const handleDragOver = (event: DragEvent) => {
    if (!activeDragType) {
      return;
    }
    if (!event.dataTransfer) {
      return;
    }
    event.preventDefault();
    event.dataTransfer.dropEffect = "copy";
    updateDropIndicator(event.clientX, event.clientY);
  };

  const handleDragLeave = (event: DragEvent) => {
    if (!activeDragType) {
      return;
    }
    const related = event.relatedTarget as Node | null;
    if (related && paper?.contains(related)) {
      return;
    }
    clearDropIndicator();
    if (!related) {
      disableDropTarget();
    }
  };

  const handleDrop = (event: DragEvent) => {
    if (!activeDragType) {
      return;
    }
    event.preventDefault();
    const transferType = event.dataTransfer
      ? readDragType(event.dataTransfer)
      : null;
    const resolvedType = transferType ?? activeDragType;
    if (!resolvedType) {
      resetDragState();
      return;
    }
    if (!dropPosition) {
      updateDropIndicator(event.clientX, event.clientY);
    }
    const position = dropPosition;
    resetDragState();
    if (position) {
      store.addField(resolvedType, {
        x: position.x,
        y: position.y,
      });
    }
  };

  const registerDropTarget = (target: HTMLElement | null) => {
    if (!target) {
      return;
    }
    target.addEventListener("dragenter", (event) => {
      if (!activeDragType) {
        return;
      }
      event.preventDefault();
      enableDropTarget();
    });
    target.addEventListener("dragover", handleDragOver);
    target.addEventListener("dragleave", handleDragLeave);
    target.addEventListener("drop", handleDrop);
  };

  registerDropTarget(paper);
  registerDropTarget(fieldLayer);
  registerDropTarget(view.grid);

  window.addEventListener("dragend", resetDragState, { passive: true });
  window.addEventListener("drop", resetDragState);
}

function formatDateTime(value?: string | null): string {
  if (!value) {
    return new Date().toLocaleString("de-DE");
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return new Date().toLocaleString("de-DE");
  }
  return parsed.toLocaleString("de-DE");
}

type TemplateOutputPayload = {
  title: string;
  styles: string;
  content: string;
};

function buildTemplateOutput(state: EditorState): TemplateOutputPayload {
  const layoutMeta = state.layoutMeta ?? {
    columns: GRID_COLUMNS,
    rows: GRID_ROWS,
    cellWidth: GRID_CELL_SIZE,
    cellHeight: GRID_CELL_SIZE,
    padding: GRID_PADDING,
  };
  const columns = Math.max(1, layoutMeta.columns ?? GRID_COLUMNS);
  const rows = Math.max(1, layoutMeta.rows ?? GRID_ROWS);
  const cellWidth = layoutMeta.cellWidth ?? GRID_CELL_SIZE;
  const cellHeight = layoutMeta.cellHeight ?? GRID_CELL_SIZE;
  const padding = layoutMeta.padding ?? GRID_PADDING;
  const pageWidth = columns * cellWidth + padding * 2;
  const pageHeight = rows * cellHeight + padding * 2;

  const usableWidth = pageWidth - padding * 2;
  const columnWidth = usableWidth / columns;
  const rowHeight = (pageHeight - padding * 2) / rows;

  const title = state.name?.trim()
    ? `Vorlage - ${state.name.trim()}`
    : "Vorlage";

  const styles = `
    :root { color-scheme: only light; }
    body {
      background: #f5f6f8;
      margin: 0;
      padding: 32px;
      font-family: "Inter", "Helvetica Neue", Arial, sans-serif;
      color: #111;
      line-height: 1.45;
      display: flex;
      justify-content: center;
    }
    .template-print {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 24px;
      width: 100%;
    }
    .template-print__meta {
      width: ${pageWidth}px;
      max-width: 100%;
    }
    .template-print__meta h1 {
      margin: 0 0 0.35rem;
      font-size: 1.75rem;
      letter-spacing: 0.02em;
    }
    .template-print__description {
      margin: 0 0 1rem;
      font-size: 0.95rem;
      color: rgba(17, 17, 17, 0.72);
    }
    .template-print__meta-list {
      display: flex;
      flex-wrap: wrap;
      list-style: none;
      margin: 0;
      padding: 0;
      gap: 1.25rem;
      font-size: 0.85rem;
    }
    .template-print__meta-list li strong {
      font-weight: 600;
      color: rgba(17, 17, 17, 0.85);
    }
    .template-print__page {
      width: ${pageWidth}px;
      height: ${pageHeight}px;
      background: #fff;
      border-radius: 18px;
      border: 1px solid rgba(18, 21, 24, 0.16);
      box-shadow: 0 20px 60px rgba(15, 18, 24, 0.12);
      padding: ${padding}px;
      box-sizing: border-box;
      position: relative;
      overflow: hidden;
    }
    .template-print__grid {
      position: relative;
      width: 100%;
      height: 100%;
    }
    .template-print__field {
      position: absolute;
      display: flex;
      flex-direction: column;
      justify-content: flex-start;
      gap: 8px;
      padding: 12px 16px;
      box-sizing: border-box;
      background: rgba(246, 248, 250, 0.85);
      border: 1px solid rgba(18, 21, 24, 0.08);
      border-radius: 8px;
    }
    .template-print__field-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 0.95rem;
      letter-spacing: 0.01em;
      color: rgba(18, 21, 24, 0.9);
    }
    .template-print__field-label {
      font-weight: 600;
      color: #11131a;
    }
    .template-print__field-meta {
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: rgba(18, 21, 24, 0.5);
    }
    .template-print__field-body {
      flex: 1 1 auto;
      display: flex;
      align-items: flex-end;
    }
    .template-print__field-input {
      flex: 1 1 auto;
      display: block;
      border: none;
      min-height: ${Math.max(28, rowHeight - 24)}px;
      background: transparent;
      font-size: 0.92rem;
      color: rgba(18, 21, 24, 0.72);
      padding: 6px 0;
    }
    .template-print__field-input span {
      opacity: 0.6;
    }
    .template-print__field-input[data-type="number"] {
      font-variant-numeric: tabular-nums;
      text-align: right;
    }
    .template-print__field--label {
      justify-content: center;
      align-items: center;
      text-align: center;
      font-size: 1.05rem;
      font-weight: 600;
      letter-spacing: 0.05em;
      color: #101216;
      border: none;
      background: transparent;
    }
    .template-print__field--align-center { text-align: center; }
    .template-print__field--align-right {
      text-align: right;
      align-items: flex-end;
    }
    .template-print__label-text {
      display: block;
      width: 100%;
    }
    .template-print__field.is-required .template-print__field-label::after {
      content: "*";
      margin-left: 4px;
      color: #c62828;
      font-weight: 700;
    }
    @media print {
      body { background: #fff; padding: 0; }
      .template-print__page {
        box-shadow: none;
        border-color: transparent;
      }
      .template-print__meta {
        width: 100%;
        max-width: none;
      }
    }
  `;

  const safeName = escapeHtml(state.name || "Unbenannte Vorlage");
  const description = state.description?.trim();
  const metaList = "";

  const sortedFields = [...state.fields].sort((a, b) => {
    const layerDiff = (a.layout.layer ?? 0) - (b.layout.layer ?? 0);
    if (layerDiff !== 0) {
      return layerDiff;
    }
    if (a.layout.y !== b.layout.y) {
      return a.layout.y - b.layout.y;
    }
    return a.layout.x - b.layout.x;
  });

  const fieldsHtml = sortedFields
    .map((field) => renderTemplatePrintField(field))
    .join("\n");

  const descriptionBlock = description
    ? `<p class="template-print__description">${escapeHtml(description)}</p>`
    : "";

  const content = `
    <section class="template-print" aria-label="Vorlagenvorschau">
      <header class="template-print__meta">
        <h1>${safeName}</h1>
        ${descriptionBlock}
        ${metaList}
      </header>
      <div class="template-print__page" role="img" aria-label="Vorlage">
        <div class="template-print__grid">
          ${fieldsHtml || ""}
        </div>
      </div>
    </section>
  `;

  return {
    title,
    styles,
    content,
  };
}

function renderTemplatePrintField(field: EditorField): string {
  const layoutMeta = activeStore?.getState().layoutMeta ?? {
    columns: GRID_COLUMNS,
    rows: GRID_ROWS,
    cellWidth: GRID_CELL_SIZE,
    cellHeight: GRID_CELL_SIZE,
    padding: GRID_PADDING,
  };

  const cellWidth = layoutMeta.cellWidth ?? GRID_CELL_SIZE;
  const cellHeight = layoutMeta.cellHeight ?? GRID_CELL_SIZE;
  const padding = layoutMeta.padding ?? GRID_PADDING;

  const left = padding + field.layout.x * cellWidth;
  const top = padding + field.layout.y * cellHeight;
  const width = field.layout.w * cellWidth;
  const height = field.layout.h * cellHeight;
  const style = `left: ${left}px; top: ${top}px; width: ${width}px; height: ${height}px;`;
  const alignClass = field.layout.align
    ? ` template-print__field--align-${field.layout.align}`
    : "";
  const label = escapeHtml(field.label || field.name);
  if (field.type === "label") {
    return `<div class="template-print__field template-print__field--label${alignClass}" style="${style}"><span class="template-print__label-text">${label}</span></div>`;
  }

  const placeholder = field.placeholder?.trim()
    ? escapeHtml(field.placeholder.trim())
    : "";
  const typeLabel = FIELD_TYPE_LABELS[field.type] ?? field.type;

  return `<div class="template-print__field template-print__field--${field.type}${alignClass}${field.required ? " is-required" : ""}" style="${style}">
    <div class="template-print__field-header">
      <span class="template-print__field-label">${label}</span>
      <span class="template-print__field-meta">${escapeHtml(typeLabel)}</span>
    </div>
    <div class="template-print__field-body">
      <div class="template-print__field-input" data-type="${field.type}">
        <span class="template-print__input-placeholder">${placeholder}</span>
      </div>
    </div>
  </div>`;
}

function openTemplatePreviewWindow(payload: TemplateOutputPayload): boolean {
  let preview: Window | null = null;
  try {
    preview = window.open("about:blank", "_blank", "width=1024,height=768");
  } catch (error) {
    console.warn("Vorschau konnte nicht geöffnet werden", error);
  }
  if (!preview) {
    return false;
  }

  const safeTitle = escapeHtml(payload.title);
  let wrote = false;
  let blobUrl: string | null = null;
  try {
    const doc = preview.document;
    doc.open();
    doc.write(
      '<!DOCTYPE html><html lang="de"><head><meta charset="utf-8" /><title>' +
        safeTitle +
        "</title><style>" +
        payload.styles +
        "</style></head><body></body></html>"
    );
    doc.close();
    const body = doc.body;
    if (body) {
      body.innerHTML = payload.content;
    }
    wrote = true;
  } catch (error) {
    console.warn(
      "Direktes Schreiben in die Vorschau ist fehlgeschlagen",
      error
    );
  }

  if (!wrote) {
    try {
      const html = `<!DOCTYPE html>
  <html lang="de">
    <head>
      <meta charset="utf-8" />
      <title>${safeTitle}</title>
      <style>${payload.styles}</style>
    </head>
    <body>
      ${payload.content}
    </body>
  </html>`;
      const blob = new Blob([html], { type: "text/html" });
      blobUrl = URL.createObjectURL(blob);
      preview.location.href = blobUrl;
      preview.addEventListener(
        "load",
        () => {
          if (blobUrl) {
            URL.revokeObjectURL(blobUrl);
            blobUrl = null;
          }
          try {
            (preview as Window).focus();
          } catch (focusError) {
            console.warn(
              "Vorschaufenster konnte nach Blob-Ladung nicht fokussiert werden",
              focusError
            );
          }
        },
        { once: true }
      );
      wrote = true;
    } catch (blobError) {
      console.error(
        "Fallback zum Laden der Vorschau aus einem Blob ist fehlgeschlagen",
        blobError
      );
      return false;
    }
  }

  if (wrote) {
    try {
      preview.focus();
    } catch (error) {
      console.warn("Vorschaufenster konnte nicht fokussiert werden", error);
    }
  }
  return wrote;
}

function handleTemplatePreview(
  view: TemplateEditorView,
  store: EditorStore
): void {
  const state = store.getState();
  if (!state.fields.length) {
    showTemplateNotification(
      view,
      "Füge mindestens ein Feld hinzu, um eine Vorschau zu öffnen.",
      {
        kind: "info",
        duration: 3500,
      }
    );
    return;
  }

  const payload = buildTemplateOutput(state);
  const opened = openTemplatePreviewWindow(payload);
  if (!opened) {
    showTemplateNotification(
      view,
      "Vorschau konnte nicht geöffnet werden. Bitte Pop-up Blocker prüfen.",
      {
        kind: "warning",
        duration: 5000,
      }
    );
  }
}

function setupToolbar(
  view: TemplateEditorView,
  store: EditorStore,
  services: Services
): void {
  view.toolbar.addEventListener("click", (event) => {
    const button = (
      event.target as HTMLElement | null
    )?.closest<HTMLButtonElement>("[data-action]");
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    switch (action) {
      case "template-new":
        store.reset();
        break;
      case "template-save":
        void handleTemplateSave("save", store, services, view).catch(
          (error: unknown) => {
            console.error("Speichern der Vorlage fehlgeschlagen", error);
          }
        );
        break;
      case "template-save-as":
        void handleTemplateSave("saveAs", store, services, view).catch(
          (error: unknown) => {
            console.error("Speichern unter fehlgeschlagen", error);
          }
        );
        break;
      case "template-duplicate":
        void handleTemplateSave("duplicate", store, services, view).catch(
          (error: unknown) => {
            console.error("Duplizieren der Vorlage fehlgeschlagen", error);
          }
        );
        break;
      case "toggle-grid":
        store.toggleGrid();
        break;
      case "toggle-snap":
        store.toggleSnapping();
        break;
      case "template-preview":
        handleTemplatePreview(view, store);
        break;
      case "template-undo":
        if (store.getState().undoStack.length) {
          store.undo();
        } else {
          showTemplateNotification(view, "Nichts zum Rückgängig machen.", {
            kind: "info",
            duration: 2500,
          });
        }
        break;
      case "template-redo":
        if (store.getState().redoStack.length) {
          store.redo();
        } else {
          showTemplateNotification(view, "Keine Schritte zum Wiederholen.", {
            kind: "info",
            duration: 2500,
          });
        }
        break;
      default:
        break;
    }
  });
}

function setupZoomControls(view: TemplateEditorView, store: EditorStore): void {
  const controls = view.zoomControls;
  if (!controls) {
    return;
  }

  const adjustZoom = (direction: "in" | "out", multiplier = 1): void => {
    const current = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
    const stepDelta = (direction === "in" ? 1 : -1) * multiplier;
    const next = adjustZoomValue(current, stepDelta);
    if (next !== current) {
      store.setZoom(next);
    }
  };

  const activateFromButton = (button: HTMLButtonElement): void => {
    const action = button.dataset.action;
    if (action === "zoom-in") {
      adjustZoom("in");
    } else if (action === "zoom-out") {
      adjustZoom("out");
    }
  };

  controls.addEventListener("click", (event) => {
    const button = (
      event.target as HTMLElement | null
    )?.closest<HTMLButtonElement>("[data-action]");
    if (!button) {
      return;
    }
    event.preventDefault();
    activateFromButton(button);
  });

  controls.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    const button = (
      event.target as HTMLElement | null
    )?.closest<HTMLButtonElement>("[data-action]");
    if (!button) {
      return;
    }
    event.preventDefault();
    activateFromButton(button);
  });

  if (view.paperWrapper) {
    view.paperWrapper.addEventListener(
      "wheel",
      (event) => {
        if (!event.ctrlKey && !event.metaKey) {
          return;
        }
        event.preventDefault();
        const direction = event.deltaY < 0 ? "in" : "out";
        const intensity = Math.min(
          3,
          Math.max(1, Math.round(Math.abs(event.deltaY) / 60))
        );
        adjustZoom(direction, intensity);
      },
      { passive: false }
    );
  }
}

function setupCanvasSelection(
  view: TemplateEditorView,
  store: EditorStore
): void {
  let marqueeElement: HTMLElement | null = null;
  let isSelecting = false;
  let originX = 0;
  let originY = 0;

  view.fieldLayer.addEventListener("mousedown", (event) => {
    if (event.target !== view.fieldLayer) {
      return;
    }

    isSelecting = true;
    const zoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
    originX = event.offsetX / zoom;
    originY = event.offsetY / zoom;
    marqueeElement = document.createElement("div");
    marqueeElement.className = "template-editor__selection-marquee";
    marqueeElement.style.left = `${originX}px`;
    marqueeElement.style.top = `${originY}px`;
    marqueeElement.style.width = "0px";
    marqueeElement.style.height = "0px";
    view.fieldLayer.appendChild(marqueeElement);

    store.selectField(null);
    event.preventDefault();
  });

  view.fieldLayer.addEventListener("mousemove", (event) => {
    if (!isSelecting || !marqueeElement) {
      return;
    }

    const zoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
    const currentX = event.offsetX / zoom;
    const currentY = event.offsetY / zoom;
    const left = Math.min(originX, currentX);
    const top = Math.min(originY, currentY);
    const width = Math.abs(currentX - originX);
    const height = Math.abs(currentY - originY);
    marqueeElement.style.left = `${left}px`;
    marqueeElement.style.top = `${top}px`;
    marqueeElement.style.width = `${width}px`;
    marqueeElement.style.height = `${height}px`;

    const selectedIds = computeMarqueeSelection(view, left, top, width, height);
    store.selectFields(selectedIds);
  });

  const finishSelection = () => {
    if (!isSelecting) {
      return;
    }
    isSelecting = false;
    if (marqueeElement) {
      marqueeElement.remove();
      marqueeElement = null;
    }
  };

  view.fieldLayer.addEventListener("mouseup", finishSelection);
  view.fieldLayer.addEventListener("mouseleave", () => {
    if (isSelecting) {
      finishSelection();
    }
  });
  window.addEventListener("mouseup", finishSelection);
}

function computeMarqueeSelection(
  view: TemplateEditorView,
  left: number,
  top: number,
  width: number,
  height: number
): string[] {
  if (width < 2 && height < 2) {
    return [];
  }

  const selectionRight = left + width;
  const selectionBottom = top + height;
  const selected = new Set<string>();

  view.fieldLayer
    .querySelectorAll<HTMLElement>(".template-editor__field")
    .forEach((field) => {
      const fieldId = field.dataset.fieldId;
      if (!fieldId) {
        return;
      }
      const fieldLeft = parseFloat(field.dataset.left ?? "0");
      const fieldTop = parseFloat(field.dataset.top ?? "0");
      const fieldWidth = parseFloat(
        field.dataset.width ?? `${field.offsetWidth}`
      );
      const fieldHeight = parseFloat(
        field.dataset.height ?? `${field.offsetHeight}`
      );
      const fieldRight = fieldLeft + fieldWidth;
      const fieldBottom = fieldTop + fieldHeight;
      const intersects =
        fieldLeft <= selectionRight &&
        fieldRight >= left &&
        fieldTop <= selectionBottom &&
        fieldBottom >= top;
      if (intersects) {
        selected.add(fieldId);
      }
    });

  return Array.from(selected);
}

function setupContextMenu(view: TemplateEditorView, store: EditorStore): void {
  const menu = view.contextMenu;

  const isMenuOpen = () => !menu.classList.contains("d-none");

  const hideMenu = () => {
    if (!isMenuOpen()) {
      return;
    }
    menu.classList.add("d-none");
    menu.style.left = "-9999px";
    menu.style.top = "-9999px";
  };

  const updateMenuState = (selectionCount: number) => {
    menu
      .querySelectorAll<HTMLButtonElement>("[data-menu-action]")
      .forEach((button) => {
        const disabled = selectionCount === 0;
        button.disabled = disabled;
        button.setAttribute("aria-disabled", disabled ? "true" : "false");
      });
  };

  const openMenu = (clientX: number, clientY: number) => {
    const selectionCount = store.getState().selectedFieldIds.length;
    if (!selectionCount) {
      hideMenu();
      return;
    }

    updateMenuState(selectionCount);

    const rootRect = view.root.getBoundingClientRect();
    let left = clientX - rootRect.left;
    let top = clientY - rootRect.top;
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;
    menu.classList.remove("d-none");

    const menuRect = menu.getBoundingClientRect();
    const overflowX = menuRect.right - rootRect.right;
    const overflowY = menuRect.bottom - rootRect.bottom;
    if (overflowX > 0) {
      left -= overflowX;
    }
    if (overflowY > 0) {
      top -= overflowY;
    }
    left = Math.max(8, left);
    top = Math.max(8, top);
    menu.style.left = `${left}px`;
    menu.style.top = `${top}px`;

    const focusTarget = menu.querySelector<HTMLButtonElement>(
      "[data-menu-action]:not(:disabled)"
    );
    focusTarget?.focus({ preventScroll: true });
  };

  view.fieldLayer.addEventListener("contextmenu", (event) => {
    event.preventDefault();
    const target = (event.target as HTMLElement | null)?.closest<HTMLElement>(
      ".template-editor__field"
    );
    const { clientX, clientY } = event;
    if (!target) {
      hideMenu();
      if (store.getState().selectedFieldIds.length) {
        store.selectField(null);
      }
      return;
    }

    const fieldId = target.dataset.fieldId;
    if (!fieldId) {
      hideMenu();
      return;
    }

    const selection = store.getState().selectedFieldIds;
    const reveal = () => openMenu(clientX, clientY);
    if (!selection.includes(fieldId)) {
      store.selectField(fieldId);
      requestAnimationFrame(reveal);
    } else {
      reveal();
    }
  });

  view.fieldLayer.addEventListener("mousedown", () => {
    hideMenu();
  });

  menu.addEventListener("click", (event) => {
    const button = (
      event.target as HTMLElement | null
    )?.closest<HTMLButtonElement>("[data-menu-action]");
    if (!button) {
      return;
    }
    event.preventDefault();
    const action = button.dataset.menuAction;
    const selectedIds = store.getState().selectedFieldIds;
    if (!selectedIds.length) {
      hideMenu();
      return;
    }

    switch (action) {
      case "duplicate":
        store.duplicateFields(selectedIds);
        break;
      case "delete":
        store.removeFields(selectedIds);
        break;
      case "bring-front":
        store.bringFieldsToFront(selectedIds);
        break;
      case "send-back":
        store.sendFieldsToBack(selectedIds);
        break;
      default:
        break;
    }

    hideMenu();
  });

  window.addEventListener("scroll", hideMenu, true);
  window.addEventListener("resize", hideMenu);
  window.addEventListener("click", (event) => {
    if (isMenuOpen() && !menu.contains(event.target as Node)) {
      hideMenu();
    }
  });
  window.addEventListener("contextmenu", (event) => {
    if (
      isMenuOpen() &&
      !view.fieldLayer.contains(event.target as Node) &&
      !menu.contains(event.target as Node)
    ) {
      hideMenu();
    }
  });
  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      hideMenu();
    }
  });
}

function setupKeyboardShortcuts(
  view: TemplateEditorView,
  store: EditorStore
): void {
  const hideMenu = () => {
    if (!view.contextMenu.classList.contains("d-none")) {
      view.contextMenu.classList.add("d-none");
      view.contextMenu.style.left = "-9999px";
      view.contextMenu.style.top = "-9999px";
    }
  };

  window.addEventListener("keydown", (event) => {
    const active = document.activeElement as HTMLElement | null;
    const isEditable =
      active &&
      (active.tagName === "INPUT" ||
        active.tagName === "TEXTAREA" ||
        active.tagName === "SELECT" ||
        active.isContentEditable);
    if (isEditable) {
      return;
    }

    const insideEditor = active ? view.root.contains(active) : true;
    if (!insideEditor) {
      return;
    }

    const isUndoShortcut =
      (event.key === "z" || event.key === "Z") &&
      (event.metaKey || event.ctrlKey) &&
      !event.shiftKey;
    const isRedoShortcut =
      ((event.key === "z" || event.key === "Z") &&
        (event.metaKey || event.ctrlKey) &&
        event.shiftKey) ||
      ((event.key === "y" || event.key === "Y") &&
        (event.metaKey || event.ctrlKey));

    if (isUndoShortcut) {
      event.preventDefault();
      hideMenu();
      store.undo();
      return;
    }

    if (isRedoShortcut) {
      event.preventDefault();
      hideMenu();
      store.redo();
      return;
    }

    const isZoomInShortcut =
      (event.key === "+" || event.key === "=") &&
      (event.metaKey || event.ctrlKey);
    if (isZoomInShortcut) {
      event.preventDefault();
      hideMenu();
      const currentZoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
      const nextZoom = adjustZoomValue(currentZoom, 1);
      if (nextZoom !== currentZoom) {
        store.setZoom(nextZoom);
      }
      return;
    }

    const isZoomOutShortcut =
      (event.key === "-" || event.key === "_") &&
      (event.metaKey || event.ctrlKey);
    if (isZoomOutShortcut) {
      event.preventDefault();
      hideMenu();
      const currentZoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
      const nextZoom = adjustZoomValue(currentZoom, -1);
      if (nextZoom !== currentZoom) {
        store.setZoom(nextZoom);
      }
      return;
    }

    const isZoomResetShortcut =
      event.key === "0" && (event.metaKey || event.ctrlKey) && !event.shiftKey;
    if (isZoomResetShortcut) {
      event.preventDefault();
      hideMenu();
      const currentZoom = clampZoom(store.getState().zoom ?? DEFAULT_ZOOM);
      if (currentZoom !== DEFAULT_ZOOM) {
        store.setZoom(DEFAULT_ZOOM);
      }
      return;
    }

    const selectedIds = store.getState().selectedFieldIds;
    if (!selectedIds.length) {
      return;
    }

    if (event.key === "Delete" || event.key === "Backspace") {
      event.preventDefault();
      hideMenu();
      store.removeFields(selectedIds);
      return;
    }

    if (
      (event.key === "d" || event.key === "D") &&
      (event.metaKey || event.ctrlKey)
    ) {
      event.preventDefault();
      hideMenu();
      store.duplicateFields(selectedIds);
      return;
    }

    if (
      (event.key === "]" || event.key === "}") &&
      (event.metaKey || event.ctrlKey)
    ) {
      event.preventDefault();
      hideMenu();
      store.bringFieldsToFront(selectedIds);
    } else if (
      (event.key === "[" || event.key === "{") &&
      (event.metaKey || event.ctrlKey)
    ) {
      event.preventDefault();
      hideMenu();
      store.sendFieldsToBack(selectedIds);
    }
  });
}

type TemplateSaveMode = "save" | "saveAs" | "duplicate";

function setupTemplateList(
  view: TemplateEditorView,
  store: EditorStore,
  services: Services
): (templates: TemplateDocument[]) => void {
  const select = view.templateSelect;
  const searchInput = view.templateSearch;
  const deleteButton = view.templateDeleteButton;
  const refreshButton = view.templateRefreshButton;

  const templateMap = new Map<string, TemplateDocument>();
  let allTemplates: TemplateDocument[] = [];
  let filteredTemplates: TemplateDocument[] = [];
  let currentQuery = "";

  const renderOptions = () => {
    if (!select) {
      return;
    }
    const selectedId = store.getState().templateId ?? "";
    const previousValue = select.value;
    select.innerHTML = "";

    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "Neue Vorlage erstellen…";
    select.appendChild(placeholder);

    for (const template of filteredTemplates) {
      const option = document.createElement("option");
      option.value = template.id;
      option.textContent = `${template.name} · v${template.version}`;
      const latestRevision = Array.isArray(template.revisions)
        ? template.revisions[template.revisions.length - 1] || null
        : null;
      const tooltipBase = buildUpdatedTitle(template.updatedAt);
      option.title = latestRevision?.summary
        ? `Zuletzt geändert: ${tooltipBase} — ${latestRevision.summary}`
        : `Zuletzt geändert: ${tooltipBase}`;
      select.appendChild(option);
    }

    if (selectedId && templateMap.has(selectedId)) {
      select.value = selectedId;
    } else if (previousValue && templateMap.has(previousValue)) {
      select.value = previousValue;
    } else {
      select.value = "";
    }
  };

  const applyFilter = () => {
    const query = currentQuery.trim().toLowerCase();
    const selectedId = store.getState().templateId ?? null;
    const baseList = query
      ? allTemplates.filter((template) => {
          const haystack =
            `${template.name} ${template.description}`.toLowerCase();
          return haystack.includes(query);
        })
      : [...allTemplates];

    const dedupe = new Map<string, TemplateDocument>();
    if (selectedId) {
      const selectedTemplate = templateMap.get(selectedId);
      if (selectedTemplate) {
        dedupe.set(selectedTemplate.id, selectedTemplate);
      }
    }
    for (const template of baseList) {
      if (!dedupe.has(template.id)) {
        dedupe.set(template.id, template);
      }
    }
    filteredTemplates = sortTemplatesByUpdated(Array.from(dedupe.values()));
    renderOptions();
  };

  const sync = (templates: TemplateDocument[]): void => {
    allTemplates = sortTemplatesByUpdated(cloneTemplateCollection(templates));
    templateMap.clear();
    for (const template of allTemplates) {
      templateMap.set(template.id, template);
    }
    applyFilter();
  };

  if (select) {
    select.addEventListener("change", () => {
      const selectedId = select.value;
      if (!selectedId) {
        store.reset();
        return;
      }
      if (store.getState().templateId === selectedId) {
        return;
      }
      const template = templateMap.get(selectedId);
      if (template) {
        store.loadTemplate(template);
      }
    });
  }

  if (searchInput) {
    searchInput.addEventListener("input", () => {
      currentQuery = searchInput.value ?? "";
      applyFilter();
    });
  }

  if (refreshButton) {
    refreshButton.addEventListener("click", (event) => {
      event.preventDefault();
      applyFilter();
    });
  }

  if (deleteButton) {
    deleteButton.addEventListener("click", () => {
      void handleTemplateDeletion(
        select,
        templateMap,
        store,
        view,
        services
      ).catch((error: unknown) => {
        console.error("Vorlage konnte nicht gelöscht werden", error);
      });
    });
  }

  return sync;
}

async function handleTemplateDeletion(
  select: HTMLSelectElement | null,
  templateMap: Map<string, TemplateDocument>,
  store: EditorStore,
  view: TemplateEditorView,
  services: Services
): Promise<void> {
  if (!select) {
    showTemplateNotification(view, "Bitte wähle zuerst eine Vorlage aus.", {
      kind: "warning",
      duration: 3500,
    });
    return;
  }
  const selectedId = select.value;
  if (!selectedId) {
    showTemplateNotification(view, "Bitte wähle zuerst eine Vorlage aus.", {
      kind: "warning",
      duration: 3500,
    });
    return;
  }
  const template = templateMap.get(selectedId) ?? null;
  const templateName = template?.name ?? "Vorlage";
  const confirmed = window.confirm(
    `Soll die Vorlage "${templateName}" wirklich gelöscht werden?`
  );
  if (!confirmed) {
    showTemplateNotification(view, "Löschen abgebrochen.", {
      kind: "info",
      duration: 2500,
    });
    return;
  }

  const state = services.state.getState();
  const previousTemplates = cloneTemplateCollection(state.templates ?? []);
  const nextTemplates = sortTemplatesByUpdated(
    previousTemplates.filter((item) => item.id !== selectedId)
  );

  if (nextTemplates.length === previousTemplates.length) {
    showTemplateNotification(view, "Vorlage konnte nicht gefunden werden.", {
      kind: "error",
    });
    return;
  }

  updateSlice("templates", () => cloneTemplateCollection(nextTemplates));

  try {
    await persistTemplates(services, view);
    if (store.getState().templateId === selectedId) {
      store.reset();
    }
    services.events.emit("templates:changed", {
      action: "delete",
      templateId: selectedId,
      name: templateName,
    });
    showTemplateNotification(
      view,
      `Vorlage "${templateName}" wurde gelöscht.`,
      {
        kind: "success",
        duration: 3200,
        id: "template-delete",
      }
    );
  } catch (error) {
    updateSlice("templates", () => previousTemplates);
    showTemplateNotification(
      view,
      "Vorlage konnte nicht gelöscht werden. Bitte später erneut versuchen.",
      {
        kind: "error",
      }
    );
    throw error;
  }
}

async function handleTemplateSave(
  mode: TemplateSaveMode,
  store: EditorStore,
  services: Services,
  view: TemplateEditorView,
  options: TemplateSaveOptions = {}
): Promise<void> {
  cancelAutoSaveTimer();
  const { silent = false, summary: summaryOverride } = options;
  const state = store.getState();
  if (mode === "save" && state.templateId && state.dirty) {
    setAutoSaveStatus("saving", view, state);
  }

  const appState = services.state.getState();
  const templates = appState.templates ?? [];
  const existing = state.templateId
    ? (templates.find((template) => template.id === state.templateId) ?? null)
    : null;

  let desiredName =
    state.name?.trim() || existing?.name?.trim() || "Neue Vorlage";

  if (mode === "duplicate" && desiredName) {
    desiredName = `${desiredName} Kopie`;
  }

  if (!existing || mode === "saveAs" || !desiredName) {
    const promptLabel =
      mode === "saveAs"
        ? "Name für die kopierte Vorlage:"
        : mode === "duplicate"
          ? "Name für die duplizierte Vorlage:"
          : "Name für die neue Vorlage:";
    const input = window.prompt(promptLabel, desiredName || "Neue Vorlage");
    if (input === null) {
      if (!silent) {
        showTemplateNotification(view, "Speichern abgebrochen.", {
          kind: "info",
          duration: 2500,
          id: "template-save",
        });
      }
      return;
    }
    desiredName = input.trim() || "Neue Vorlage";
  }

  const comparisonList =
    existing && mode === "save"
      ? templates.filter((template) => template.id !== existing.id)
      : templates;
  const uniqueName = ensureUniqueTemplateName(desiredName, comparisonList);

  if (uniqueName !== state.name) {
    store.setTemplateMeta({ name: uniqueName });
  }

  const updatedState = store.getState();
  const previousTemplates = cloneTemplateCollection(templates);

  let revisionSummary: string | undefined;
  if (existing && mode === "save") {
    if (silent) {
      revisionSummary = summaryOverride ?? "Automatisch gespeichert";
    } else {
      const note = window.prompt("Änderungen zusammenfassen (optional):", "");
      if (note !== null) {
        const trimmed = note.trim();
        revisionSummary = trimmed ? trimmed : undefined;
      }
    }
  } else if (!existing) {
    revisionSummary = "Vorlage erstellt";
  } else if (mode === "duplicate") {
    revisionSummary = existing?.name
      ? `Duplikat von "${existing.name}"`
      : "Vorlage dupliziert";
  } else if (mode === "saveAs") {
    revisionSummary = existing?.name
      ? `Kopie von "${existing.name}"`
      : "Vorlage kopiert";
  }

  let documentOptions: {
    existing?: TemplateDocument | null;
    id?: string;
    version?: number;
    summary?: string;
  } = {};

  if (mode === "save" && existing) {
    documentOptions = { existing };
  } else if (existing) {
    documentOptions = { id: generateTemplateId(), version: 1 };
  } else {
    documentOptions = { version: 1 };
  }
  if (revisionSummary) {
    documentOptions.summary = revisionSummary;
  }

  const templateDocument = createTemplateDocumentFromState(
    updatedState,
    documentOptions
  );
  templateDocument.name = uniqueName;

  let nextTemplates: TemplateDocument[];
  if (mode === "save" && existing) {
    nextTemplates = templates.map((template) =>
      template.id === templateDocument.id
        ? cloneTemplateDocument(templateDocument)
        : cloneTemplateDocument(template)
    );
  } else {
    const filtered = templates.filter(
      (template) => template.id !== templateDocument.id
    );
    nextTemplates = [
      ...filtered.map(cloneTemplateDocument),
      cloneTemplateDocument(templateDocument),
    ];
  }

  nextTemplates = sortTemplatesByUpdated(nextTemplates);

  updateSlice("templates", () => cloneTemplateCollection(nextTemplates));

  try {
    await persistTemplates(services, view);
  } catch (error) {
    updateSlice("templates", () => previousTemplates);
    setAutoSaveStatus("error", view, store.getState());
    showTemplateNotification(
      view,
      "Vorlage konnte nicht gespeichert werden. Änderungen wurden verworfen.",
      {
        kind: "error",
      }
    );
    throw error;
  }

  const latestRevision = Array.isArray(templateDocument.revisions)
    ? templateDocument.revisions[templateDocument.revisions.length - 1] || null
    : null;
  const summaryForState = latestRevision?.summary
    ? latestRevision.summary.trim() || null
    : revisionSummary?.trim() || null;

  store.markSaved({
    id: templateDocument.id,
    version: templateDocument.version,
    updatedAt: templateDocument.updatedAt,
    summary: summaryForState,
    revisions: templateDocument.revisions,
  });
  setAutoSaveStatus("idle", view, store.getState());
  services.events.emit("templates:changed", {
    action:
      existing && mode === "save"
        ? "update"
        : (existing && mode !== "save") || !existing
          ? "create"
          : "update",
    template: templateDocument,
    summary: summaryForState ?? undefined,
    updatedAt: templateDocument.updatedAt,
  });

  if (!silent) {
    let successMessage = `${templateDocument.name} gespeichert.`;
    if (mode === "save" && existing) {
      successMessage = `${templateDocument.name} · v${templateDocument.version} gespeichert.`;
    } else if (mode === "duplicate") {
      successMessage = `Duplikat "${templateDocument.name}" erstellt.`;
    } else if (mode === "saveAs") {
      successMessage = `Kopie "${templateDocument.name}" gespeichert.`;
    }

    showTemplateNotification(view, successMessage, {
      kind: "success",
      duration: 3200,
      id: "template-save",
    });
  }
}

async function handleRevisionRestore(
  store: EditorStore,
  targetVersion: number,
  view: TemplateEditorView
): Promise<void> {
  const state = store.getState();

  if (!state.templateId) {
    showTemplateNotification(
      view,
      "Bitte speichere die Vorlage, bevor Revisionen wiederhergestellt werden.",
      {
        kind: "warning",
      }
    );
    return;
  }

  if (!Array.isArray(state.revisions) || !state.revisions.length) {
    showTemplateNotification(view, "Keine Revisionen verfügbar.", {
      kind: "warning",
    });
    return;
  }

  const revision = state.revisions.find(
    (entry) => entry && entry.version === targetVersion
  );

  if (!revision) {
    showTemplateNotification(view, "Revision konnte nicht gefunden werden.", {
      kind: "error",
    });
    return;
  }

  if (!revision.snapshot) {
    showTemplateNotification(
      view,
      "Für diese Revision liegen keine gespeicherten Daten vor. Wiederherstellung nicht möglich.",
      {
        kind: "error",
      }
    );
    return;
  }

  const confirmMessage = state.dirty
    ? `Ungespeicherte Änderungen gehen verloren. Revision v${targetVersion} trotzdem wiederherstellen?`
    : `Revision v${targetVersion} wiederherstellen?`;
  const confirmed = window.confirm(confirmMessage);
  if (!confirmed) {
    return;
  }

  store.restoreRevision(revision);
  showTemplateNotification(
    view,
    `Revision v${targetVersion} wiederhergestellt.`,
    {
      kind: "success",
      duration: 3200,
    }
  );
}

async function persistTemplates(
  services: Services,
  view?: TemplateEditorView
): Promise<void> {
  const driverKey = getActiveDriverKey();
  if (!driverKey || driverKey === "memory") {
    return;
  }
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
    services.events.emit("database:saved", {
      scope: "templates",
      driver: driverKey,
    });
  } catch (error) {
    services.events.emit("database:error", {
      scope: "templates",
      error,
    });
    console.error("Vorlagen konnten nicht gespeichert werden", error);
    const notify = view
      ? (message: string) =>
          showTemplateNotification(view, message, { kind: "error" })
      : (message: string) =>
          emitTemplateNotification(message, { kind: "error" });
    notify(
      "Die Vorlage wurde lokal aktualisiert, konnte aber nicht gesichert werden. Bitte später erneut versuchen."
    );
    throw error;
  }
}

function cloneTemplateCollection(
  items: TemplateDocument[]
): TemplateDocument[] {
  return items.map(cloneTemplateDocument);
}

function sortTemplatesByUpdated(items: TemplateDocument[]): TemplateDocument[] {
  return [...items].sort((a, b) => {
    const aTime = Date.parse(a.updatedAt ?? "") || 0;
    const bTime = Date.parse(b.updatedAt ?? "") || 0;
    return bTime - aTime;
  });
}

function buildUpdatedTitle(updatedAt: string): string {
  try {
    const date = new Date(updatedAt);
    if (Number.isNaN(date.getTime())) {
      return updatedAt;
    }
    return new Intl.DateTimeFormat("de-DE", {
      dateStyle: "short",
      timeStyle: "short",
    }).format(date);
  } catch (error) {
    return updatedAt;
  }
}

function updateSelectionOutline(
  view: TemplateEditorView,
  state: EditorState
): void {
  const layer = view.selectionLayer;
  if (!layer) {
    return;
  }

  layer.innerHTML = "";
  if (state.selectedFieldIds.length < 2) {
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const fieldId of state.selectedFieldIds) {
    const safeId =
      typeof CSS !== "undefined" && CSS.escape ? CSS.escape(fieldId) : fieldId;
    const fieldElement = view.fieldLayer.querySelector<HTMLElement>(
      `[data-field-id="${safeId}"]`
    );
    if (!fieldElement) {
      continue;
    }
    const left = parseFloat(fieldElement.dataset.left ?? "0");
    const top = parseFloat(fieldElement.dataset.top ?? "0");
    const width = parseFloat(
      fieldElement.dataset.width ?? `${fieldElement.offsetWidth}`
    );
    const height = parseFloat(
      fieldElement.dataset.height ?? `${fieldElement.offsetHeight}`
    );
    const outline = document.createElement("div");
    outline.className = "template-editor__selection-outline";
    outline.style.left = `${left}px`;
    outline.style.top = `${top}px`;
    outline.style.width = `${width}px`;
    outline.style.height = `${height}px`;
    fragment.appendChild(outline);
  }

  layer.appendChild(fragment);
}

function setupPropertyPanel(
  view: TemplateEditorView,
  store: EditorStore
): void {
  if (!view.propertyGroups) {
    return;
  }

  const { name, label, placeholder, required, width, height, x, y } =
    view.propertyInputs;

  name?.addEventListener("input", () => {
    withSelectedField(view, store, (field) => {
      const nextValue = name.value.trim();
      if (!nextValue) {
        setInputValue(name, field.name);
        return;
      }
      if (nextValue !== field.name) {
        store.updateField(field.id, { name: nextValue });
      }
    });
  });

  label?.addEventListener("input", () => {
    withSelectedField(view, store, (field) => {
      const nextValue = label.value;
      if (nextValue !== field.label) {
        store.updateField(field.id, { label: nextValue });
      }
    });
  });

  placeholder?.addEventListener("input", () => {
    if (placeholder.disabled) {
      return;
    }
    withSelectedField(view, store, (field) => {
      const nextValue = placeholder.value;
      if (nextValue !== field.placeholder) {
        store.updateField(field.id, { placeholder: nextValue });
      }
    });
  });

  required?.addEventListener("change", () => {
    withSelectedField(view, store, (field) => {
      const nextRequired = Boolean(required.checked);
      if (nextRequired !== field.required) {
        store.updateField(field.id, { required: nextRequired });
      }
    });
  });

  const layoutInputs: Array<{
    input: HTMLInputElement | null;
    key: "w" | "h" | "x" | "y";
  }> = [
    { input: width, key: "w" },
    { input: height, key: "h" },
    { input: x, key: "x" },
    { input: y, key: "y" },
  ];

  layoutInputs.forEach(({ input, key }) => {
    input?.addEventListener("change", () => {
      withSelectedField(view, store, (field) => {
        const raw = Number.parseInt(input.value, 10);
        if (Number.isNaN(raw)) {
          setInputValue(input, String(field.layout[key]));
          return;
        }
        const nextLayout = clampLayout({
          ...field.layout,
          [key]: raw,
        } as FieldLayout);
        if (
          nextLayout.x !== field.layout.x ||
          nextLayout.y !== field.layout.y ||
          nextLayout.w !== field.layout.w ||
          nextLayout.h !== field.layout.h
        ) {
          store.updateFieldLayout(field.id, nextLayout);
        } else {
          setInputValue(input, String(field.layout[key]));
        }
      });
    });
  });

  const {
    label: multiLabel,
    placeholder: multiPlaceholder,
    width: multiWidth,
    height: multiHeight,
    x: multiX,
    y: multiY,
  } = view.propertyMultiInputs;

  multiLabel?.addEventListener("change", () => {
    const state = store.getState();
    if (state.selectedFieldIds.length < 2) {
      return;
    }
    const nextValue = multiLabel.value;
    store.updateFields(state.selectedFieldIds, { label: nextValue });
  });

  multiPlaceholder?.addEventListener("change", () => {
    const state = store.getState();
    if (state.selectedFieldIds.length < 2) {
      return;
    }
    const placeholderIds = state.selectedFieldIds.filter((id) => {
      const target = state.fields.find((field) => field.id === id);
      return target?.type === "text";
    });
    if (!placeholderIds.length) {
      return;
    }
    store.updateFields(placeholderIds, {
      placeholder: multiPlaceholder.value,
    });
  });

  view.propertyMultiRequiredButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const state = store.getState();
      if (state.selectedFieldIds.length < 2) {
        return;
      }
      const desired = button.dataset.required === "true";
      store.updateFields(state.selectedFieldIds, { required: desired });
    });
  });

  const multiLayoutInputs: Array<{
    input: HTMLInputElement | null;
    key: "w" | "h" | "x" | "y";
  }> = [
    { input: multiWidth, key: "w" },
    { input: multiHeight, key: "h" },
    { input: multiX, key: "x" },
    { input: multiY, key: "y" },
  ];

  multiLayoutInputs.forEach(({ input, key }) => {
    input?.addEventListener("change", () => {
      const state = store.getState();
      if (state.selectedFieldIds.length < 2) {
        return;
      }
      const raw = Number.parseInt(input.value, 10);
      if (Number.isNaN(raw)) {
        input.value = "";
        return;
      }
      const layoutPatch: Partial<FieldLayout> = {
        [key]: raw,
      } as Partial<FieldLayout>;
      store.updateFields(state.selectedFieldIds, {
        layout: layoutPatch,
      });
    });
  });
}

function setupRevisionPanel(
  view: TemplateEditorView,
  store: EditorStore
): void {
  const list = view.revisionList;
  if (list) {
    list.addEventListener("click", (event) => {
      const button = (
        event.target as HTMLElement | null
      )?.closest<HTMLButtonElement>("[data-revision-version]");
      if (!button) {
        return;
      }
      const rawVersion = button.dataset.revisionVersion;
      if (!rawVersion) {
        return;
      }
      const parsedVersion = Number.parseInt(rawVersion, 10);
      if (!Number.isFinite(parsedVersion)) {
        return;
      }
      store.selectRevision(parsedVersion);
      void ensureRevisionSnapshot(view, store, parsedVersion);
    });
  }

  if (view.revisionRevertButton) {
    view.revisionRevertButton.addEventListener("click", () => {
      void (async () => {
        const targetVersion = view.revisionRevertButton?.dataset.revisionTarget;
        if (!targetVersion) {
          return;
        }
        const parsedVersion = Number.parseInt(targetVersion, 10);
        if (!Number.isFinite(parsedVersion)) {
          return;
        }
        await ensureRevisionSnapshot(view, store, parsedVersion);
        await handleRevisionRestore(store, parsedVersion, view);
      })().catch((error: unknown) => {
        console.error("Revision konnte nicht wiederhergestellt werden", error);
        showTemplateNotification(
          view,
          "Die Revision konnte nicht geladen werden. Bitte später erneut versuchen.",
          {
            kind: "error",
          }
        );
      });
    });
  }
}

function updatePropertyPanel(
  view: TemplateEditorView,
  state: EditorState
): void {
  const propertyGroups = view.propertyGroups;
  if (!propertyGroups) {
    return;
  }

  const emptyState = view.emptyPropertyState;
  const multiPanel = view.propertyMultiPanel;
  const selectionCount = state.selectedFieldIds.length;

  if (selectionCount === 0) {
    propertyGroups.dataset.fieldId = "";
    propertyGroups.classList.add("d-none");
    if (multiPanel) {
      multiPanel.classList.add("d-none");
    }
    if (emptyState) {
      emptyState.classList.remove("d-none");
      const defaultMessage = emptyState.dataset.defaultMessage;
      if (defaultMessage) {
        emptyState.innerHTML = defaultMessage;
      }
    }
    setPropertyInputsDisabled(view, true);
    return;
  }

  if (selectionCount > 1) {
    propertyGroups.dataset.fieldId = "";
    propertyGroups.classList.add("d-none");
    setPropertyInputsDisabled(view, true);
    if (emptyState) {
      emptyState.classList.add("d-none");
    }
    if (multiPanel) {
      multiPanel.classList.remove("d-none");
    }

    Object.values(view.propertyMultiInputs).forEach((input) => {
      if (input) {
        input.disabled = false;
      }
    });

    const selectedFields = getSelectedFields(state);
    if (!selectedFields.length) {
      if (multiPanel) {
        multiPanel.classList.add("d-none");
      }
      if (emptyState) {
        emptyState.classList.remove("d-none");
        const defaultMessage = emptyState.dataset.defaultMessage;
        if (defaultMessage) {
          emptyState.innerHTML = defaultMessage;
        }
      }
      return;
    }

    const allRequired = selectedFields.every((field) => field.required);
    const noneRequired = selectedFields.every((field) => !field.required);

    if (view.propertyMultiSummary) {
      const typeCounts = new Map<FieldType, number>();
      selectedFields.forEach((field) => {
        typeCounts.set(field.type, (typeCounts.get(field.type) ?? 0) + 1);
      });
      const typeSummary = Array.from(typeCounts.entries())
        .map(
          ([type, count]) =>
            `${FIELD_TYPE_LABELS[type] ?? type.toUpperCase()} × ${count}`
        )
        .join(" · ");
      const requiredSummary = allRequired
        ? "Pflichtstatus: alle Pflicht"
        : noneRequired
          ? "Pflichtstatus: alle optional"
          : "Pflichtstatus: gemischt";
      view.propertyMultiSummary.innerHTML = `
        <p class="mb-1"><strong>${selectionCount}</strong> Felder ausgewählt</p>
        <p class="mb-0">${typeSummary || "Typen gemischt"}</p>
        <p class="mb-0 text-muted">${requiredSummary}</p>
      `;
    }

    view.propertyMultiRequiredButtons.forEach((button) => {
      const desired = button.dataset.required === "true";
      const active = desired ? allRequired : noneRequired;
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });

    view.propertyMultiAdvancedSections.forEach((section) => {
      const raw = section.dataset.propertyMultiAdvanced;
      if (!raw) {
        section.classList.remove("d-none");
        section
          .querySelectorAll<
            HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement
          >("input, textarea, select")
          .forEach((control) => {
            control.disabled = false;
          });
        return;
      }
      const allowedTypes = raw
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean) as FieldType[];
      const visible = allowedTypes.length
        ? selectedFields.every((field) => allowedTypes.includes(field.type))
        : true;
      section.classList.toggle("d-none", !visible);
      section
        .querySelectorAll<
          HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement
        >("input, textarea, select")
        .forEach((control) => {
          control.disabled = !visible;
        });
    });

    const labelShared = computeSharedValue(selectedFields, (field) => {
      return field.label ?? "";
    });
    setMultiInputValue(
      view.propertyMultiInputs.label,
      labelShared as SharedValueResult<string | number>
    );

    const placeholderFields = selectedFields.filter(
      (field) => field.type === "text"
    );
    if (
      view.propertyMultiInputs.placeholder &&
      !view.propertyMultiInputs.placeholder.disabled
    ) {
      const placeholderShared = computeSharedValue(
        placeholderFields,
        (field) => field.placeholder ?? ""
      );
      setMultiInputValue(
        view.propertyMultiInputs.placeholder,
        placeholderShared as SharedValueResult<string | number>
      );
    }

    const widthShared = computeSharedValue(
      selectedFields,
      (field) => field.layout.w
    );
    setMultiInputValue(
      view.propertyMultiInputs.width,
      widthShared as SharedValueResult<string | number>
    );

    const heightShared = computeSharedValue(
      selectedFields,
      (field) => field.layout.h
    );
    setMultiInputValue(
      view.propertyMultiInputs.height,
      heightShared as SharedValueResult<string | number>
    );

    const xShared = computeSharedValue(
      selectedFields,
      (field) => field.layout.x
    );
    setMultiInputValue(
      view.propertyMultiInputs.x,
      xShared as SharedValueResult<string | number>
    );

    const yShared = computeSharedValue(
      selectedFields,
      (field) => field.layout.y
    );
    setMultiInputValue(
      view.propertyMultiInputs.y,
      yShared as SharedValueResult<string | number>
    );

    return;
  }

  if (multiPanel) {
    multiPanel.classList.add("d-none");
  }
  const selectedId = state.selectedFieldIds[0];
  const field = state.fields.find((item) => item.id === selectedId);
  if (!field) {
    propertyGroups.dataset.fieldId = "";
    propertyGroups.classList.add("d-none");
    if (emptyState) {
      emptyState.classList.remove("d-none");
      const defaultMessage = emptyState.dataset.defaultMessage;
      if (defaultMessage) {
        emptyState.innerHTML = defaultMessage;
      }
    }
    setPropertyInputsDisabled(view, true);
    return;
  }

  propertyGroups.dataset.fieldId = field.id;
  propertyGroups.classList.remove("d-none");
  if (emptyState) {
    emptyState.classList.add("d-none");
    const defaultMessage = emptyState.dataset.defaultMessage;
    if (defaultMessage) {
      emptyState.innerHTML = defaultMessage;
    }
  }
  setPropertyInputsDisabled(view, false);

  const { name, label, placeholder, required, width, height, x, y } =
    view.propertyInputs;
  setInputValue(name, field.name);
  setInputValue(label, field.label);
  setInputValue(placeholder, field.placeholder ?? "");
  if (required) {
    required.checked = Boolean(field.required);
  }
  setInputValue(width, String(field.layout.w));
  setInputValue(height, String(field.layout.h));
  setInputValue(x, String(field.layout.x));
  setInputValue(y, String(field.layout.y));

  view.propertyAdvancedSections.forEach((section) => {
    const types = section.dataset.propertyAdvanced
      ?.split(",")
      .map((value) => value.trim())
      .filter(Boolean);
    if (!types || !types.length) {
      return;
    }
    const visible = types.includes(field.type);
    section.classList.toggle("d-none", !visible);
    const control = section.querySelector<
      HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement
    >("input, textarea, select");
    if (control) {
      control.disabled = !visible;
    }
  });
}

function setPropertyInputsDisabled(
  view: TemplateEditorView,
  disabled: boolean
): void {
  if (view.propertyGroups) {
    view.propertyGroups.setAttribute(
      "aria-disabled",
      disabled ? "true" : "false"
    );
  }
  Object.values(view.propertyInputs).forEach((input) => {
    if (input) {
      input.disabled = disabled;
    }
  });
}

function withSelectedField(
  view: TemplateEditorView,
  store: EditorStore,
  handler: (field: EditorField) => void
): void {
  const fieldId = view.propertyGroups?.dataset.fieldId;
  if (!fieldId) {
    return;
  }
  const field = store.getState().fields.find((item) => item.id === fieldId);
  if (!field) {
    return;
  }
  handler(field);
}

function getSelectedFields(state: EditorState): EditorField[] {
  const idSet = new Set(state.selectedFieldIds);
  if (!idSet.size) {
    return [];
  }
  return state.fields.filter((field) => idSet.has(field.id));
}

function setInputValue(input: HTMLInputElement | null, value: string): void {
  if (!input) {
    return;
  }
  if (input.value === value) {
    return;
  }
  const wasFocused = document.activeElement === input;
  const selectionStart = input.selectionStart;
  const selectionEnd = input.selectionEnd;
  input.value = value;
  if (wasFocused && selectionStart !== null && selectionEnd !== null) {
    const safeStart = Math.min(selectionStart, value.length);
    const safeEnd = Math.min(selectionEnd, value.length);
    input.setSelectionRange(safeStart, safeEnd);
  }
}

interface SharedValueResult<T> {
  mixed: boolean;
  value: T | null;
}

function computeSharedValue<T>(
  fields: EditorField[],
  selector: (field: EditorField) => T
): SharedValueResult<T> {
  if (!fields.length) {
    return { mixed: false, value: null };
  }
  const first = selector(fields[0]);
  for (let index = 1; index < fields.length; index += 1) {
    const next = selector(fields[index]);
    if (!Object.is(next, first)) {
      return { mixed: true, value: null };
    }
  }
  return { mixed: false, value: first };
}

function setMultiInputValue(
  input: HTMLInputElement | null,
  result: SharedValueResult<string | number>
): void {
  if (!input) {
    return;
  }
  if (!input.dataset.defaultPlaceholder) {
    const defaultPlaceholder =
      input.getAttribute("data-default-placeholder") ??
      input.getAttribute("placeholder") ??
      "";
    input.dataset.defaultPlaceholder = defaultPlaceholder;
  }
  const defaultPlaceholder = input.dataset.defaultPlaceholder ?? "";
  const mixedPlaceholder =
    input.dataset.mixedPlaceholder ??
    input.getAttribute("data-mixed-placeholder") ??
    "Mehrere Werte";

  if (result.mixed) {
    input.value = "";
    input.placeholder = mixedPlaceholder;
    input.classList.add("is-mixed");
    input.dataset.mixed = "true";
    input.title = mixedPlaceholder;
    return;
  }

  const stringValue =
    result.value === null || result.value === undefined
      ? ""
      : String(result.value);
  input.value = stringValue;
  input.placeholder = defaultPlaceholder;
  input.classList.remove("is-mixed");
  input.dataset.mixed = "false";
  input.title = stringValue || defaultPlaceholder;
}

function updateToolbarToggles(
  view: TemplateEditorView,
  state: EditorState
): void {
  const undoButton = view.toolbar.querySelector<HTMLButtonElement>(
    "[data-action='template-undo']"
  );
  if (undoButton) {
    const canUndo = state.undoStack.length > 0;
    undoButton.disabled = !canUndo;
    undoButton.setAttribute("aria-disabled", canUndo ? "false" : "true");
    undoButton.title = canUndo
      ? "Letzte Änderung rückgängig machen"
      : "Keine Schritte zum Rückgängig machen";
  }

  const redoButton = view.toolbar.querySelector<HTMLButtonElement>(
    "[data-action='template-redo']"
  );
  if (redoButton) {
    const canRedo = state.redoStack.length > 0;
    redoButton.disabled = !canRedo;
    redoButton.setAttribute("aria-disabled", canRedo ? "false" : "true");
    redoButton.title = canRedo
      ? "Zuletzt rückgängig gemachte Änderung wiederholen"
      : "Keine Schritte zum Wiederholen";
  }

  const gridButton = view.toolbar.querySelector<HTMLButtonElement>(
    "[data-action='toggle-grid']"
  );
  if (gridButton) {
    gridButton.classList.toggle("active", state.gridVisible);
    gridButton.setAttribute(
      "aria-pressed",
      state.gridVisible ? "true" : "false"
    );
  }

  const snapButton = view.toolbar.querySelector<HTMLButtonElement>(
    "[data-action='toggle-snap']"
  );
  if (snapButton) {
    snapButton.classList.toggle("active", state.snapping);
    snapButton.setAttribute("aria-pressed", state.snapping ? "true" : "false");
  }
}

function updateZoomControls(
  view: TemplateEditorView,
  state: EditorState
): void {
  const zoom = clampZoom(state.zoom ?? DEFAULT_ZOOM);

  if (view.paperWrapper) {
    view.paperWrapper.style.transform = `scale(${zoom})`;
    view.paperWrapper.dataset.zoom = `${zoom}`;
  }

  if (view.zoomIndicator) {
    const zoomPercentage = Math.round(zoom * 100);
    view.zoomIndicator.textContent = `${zoomPercentage}%`;
    view.zoomIndicator.setAttribute(
      "aria-label",
      `Aktuelle Zoomstufe ${zoomPercentage}%`
    );
  }

  if (view.zoomInButton) {
    const atMax = zoom >= MAX_ZOOM - ZOOM_EPSILON;
    view.zoomInButton.disabled = atMax;
    view.zoomInButton.setAttribute("aria-disabled", atMax ? "true" : "false");
  }

  if (view.zoomOutButton) {
    const atMin = zoom <= MIN_ZOOM + ZOOM_EPSILON;
    view.zoomOutButton.disabled = atMin;
    view.zoomOutButton.setAttribute("aria-disabled", atMin ? "true" : "false");
  }
}

function updateRevisionPanel(
  view: TemplateEditorView,
  state: EditorState
): void {
  const list = view.revisionList;
  if (!view.revisionPanel || !list) {
    return;
  }

  const revisions = Array.isArray(state.revisions) ? [...state.revisions] : [];
  const emptyState = view.revisionEmpty;
  const revertButton = view.revisionRevertButton;

  if (!revisions.length) {
    list.innerHTML = "";
    list.classList.add("d-none");
    if (emptyState) {
      emptyState.classList.remove("d-none");
    }
    if (revertButton) {
      revertButton.disabled = true;
      revertButton.setAttribute("aria-disabled", "true");
    }
    return;
  }

  if (emptyState) {
    emptyState.classList.add("d-none");
  }
  list.classList.remove("d-none");

  const fragment = document.createDocumentFragment();
  const sorted = revisions.sort((a, b) => (b.version ?? 0) - (a.version ?? 0));
  const activeVersion = state.selectedRevisionVersion;
  const currentVersion = state.version;
  const activeRevision =
    typeof activeVersion === "number"
      ? (state.revisions.find(
          (revision) => revision && revision.version === activeVersion
        ) ?? null)
      : null;
  const activeRevisionHasSnapshot = Boolean(activeRevision?.snapshot);

  for (const revision of sorted) {
    if (!revision) {
      continue;
    }
    const version = revision.version ?? 0;
    if (!Number.isFinite(version)) {
      continue;
    }

    const item = document.createElement("button");
    item.type = "button";
    item.className = "template-editor__revision-item";
    item.dataset.revisionVersion = String(version);

    if (version === activeVersion) {
      item.classList.add("is-active");
      item.setAttribute("aria-current", "true");
    } else {
      item.removeAttribute("aria-current");
    }
    if (version === currentVersion) {
      item.classList.add("is-current");
    }

    if (revision.snapshot) {
      item.dataset.hasSnapshot = "true";
    } else {
      item.dataset.hasSnapshot = "false";
      item.classList.add("is-disabled");
      item.title = `${item.title} — Keine Wiederherstellung möglich`;
    }

    item.setAttribute(
      "aria-pressed",
      version === activeVersion ? "true" : "false"
    );

    const updatedLabel =
      buildUpdatedTitle(revision.updatedAt ?? "") || revision.updatedAt || "";
    const summaryText = revision.summary?.trim() || "Keine Notiz";
    item.title = `Revision v${version} · ${updatedLabel}${summaryText ? ` — ${summaryText}` : ""}`;

    const title = document.createElement("span");
    title.className = "template-editor__revision-item-version";
    title.textContent = `v${version}`;
    item.appendChild(title);

    const summary = document.createElement("span");
    summary.className = "template-editor__revision-item-summary";
    summary.textContent =
      revision.summary && revision.summary.trim()
        ? revision.summary
        : "Keine Notiz";
    item.appendChild(summary);

    const meta = document.createElement("span");
    meta.className = "template-editor__revision-item-meta";
    meta.textContent = buildUpdatedTitle(revision.updatedAt ?? "");
    item.appendChild(meta);

    fragment.appendChild(item);
  }

  list.innerHTML = "";
  list.appendChild(fragment);

  if (revertButton) {
    const canRevert =
      typeof activeVersion === "number" &&
      activeVersion !== currentVersion &&
      activeRevisionHasSnapshot;
    revertButton.disabled = !canRevert;
    revertButton.setAttribute("aria-disabled", canRevert ? "false" : "true");
    revertButton.dataset.revisionTarget = canRevert
      ? String(activeVersion)
      : "";
    revertButton.title = canRevert
      ? `Revision v${activeVersion} wiederherstellen`
      : activeRevisionHasSnapshot
        ? "Keine alternative Revision ausgewählt"
        : "Diese Revision enthält keine gespeicherten Daten für eine Wiederherstellung.";
  }
}

async function ensureRevisionSnapshot(
  view: TemplateEditorView,
  store: EditorStore,
  version: number
): Promise<void> {
  if (!Number.isFinite(version)) {
    return;
  }

  const state = store.getState();
  const templateId = state.templateId;
  if (!templateId) {
    return;
  }

  const revision = state.revisions.find(
    (entry) => entry && entry.version === version
  );
  if (!revision) {
    return;
  }
  if (revision.snapshot !== undefined) {
    return;
  }

  const loadKey = `${templateId}:${version}`;
  if (pendingRevisionSnapshotLoads.has(loadKey)) {
    await pendingRevisionSnapshotLoads.get(loadKey);
    return;
  }

  const loadPromise = (async () => {
    const listItem = view.revisionList?.querySelector<HTMLButtonElement>(
      `[data-revision-version='${version}']`
    );
    const itemWasDisabled = listItem?.disabled ?? false;
    const revertButton = view.revisionRevertButton;
    const revertMatches = Boolean(
      revertButton &&
        Number.parseInt(revertButton.dataset.revisionTarget ?? "", 10) ===
          version
    );
    const revertWasDisabled = revertButton?.disabled ?? false;

    if (listItem) {
      listItem.classList.add("is-loading");
      listItem.setAttribute("aria-busy", "true");
      listItem.dataset.loading = "true";
      listItem.disabled = true;
    }
    if (revertMatches && revertButton) {
      revertButton.dataset.loading = "true";
      revertButton.setAttribute("aria-busy", "true");
      revertButton.disabled = true;
      revertButton.setAttribute("aria-disabled", "true");
    }

    try {
      const document = await loadTemplateRevisionDocument(templateId, version);
      if (!document) {
        store.setRevisionSnapshot(version, null);
        return;
      }

      const latestState = store.getState();
      if (latestState.templateId !== templateId) {
        return;
      }

      const latestRevision = latestState.revisions.find(
        (entry) => entry && entry.version === version
      );
      if (!latestRevision || latestRevision.snapshot !== undefined) {
        return;
      }

      const fallback = createRevisionSnapshotFromState(latestState, {
        name: latestState.name,
        description: latestState.description,
      });
      const snapshot = normalizeRevisionSnapshotFromDocument(
        document,
        fallback
      );
      if (snapshot) {
        store.setRevisionSnapshot(version, snapshot);
      } else {
        store.setRevisionSnapshot(version, null);
      }
    } finally {
      if (listItem && listItem.isConnected) {
        listItem.classList.remove("is-loading");
        listItem.removeAttribute("aria-busy");
        listItem.removeAttribute("data-loading");
        if (!itemWasDisabled) {
          listItem.disabled = false;
        }
      }
      const latestButton = view.revisionRevertButton;
      if (latestButton && revertMatches) {
        latestButton.removeAttribute("data-loading");
        latestButton.removeAttribute("aria-busy");
        if (!revertWasDisabled) {
          latestButton.disabled = false;
          latestButton.setAttribute("aria-disabled", "false");
        }
      }
    }
  })();

  pendingRevisionSnapshotLoads.set(loadKey, loadPromise);
  try {
    await loadPromise;
  } finally {
    pendingRevisionSnapshotLoads.delete(loadKey);
  }
}
