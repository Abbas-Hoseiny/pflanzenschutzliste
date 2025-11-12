// src/scripts/core/labels.ts
var DEFAULT_FIELD_LABELS = {
  calculation: {
    fields: {
      creator: {
        label: "Erstellt von",
        placeholder: "Name der verantwortlichen Person"
      },
      location: {
        label: "Standort / Abteil",
        placeholder: "z. B. Gew\xE4chshaus 1"
      },
      crop: {
        label: "Kultur",
        placeholder: "z. B. Salat"
      },
      quantity: {
        label: "Anzahl Kisten",
        placeholder: "z. B. 42",
        unit: "Kisten"
      }
    },
    summary: {
      water: "Gesamtwasser (L)",
      area: "Fl\xE4che (Ar / m\xB2)"
    },
    tableColumns: {
      medium: "Mittel",
      unit: "Einheit",
      method: "Methode",
      value: "Wert",
      perQuantity: "Kisten",
      areaAr: "Ar",
      areaSqm: "m\xB2",
      total: "Gesamt"
    },
    resultTitle: "Ben\xF6tigte Mittel"
  },
  history: {
    tableColumns: {
      date: "Datum",
      creator: "Erstellt von",
      location: "Standort",
      crop: "Kultur",
      quantity: "Kisten"
    },
    detail: {
      title: "Historieneintrag",
      creator: "Erstellt von",
      location: "Standort / Abteil",
      crop: "Kultur",
      quantity: "Kisten"
    },
    summaryTitle: "Historie (Zusammenfassung)",
    mediumsHeading: "Mittel & Gesamtmengen"
  },
  reporting: {
    tableColumns: {
      date: "Datum",
      creator: "Erstellt von",
      location: "Standort",
      crop: "Kultur",
      quantity: "Kisten",
      mediums: "Mittel & Gesamtmengen"
    },
    infoAll: "Alle Eintr\xE4ge",
    infoEmpty: "Keine Eintr\xE4ge vorhanden",
    infoPrefix: "Zeitraum",
    printTitle: "Auswertung"
  }
};
function cloneLabels(labels) {
  return JSON.parse(JSON.stringify(labels));
}
function getDefaultFieldLabels() {
  return cloneLabels(DEFAULT_FIELD_LABELS);
}

// src/scripts/core/state.ts
var listeners = /* @__PURE__ */ new Set();
var state = {
  app: {
    ready: false,
    version: null,
    hasFileAccess: false,
    hasDatabase: false,
    activeSection: "calc",
    storageDriver: "memory"
  },
  company: {
    name: "",
    headline: "",
    logoUrl: "",
    contactEmail: "",
    address: "",
    accentColor: ""
  },
  defaults: {
    waterPerKisteL: 5,
    kistenProAr: 300,
    form: {
      creator: "",
      location: "",
      crop: "",
      quantity: ""
    }
  },
  measurementMethods: [],
  mediums: [],
  history: [],
  fieldLabels: getDefaultFieldLabels(),
  calcContext: null,
  zulassung: {
    filters: {
      culture: null,
      pest: null,
      text: "",
      includeExpired: false
    },
    results: [],
    lastSync: null,
    lastResultCounts: null,
    dataSource: null,
    apiStand: null,
    manifestVersion: null,
    lastSyncHash: null,
    busy: false,
    progress: { step: null, percent: 0, message: "" },
    error: null,
    logs: [],
    debug: {
      schema: null,
      lastSyncLog: [],
      manifest: null,
      lastAutoUpdateCheck: null
    },
    lookups: { cultures: [], pests: [] },
    autoUpdateAvailable: false,
    autoUpdateVersion: null
  },
  ui: {
    notifications: []
  }
};
function subscribeState(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
function notify(prevState) {
  for (const listener of listeners) {
    try {
      listener(state, prevState);
    } catch (err) {
      console.error("state listener error", err);
    }
  }
}
function patchState(patch) {
  const prevState = state;
  state = { ...state, ...patch };
  notify(prevState);
  return state;
}
function updateSlice(sliceKey, updater) {
  const currentSlice = state[sliceKey];
  const nextSlice = typeof updater === "function" ? updater(
    currentSlice,
    state
  ) : updater;
  if (nextSlice === currentSlice) {
    return state;
  }
  return patchState({ [sliceKey]: nextSlice });
}

// src/scripts/core/eventBus.ts
var subscribers = /* @__PURE__ */ new Map();
function emit(eventName, payload) {
  const handlers = subscribers.get(eventName);
  if (!handlers) {
    return;
  }
  for (const handler of handlers.values()) {
    try {
      handler(payload);
    } catch (err) {
      console.error(`eventBus handler error for ${eventName}`, err);
    }
  }
}

// src/scripts/components/shellClient.ts
if (typeof document !== "undefined") {
  let initShell = function() {
    const buttons = document.querySelectorAll(".nav-btn");
    buttons.forEach((button) => {
      button.addEventListener("click", (event) => {
        const target = event.currentTarget;
        if (!(target instanceof HTMLElement)) {
          return;
        }
        const section = target.dataset.section;
        if (!section) {
          return;
        }
        updateSlice("app", (app) => ({ ...app, activeSection: section }));
        buttons.forEach((btn) => btn.classList.remove("active"));
        button.classList.add("active");
        emit("app:sectionChanged", section);
      });
    });
    subscribeState((state2) => {
      const brandTitle = document.getElementById("brand-title");
      const brandTagline = document.getElementById("brand-tagline");
      const appVersion = document.getElementById("app-version");
      if (brandTitle && state2.company.name) {
        brandTitle.textContent = state2.company.name;
      }
      if (brandTagline && state2.company.headline) {
        brandTagline.textContent = state2.company.headline;
      }
      if (appVersion && state2.app.version) {
        appVersion.textContent = `v${state2.app.version}`;
      }
      buttons.forEach((btn) => {
        const section = btn.getAttribute("data-section");
        btn.disabled = !state2.app.hasDatabase;
        if (section === state2.app.activeSection) {
          btn.classList.add("active");
        } else {
          btn.classList.remove("active");
        }
        if (!state2.app.hasDatabase) {
          btn.classList.remove("active");
        }
      });
    });
    const calcBtn = document.querySelector('[data-section="calc"]');
    if (calcBtn instanceof HTMLElement) {
      calcBtn.classList.add("active");
    }
    let isPrinting = false;
    const activeDocumentTitle = document.title || "Pflanzenschutz";
    window.addEventListener("beforeprint", () => {
      if (isPrinting) return;
      isPrinting = true;
      document.title = " ";
    });
    window.addEventListener("afterprint", () => {
      if (!isPrinting) return;
      isPrinting = false;
      document.title = activeDocumentTitle;
    });
  }, bootstrapShell = function() {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", initShell, { once: true });
    } else {
      initShell();
    }
  };
  initShell2 = initShell, bootstrapShell2 = bootstrapShell;
  bootstrapShell();
}
var initShell2;
var bootstrapShell2;
