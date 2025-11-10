import { getDefaultFieldLabels } from "./labels";

export interface AppState {
  app: {
    ready: boolean;
    version: string | null;
    hasFileAccess: boolean;
    hasDatabase: boolean;
    activeSection: "calc" | "history" | "zulassung" | "settings" | "report";
    storageDriver: "memory" | "sqlite" | "filesystem" | "localstorage";
  };
  company: {
    name: string;
    headline: string;
    logoUrl: string;
    contactEmail: string;
    address: string;
    accentColor: string;
  };
  defaults: {
    waterPerKisteL: number;
    kistenProAr: number;
    form: {
      creator: string;
      location: string;
      crop: string;
      quantity: string;
    };
  };
  measurementMethods: any[];
  mediums: any[];
  history: any[];
  fieldLabels: any;
  calcContext: any | null;
  zulassung: {
    filters: {
      culture: string | null;
      pest: string | null;
      text: string;
      includeExpired: boolean;
      bioOnly: boolean;
    };
    results: any[];
    lastSync: string | null;
    lastResultCounts: any | null;
    dataSource: string | null;
    apiStand: string | null;
    manifestVersion: string | null;
    lastSyncHash: string | null;
    busy: boolean;
    progress: {
      step: string | null;
      percent: number;
      message: string;
    };
    error: string | null;
    logs: any[];
    debug: {
      schema: any | null;
      lastSyncLog: any[];
      manifest: any | null;
      lastAutoUpdateCheck: any | null;
    };
    lookups: {
      cultures: any[];
      pests: any[];
    };
    autoUpdateAvailable: boolean;
    autoUpdateVersion: string | null;
  };
  ui: {
    notifications: any[];
  };
}

type StateListener = (state: AppState, prevState: AppState) => void;

const listeners = new Set<StateListener>();

let state: AppState = {
  app: {
    ready: false,
    version: null,
    hasFileAccess: false,
    hasDatabase: false,
    activeSection: "calc",
    storageDriver: "memory",
  },
  company: {
    name: "",
    headline: "",
    logoUrl: "",
    contactEmail: "",
    address: "",
    accentColor: "",
  },
  defaults: {
    waterPerKisteL: 5,
    kistenProAr: 300,
    form: {
      creator: "",
      location: "",
      crop: "",
      quantity: "",
    },
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
      includeExpired: false,
      bioOnly: false,
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
      lastAutoUpdateCheck: null,
    },
    lookups: { cultures: [], pests: [] },
    autoUpdateAvailable: false,
    autoUpdateVersion: null,
  },
  ui: {
    notifications: [],
  },
};

export function getState(): AppState {
  return state;
}

export function getSlice<K extends keyof AppState>(key: K): AppState[K] {
  return state[key];
}

export function subscribeState(listener: StateListener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function notify(prevState: AppState): void {
  for (const listener of listeners) {
    try {
      listener(state, prevState);
    } catch (err) {
      console.error("state listener error", err);
    }
  }
}

export function patchState(patch: Partial<AppState>): AppState {
  const prevState = state;
  state = { ...state, ...patch };
  notify(prevState);
  return state;
}

export function updateSlice<K extends keyof AppState>(
  sliceKey: K,
  updater:
    | ((current: AppState[K], state: AppState) => AppState[K])
    | AppState[K]
): AppState {
  const currentSlice = state[sliceKey];
  const nextSlice =
    typeof updater === "function"
      ? (updater as (current: AppState[K], state: AppState) => AppState[K])(
          currentSlice,
          state
        )
      : updater;
  if (nextSlice === currentSlice) {
    return state;
  }
  return patchState({ [sliceKey]: nextSlice } as Partial<AppState>);
}

export function resetState(newState?: AppState): AppState {
  const base = newState || {
    app: {
      ready: false,
      version: null,
      hasFileAccess: false,
      hasDatabase: false,
      activeSection: "calc" as const,
      storageDriver: "memory" as const,
    },
    company: {
      name: "",
      headline: "",
      logoUrl: "",
      contactEmail: "",
      address: "",
      accentColor: "",
    },
    defaults: {
      waterPerKisteL: 5,
      kistenProAr: 300,
      form: {
        creator: "",
        location: "",
        crop: "",
        quantity: "",
      },
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
        includeExpired: false,
        bioOnly: false,
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
        lastAutoUpdateCheck: null,
      },
      lookups: { cultures: [], pests: [] },
      autoUpdateAvailable: false,
      autoUpdateVersion: null,
    },
    ui: {
      notifications: [],
    },
  };
  const prevState = state;
  state = base;
  notify(prevState);
  return state;
}
