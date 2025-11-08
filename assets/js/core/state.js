import { getDefaultFieldLabels } from './labels.js';

const listeners = new Set();

let state = {
  app: {
    ready: false,
    version: null,
  hasFileAccess: false,
  hasDatabase: false,
    activeSection: 'calc',
    storageDriver: 'memory'
  },
  company: {
    name: '',
    headline: '',
    logoUrl: '',
    contactEmail: '',
    address: '',
    accentColor: ''
  },
  defaults: {
    waterPerKisteL: 5,
    kistenProAr: 300,
    form: {
      creator: '',
      location: '',
      crop: '',
      quantity: ''
    }
  },
  measurementMethods: [],
  mediums: [],
  history: [],
  fieldLabels: getDefaultFieldLabels(),
  calcContext: null,
  ui: {
    notifications: []
  },
  zulassung: {
    filters: { culture: null, pest: null, text: '', includeExpired: false },
    results: [],
    lastSync: null,
    busy: false,
    error: null,
    lookups: { cultures: [], pests: [] }
  }
};

export function getState() {
  return state;
}

export function getSlice(key) {
  return state[key];
}

export function subscribeState(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function notify(prevState) {
  for (const listener of listeners) {
    try {
      listener(state, prevState);
    } catch (err) {
      console.error('state listener error', err);
    }
  }
}

export function patchState(patch) {
  const prevState = state;
  state = { ...state, ...patch };
  notify(prevState);
  return state;
}

export function updateSlice(sliceKey, updater) {
  const currentSlice = state[sliceKey];
  const nextSlice = typeof updater === 'function' ? updater(currentSlice, state) : updater;
  if (nextSlice === currentSlice) {
    return state;
  }
  return patchState({ [sliceKey]: nextSlice });
}

export function resetState(newState = undefined) {
  const base = newState || {
    app: {
      ready: false,
      version: null,
  hasFileAccess: false,
  hasDatabase: false,
      activeSection: 'calc',
      storageDriver: 'memory'
    },
    company: {
      name: '',
      headline: '',
      logoUrl: '',
      contactEmail: '',
      address: '',
      accentColor: ''
    },
    defaults: {
      waterPerKisteL: 5,
      kistenProAr: 300,
      form: {
        creator: '',
        location: '',
        crop: '',
        quantity: ''
      }
    },
    measurementMethods: [],
    mediums: [],
    history: [],
    fieldLabels: getDefaultFieldLabels(),
    calcContext: null,
    ui: {
      notifications: []
    },
    zulassung: {
      filters: { culture: null, pest: null, text: '', includeExpired: false },
      results: [],
      lastSync: null,
      busy: false,
      error: null,
      lookups: { cultures: [], pests: [] }
    }
  };
  const prevState = state;
  state = base;
  notify(prevState);
  return state;
}
