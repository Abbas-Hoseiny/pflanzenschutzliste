import { getDefaultsConfig } from './config.js';
import { getState, patchState } from './state.js';

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

export function applyDatabase(data) {
  if (!data) {
    throw new Error('Keine Daten zum Anwenden übergeben');
  }
  const current = getState();
  patchState({
    company: { ...current.company, ...(data.meta?.company ?? {}) },
    defaults: { ...current.defaults, ...(data.meta?.defaults ?? {}) },
    measurementMethods: [...(data.meta?.measurementMethods ?? current.measurementMethods)],
    mediums: [...(data.mediums ?? [])],
    history: [...(data.history ?? [])],
    app: {
      ...current.app,
      hasDatabase: true
    }
  });
}

export function createInitialDatabase() {
  const defaults = getDefaultsConfig();
  if (defaults) {
    return clone(defaults);
  }
  const state = getState();
  return {
    meta: {
      version: state.app.version || 1,
      company: { ...state.company },
      defaults: { ...state.defaults },
      measurementMethods: [...state.measurementMethods]
    },
    mediums: [...state.mediums],
    history: []
  };
}

export function getDatabaseSnapshot() {
  const state = getState();
  return {
    meta: {
      version: state.app.version || 1,
      company: { ...state.company },
      defaults: { ...state.defaults },
      measurementMethods: [...state.measurementMethods]
    },
    mediums: [...state.mediums],
    history: [...state.history]
  };
}
