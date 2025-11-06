import { getState, patchState } from './state.js';

let cachedDefaults = null;

export async function loadDefaultsConfig() {
  if (cachedDefaults) {
    return cachedDefaults;
  }
  const response = await fetch('assets/config/defaults.json', { cache: 'no-cache' });
  if (!response.ok) {
    throw new Error(`defaults.json not found (${response.status})`);
  }
  const defaults = await response.json();
  cachedDefaults = defaults;

  const current = getState();
  patchState({
    app: {
      ...current.app,
      version: defaults.meta?.version ?? 1
    }
  });

  // Apply defaults to store
  patchState({
    company: { ...current.company, ...(defaults.meta?.company ?? {}) },
    defaults: { ...current.defaults, ...(defaults.meta?.defaults ?? {}) },
    measurementMethods: [...(defaults.meta?.measurementMethods ?? [])],
    mediums: [...(defaults.mediums ?? [])],
    history: [...(defaults.history ?? [])]
  });

  return cachedDefaults;
}

export function getDefaultsConfig() {
  return cachedDefaults;
}
