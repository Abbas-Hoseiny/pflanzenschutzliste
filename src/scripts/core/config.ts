import defaultsSource from "../../config/defaults.json";
import { resolveFieldLabels } from "./labels";
import { getState, patchState } from "./state";

let cachedDefaults: any = null;

function mergeDefaults(base: any = {}, incoming: any = {}): any {
  const merged = { ...base, ...incoming };
  merged.form = {
    ...(base.form || { creator: "", location: "", crop: "", quantity: "" }),
    ...(incoming.form || {}),
  };
  return merged;
}

export async function loadDefaultsConfig(): Promise<any> {
  if (cachedDefaults) {
    return cachedDefaults;
  }
  const defaults = JSON.parse(JSON.stringify(defaultsSource));
  cachedDefaults = defaults;

  const current = getState();
  patchState({
    app: {
      ...current.app,
      version: defaults.meta?.version ?? 1,
    },
  });

  // Apply defaults to store
  patchState({
    company: { ...current.company, ...(defaults.meta?.company ?? {}) },
    defaults: mergeDefaults(current.defaults, defaults.meta?.defaults ?? {}),
    measurementMethods: [...(defaults.meta?.measurementMethods ?? [])],
    mediums: [...(defaults.mediums ?? [])],
    history: [...(defaults.history ?? [])],
    fieldLabels: resolveFieldLabels(defaults.meta?.fieldLabels ?? {}),
  });

  return cachedDefaults;
}

export function getDefaultsConfig(): any {
  return cachedDefaults;
}
