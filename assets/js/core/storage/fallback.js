const STORAGE_KEY = 'pflanzenschutzliste-db';

function hasLocalStorage() {
  try {
    const key = '__storage_test__';
    window.localStorage.setItem(key, '1');
    window.localStorage.removeItem(key);
    return true;
  } catch (err) {
    return false;
  }
}

export function isSupported() {
  return typeof window !== 'undefined' && hasLocalStorage();
}

export async function create(initialData) {
  if (!isSupported()) {
    throw new Error('LocalStorage nicht verfügbar');
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(initialData));
  return { data: initialData, context: { storageKey: STORAGE_KEY } };
}

export async function open() {
  if (!isSupported()) {
    throw new Error('LocalStorage nicht verfügbar');
  }
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    throw new Error('Keine gespeicherten Daten gefunden');
  }
  return { data: JSON.parse(raw), context: { storageKey: STORAGE_KEY } };
}

export async function save(data) {
  if (!isSupported()) {
    throw new Error('LocalStorage nicht verfügbar');
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
  return { context: { storageKey: STORAGE_KEY } };
}

export function getContext() {
  return { storageKey: STORAGE_KEY };
}

export function reset() {
  if (!isSupported()) {
    return;
  }
  window.localStorage.removeItem(STORAGE_KEY);
}
