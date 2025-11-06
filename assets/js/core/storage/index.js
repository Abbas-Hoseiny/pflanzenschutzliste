import * as fileSystemDriver from './fileSystem.js';
import * as fallbackDriver from './fallback.js';
import { getState, patchState } from '../state.js';

const DRIVERS = {
  fileSystem: fileSystemDriver,
  fallback: fallbackDriver
};

export function detectPreferredDriver() {
  if (fileSystemDriver.isSupported()) {
    return 'fileSystem';
  }
  if (fallbackDriver.isSupported()) {
    return 'fallback';
  }
  return 'memory';
}

let activeDriverKey = detectPreferredDriver();

export function setActiveDriver(driverKey) {
  if (!DRIVERS[driverKey]) {
    throw new Error(`Unbekannter Storage-Treiber: ${driverKey}`);
  }
  activeDriverKey = driverKey;
  patchState({
    app: {
      ...getState().app,
      storageDriver: driverKey,
      hasFileAccess: driverKey === 'fileSystem'
    }
  });
}

export function getActiveDriverKey() {
  return activeDriverKey;
}

export function getDriver() {
  return DRIVERS[activeDriverKey];
}

export async function createDatabase(initialData) {
  const driver = getDriver();
  if (!driver) {
    throw new Error('Kein verfügbarer Speicher-Treiber');
  }
  return driver.create(initialData);
}

export async function openDatabase() {
  const driver = getDriver();
  if (!driver) {
    throw new Error('Kein verfügbarer Speicher-Treiber');
  }
  return driver.open();
}

export async function saveDatabase(data) {
  const driver = getDriver();
  if (!driver) {
    throw new Error('Kein verfügbarer Speicher-Treiber');
  }
  return driver.save(data);
}

export function resetDriverContext() {
  const driver = getDriver();
  if (driver && typeof driver.reset === 'function') {
    driver.reset();
  }
}
