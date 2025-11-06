import { loadDefaultsConfig } from './config.js';
import { detectPreferredDriver, setActiveDriver } from './storage/index.js';
import { getState, patchState, subscribeState, updateSlice } from './state.js';
import { emit, subscribe as subscribeEvent } from './eventBus.js';

import { initStarfield } from '../features/starfield/index.js';
import { initShell } from '../features/shell/index.js';
import { initStartup } from '../features/startup/index.js';
import { initCalculation } from '../features/calculation/index.js';
import { initHistory } from '../features/history/index.js';
import { initSettings } from '../features/settings/index.js';
import { initReporting } from '../features/reporting/index.js';

function getRegions() {
  const root = document.getElementById('app-root');
  if (!root) {
    throw new Error('app-root Container fehlt');
  }
  return {
    startup: root.querySelector('[data-region="startup"]'),
    shell: root.querySelector('[data-region="shell"]'),
    main: root.querySelector('[data-region="main"]'),
    footer: root.querySelector('[data-region="footer"]')
  };
}

export async function bootstrap() {
  const regions = getRegions();

  const driverKey = detectPreferredDriver();
  if (driverKey !== 'memory') {
    setActiveDriver(driverKey);
  }

  await loadDefaultsConfig();

  const services = {
    state: {
      getState,
      patchState,
      updateSlice,
      subscribe: subscribeState
    },
    events: {
      emit,
      subscribe: subscribeEvent
    }
  };

  initStarfield();
  initShell({ shell: regions.shell, footer: regions.footer }, services);
  initStartup(regions.startup, services);
  initCalculation(regions.main, services);
  initHistory(regions.main, services);
  initSettings(regions.main, services);
  initReporting(regions.main, services);

  patchState({
    app: {
      ...getState().app,
      ready: true
    }
  });
}
