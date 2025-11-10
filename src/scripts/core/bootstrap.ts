import { loadDefaultsConfig } from "./config";
import { detectPreferredDriver, setActiveDriver } from "./storage/index";
import { getState, patchState, subscribeState, updateSlice } from "./state";
import { emit, subscribe as subscribeEvent } from "./eventBus";
import { initStarfield } from "../features/starfield";

// Feature imports - to be migrated
// import { initStarfield } from '../features/starfield/index';
// import { initShell } from '../features/shell/index';
// import { initStartup } from '../features/startup/index';
// import { initCalculation } from '../features/calculation/index';
// import { initHistory } from '../features/history/index';
// import { initSettings } from '../features/settings/index';
// import { initReporting } from '../features/reporting/index';
// import { initZulassung } from '../features/zulassung/index';

function setupUnloadWarning(stateService: any): void {
  const handler = (event: BeforeUnloadEvent) => {
    event.preventDefault();
    event.returnValue =
      "Die Verbindung zur Datenbank wird getrennt. Ungespeicherte Änderungen können verloren gehen.";
    return event.returnValue;
  };
  let active = false;
  const update = (state: any) => {
    const shouldWarn = Boolean(state.app?.hasDatabase);
    if (shouldWarn && !active) {
      window.addEventListener("beforeunload", handler);
      active = true;
    } else if (!shouldWarn && active) {
      window.removeEventListener("beforeunload", handler);
      active = false;
    }
  };
  update(stateService.getState());
  stateService.subscribe(update);
}

function getRegions() {
  const root = document.getElementById("app-root");
  if (!root) {
    throw new Error("app-root Container fehlt");
  }
  return {
    startup: root.querySelector('[data-region="startup"]'),
    shell: root.querySelector('[data-region="shell"]'),
    main: root.querySelector('[data-region="main"]'),
    footer: root.querySelector('[data-region="footer"]'),
  };
}

export async function bootstrap() {
  getRegions();

  const driverKey = detectPreferredDriver();
  if (driverKey !== "memory") {
    setActiveDriver(driverKey);
  }

  await loadDefaultsConfig();

  const services = {
    state: {
      getState,
      patchState,
      updateSlice,
      subscribe: subscribeState,
    },
    events: {
      emit,
      subscribe: subscribeEvent,
    },
  };

  // Feature initialization - to be implemented in Astro components
  initStarfield();
  // initShell({ shell: regions.shell, footer: regions.footer }, services);
  // initStartup(regions.startup, services);
  // initCalculation(regions.main, services);
  // initHistory(regions.main, services);
  // initSettings(regions.main, services);
  // initReporting(regions.main, services);
  // initZulassung(regions.main, services);
  setupUnloadWarning(services.state);

  patchState({
    app: {
      ...getState().app,
      ready: true,
    },
  });
}
