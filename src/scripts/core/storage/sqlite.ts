/**
 * SQLite Storage Driver
 * Main thread interface to SQLite Worker
 */

let worker: Worker | null = null;
let messageId = 0;
let pendingMessages = new Map<
  number,
  { resolve: (value: any) => void; reject: (reason?: any) => void }
>();
let fileHandle: any = null;

/**
 * Check if SQLite-WASM is supported
 */
export function isSupported(): boolean {
  // Check for Web Worker, WebAssembly, and preferably OPFS support
  if (typeof Worker === "undefined" || typeof WebAssembly === "undefined") {
    return false;
  }

  // Check if we're in a secure context (required for OPFS)
  if (typeof window !== "undefined" && !window.isSecureContext) {
    return false;
  }

  return true;
}

/**
 * Call worker method
 */
function callWorker(action: string, payload?: any): Promise<any> {
  return new Promise((resolve, reject) => {
    if (!worker) {
      reject(new Error("Worker not initialized"));
      return;
    }

    const id = ++messageId;
    pendingMessages.set(id, { resolve, reject });

    worker.postMessage({ id, action, payload });

    // Timeout after 30 seconds
    setTimeout(() => {
      if (pendingMessages.has(id)) {
        pendingMessages.delete(id);
        reject(new Error(`Worker call timeout: ${action}`));
      }
    }, 30000);
  });
}

/**
 * Initialize worker
 */
async function initWorker(): Promise<void> {
  if (worker) {
    return; // Already initialized
  }

  try {
    // Create worker with module type
    worker = new Worker(new URL("./sqliteWorker.js", import.meta.url), {
      type: "module",
    });

    // Handle worker messages
    worker.onmessage = (event) => {
      const { id, ok, result, error } = event.data;

      if (pendingMessages.has(id)) {
        const { resolve, reject } = pendingMessages.get(id)!;
        pendingMessages.delete(id);

        if (ok) {
          resolve(result);
        } else {
          reject(new Error(error || "Worker error"));
        }
      }
    };

    worker.onerror = (error) => {
      console.error("Worker error:", error);
    };

    // Initialize database in worker
    await callWorker("init", {});
  } catch (error) {
    console.error("Failed to initialize worker:", error);
    throw error;
  }
}

/**
 * Create a new database
 */
export async function create(
  initialData: any,
  suggestedName: string = "pflanzenschutz.sqlite"
): Promise<{ data: any; context: any }> {
  if (!isSupported()) {
    throw new Error("SQLite-WASM is not supported in this browser");
  }

  await initWorker();

  // Import initial data into SQLite
  await callWorker("importSnapshot", initialData);

  // Optionally save to file (if File System Access API is available)
  if (typeof (window as any).showSaveFilePicker === "function") {
    try {
      fileHandle = await (window as any).showSaveFilePicker({
        suggestedName,
        types: [
          {
            description: "SQLite Database",
            accept: { "application/x-sqlite3": [".sqlite", ".db"] },
          },
        ],
      });

      // Export and save database
      const exported = await callWorker("exportDB");
      const writable = await fileHandle.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err) {
      // User cancelled or error - continue without file handle
      console.warn("Could not save SQLite file:", err);
      fileHandle = null;
    }
  }

  return { data: initialData, context: { fileHandle } };
}

/**
 * Open an existing database
 */
export async function open(): Promise<{ data: any; context: any }> {
  if (!isSupported()) {
    throw new Error("SQLite-WASM is not supported in this browser");
  }

  await initWorker();

  // Try to open via File System Access API
  if (typeof (window as any).showOpenFilePicker === "function") {
    try {
      const [handle] = await (window as any).showOpenFilePicker({
        types: [
          {
            description: "SQLite Database or JSON",
            accept: {
              "application/x-sqlite3": [".sqlite", ".db"],
              "application/json": [".json"],
            },
          },
        ],
      });

      fileHandle = handle;
      const file = await handle.getFile();
      const arrayBuffer = await file.arrayBuffer();

      // Check if it's JSON or SQLite by file extension or content
      if (file.name.endsWith(".json")) {
        // Import JSON
        const text = await file.text();
        const data = JSON.parse(text);
        await callWorker("importSnapshot", data);
        return { data, context: { fileHandle } };
      } else {
        // Import SQLite binary
        await callWorker("importDB", arrayBuffer);
        const data = await callWorker("exportSnapshot");
        return { data, context: { fileHandle } };
      }
    } catch (err) {
      console.error("Failed to open file:", err);
      throw err;
    }
  } else {
    throw new Error("File System Access API not available");
  }
}

/**
 * Save current state to database
 */
export async function save(data: any): Promise<{ context: any }> {
  if (!worker) {
    throw new Error("Database not initialized");
  }

  // Update database with new data
  await callWorker("importSnapshot", data);

  // If we have a file handle, save to file
  if (fileHandle) {
    try {
      const exported = await callWorker("exportDB");
      const writable = await fileHandle.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err) {
      console.error("Failed to save to file:", err);
    }
  }

  return { context: { fileHandle } };
}

/**
 * Get current context
 */
export function getContext(): any {
  return { fileHandle };
}

/**
 * Reset and close database
 */
export function reset(): void {
  if (worker) {
    worker.terminate();
    worker = null;
  }
  pendingMessages.clear();
  fileHandle = null;
  messageId = 0;
}

/**
 * Execute raw SQL query
 */
export async function query(sql: string, params?: any[]): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("query", { sql, params });
}

/**
 * Execute multiple SQL statements
 */
export async function exec(sql: string): Promise<void> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("exec", { sql });
}

/**
 * Export database snapshot
 */
export async function exportSnapshot(): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("exportSnapshot");
}

/**
 * Import database snapshot
 */
export async function importSnapshot(data: any): Promise<void> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importSnapshot", data);
}

/**
 * Export raw database binary
 */
export async function exportDB(): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("exportDB");
}

/**
 * Import raw database binary
 */
export async function importDB(arrayBuffer: ArrayBuffer): Promise<void> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importDB", arrayBuffer);
}

/**
 * Import BVL dataset
 */
export async function importBvlDataset(dataset: any): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("importBvlDataset", dataset);
}

export async function importBvlSqlite(
  data: ArrayBuffer,
  manifest: any
): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("importBvlSqlite", { data, manifest });
}

export async function getBvlMeta(key: string): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("getBvlMeta", key);
}

export async function setBvlMeta(key: string, value: any): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("setBvlMeta", { key, value });
}

export async function appendBvlSyncLog(entry: any): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("appendBvlSyncLog", entry);
}

export async function listBvlSyncLog(options: any = {}): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlSyncLog", options);
}

export async function queryZulassung(params: any): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("queryZulassung", params);
}

/**
 * Query BVL data
 */
export async function queryBvl(filters: any): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("queryBvl", filters);
}

/**
 * Get BVL sync status
 */
export async function getBvlSyncStatus(): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("getBvlSyncStatus", {});
}

/**
 * List BVL cultures
 */
export async function listBvlCultures(options: any = {}): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlCultures", options);
}

/**
 * List BVL Schadorganismen
 */
export async function listBvlSchadorg(options: any = {}): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlSchadorg", options);
}

/**
 * Diagnose BVL schema
 */
export async function diagnoseBvlSchema(): Promise<any> {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("diagnoseBvlSchema", {});
}
