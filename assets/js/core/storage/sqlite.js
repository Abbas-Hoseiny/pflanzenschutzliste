/**
 * SQLite Storage Driver
 * Main thread interface to SQLite Worker
 */

let worker = null;
let messageId = 0;
let pendingMessages = new Map();
let fileHandle = null;

/**
 * Check if SQLite-WASM is supported
 */
export function isSupported() {
  // Check for Web Worker, WebAssembly, and preferably OPFS support
  if (typeof Worker === 'undefined' || typeof WebAssembly === 'undefined') {
    return false;
  }
  
  // Check if we're in a secure context (required for OPFS)
  if (typeof window !== 'undefined' && !window.isSecureContext) {
    return false;
  }
  
  return true;
}

/**
 * Call worker method
 */
function callWorker(action, payload) {
  return new Promise((resolve, reject) => {
    if (!worker) {
      reject(new Error('Worker not initialized'));
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
async function initWorker() {
  if (worker) {
    return; // Already initialized
  }
  
  try {
    // Create worker with module type
    worker = new Worker(
      new URL('./sqliteWorker.js', import.meta.url),
      { type: 'module' }
    );
    
    // Handle worker messages
    worker.onmessage = (event) => {
      const { id, ok, result, error } = event.data;
      
      if (pendingMessages.has(id)) {
        const { resolve, reject } = pendingMessages.get(id);
        pendingMessages.delete(id);
        
        if (ok) {
          resolve(result);
        } else {
          reject(new Error(error || 'Worker error'));
        }
      }
    };
    
    worker.onerror = (error) => {
      console.error('Worker error:', error);
    };
    
    // Initialize database in worker
    await callWorker('init', {});
  } catch (error) {
    console.error('Failed to initialize worker:', error);
    throw error;
  }
}

/**
 * Create a new database
 */
export async function create(initialData, suggestedName = 'pflanzenschutz.sqlite') {
  if (!isSupported()) {
    throw new Error('SQLite-WASM is not supported in this browser');
  }
  
  await initWorker();
  
  // Import initial data into SQLite
  await callWorker('importSnapshot', initialData);
  
  // Optionally save to file (if File System Access API is available)
  if (typeof window.showSaveFilePicker === 'function') {
    try {
      fileHandle = await window.showSaveFilePicker({
        suggestedName,
        types: [{
          description: 'SQLite Database',
          accept: { 'application/x-sqlite3': ['.sqlite', '.db'] }
        }]
      });
      
      // Export and save database
      const exported = await callWorker('exportDB');
      const writable = await fileHandle.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err) {
      // User cancelled or error - continue without file handle
      console.warn('Could not save SQLite file:', err);
      fileHandle = null;
    }
  }
  
  return { data: initialData, context: { fileHandle } };
}

/**
 * Open an existing database
 */
export async function open() {
  if (!isSupported()) {
    throw new Error('SQLite-WASM is not supported in this browser');
  }
  
  await initWorker();
  
  // Try to open via File System Access API
  if (typeof window.showOpenFilePicker === 'function') {
    try {
      const [handle] = await window.showOpenFilePicker({
        types: [{
          description: 'SQLite Database or JSON',
          accept: {
            'application/x-sqlite3': ['.sqlite', '.db'],
            'application/json': ['.json']
          }
        }]
      });
      
      fileHandle = handle;
      const file = await handle.getFile();
      const arrayBuffer = await file.arrayBuffer();
      
      // Check if it's JSON or SQLite by file extension or content
      if (file.name.endsWith('.json')) {
        // Import JSON
        const text = await file.text();
        const data = JSON.parse(text);
        await callWorker('importSnapshot', data);
        return { data, context: { fileHandle } };
      } else {
        // Import SQLite binary
        await callWorker('importDB', arrayBuffer);
        const data = await callWorker('exportSnapshot');
        return { data, context: { fileHandle } };
      }
    } catch (err) {
      console.error('Failed to open file:', err);
      throw err;
    }
  } else {
    throw new Error('File System Access API not available');
  }
}

/**
 * Save current state to database
 */
export async function save(data) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  
  // Update database with new data
  await callWorker('importSnapshot', data);
  
  // If we have a file handle, save to file
  if (fileHandle) {
    try {
      const exported = await callWorker('exportDB');
      const writable = await fileHandle.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err) {
      console.error('Failed to save to file:', err);
      // Continue anyway - data is still in OPFS if available
    }
  }
  
  return { context: { fileHandle } };
}

/**
 * Get current context
 */
export function getContext() {
  return { fileHandle, worker };
}

/**
 * Reset driver state
 */
export function reset() {
  if (worker) {
    worker.terminate();
    worker = null;
  }
  fileHandle = null;
  pendingMessages.clear();
  messageId = 0;
}

/**
 * Export database as JSON
 */
export async function exportAsJson() {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('exportSnapshot');
}

/**
 * Export database as SQLite file
 */
export async function exportAsSQLite() {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('exportDB');
}

/**
 * Get mediums list
 */
export async function getMediums() {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('listMediums');
}

/**
 * Get history with paging
 */
export async function getHistory(options) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('listHistory', options);
}

/**
 * Get single history entry
 */
export async function getHistoryEntry(id) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('getHistoryEntry', id);
}

/**
 * Add history entry
 */
export async function addHistoryEntry(entry) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('appendHistoryEntry', entry);
}

/**
 * Delete history entry
 */
export async function deleteHistory(id) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('deleteHistoryEntry', id);
}

/**
 * Upsert medium
 */
export async function upsertMedium(medium) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('upsertMedium', medium);
}

/**
 * Delete medium
 */
export async function deleteMedium(id) {
  if (!worker) {
    throw new Error('Database not initialized');
  }
  return await callWorker('deleteMedium', id);
}
