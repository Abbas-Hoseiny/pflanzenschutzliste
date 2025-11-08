/**
 * BVL Sync Orchestrator
 * Coordinates data fetching, transformation, and storage for BVL data
 */

import { fetchCollection, hashData, classifyError } from './bvlClient.js';
import * as sqlite from './storage/sqlite.js';

// Default endpoints to sync
const DEFAULT_ENDPOINTS = [
  'mittel',
  'awg',
  'awg_kultur',
  'awg_schadorg',
  'awg_aufwand',
  'awg_wartezeit'
];

/**
 * Sync BVL data from API to local database
 * @param {Object} options - Sync options
 * @param {Array<string>} options.endpoints - Endpoints to sync (defaults to all)
 * @param {Function} options.onProgress - Progress callback(current, total, message)
 * @returns {Promise<Object>} Result object with status, counts, message
 */
export async function syncBvlData(options = {}) {
  const { 
    endpoints = DEFAULT_ENDPOINTS, 
    onProgress = null 
  } = options;
  
  const controller = new AbortController();
  const signal = controller.signal;
  
  try {
    // Read current metadata
    reportProgress(onProgress, 0, endpoints.length + 2, 'Lese Metadaten...');
    const lastSyncHash = await sqlite.getBvlMeta('lastSyncHash');
    
    // Fetch data from all endpoints
    reportProgress(onProgress, 1, endpoints.length + 2, 'Lade Daten von BVL API...');
    const rawData = {};
    
    for (let i = 0; i < endpoints.length; i++) {
      const endpoint = endpoints[i];
      reportProgress(onProgress, i + 2, endpoints.length + 2, `Lade ${endpoint}...`);
      
      try {
        rawData[endpoint] = await fetchCollection(endpoint, { signal });
      } catch (error) {
        console.error(`Failed to fetch ${endpoint}:`, error);
        throw new Error(`Fehler beim Laden von ${endpoint}: ${classifyError(error).message}`);
      }
    }
    
    // Calculate hash of complete dataset
    const currentHash = await hashData(rawData);
    
    // Early exit if no changes
    if (lastSyncHash === currentHash) {
      return {
        status: 'no-change',
        counts: {},
        message: 'Keine Aktualisierung erforderlich - Daten sind bereits aktuell.'
      };
    }
    
    // Transform data to database format
    reportProgress(onProgress, endpoints.length + 1, endpoints.length + 2, 'Transformiere Daten...');
    const dataset = transformDataset(rawData);
    
    // Import into database
    reportProgress(onProgress, endpoints.length + 2, endpoints.length + 2, 'Speichere in Datenbank...');
    await sqlite.importBvlDataset(dataset);
    
    // Update metadata
    const now = new Date().toISOString();
    await sqlite.setBvlMeta('lastSyncIso', now);
    await sqlite.setBvlMeta('lastSyncHash', currentHash);
    
    // Write sync log
    await sqlite.appendBvlSyncLog({
      synced_at: now,
      ok: 1,
      message: 'Sync erfolgreich',
      payload_hash: currentHash
    });
    
    // Calculate counts
    const counts = {};
    for (const [key, value] of Object.entries(dataset)) {
      counts[key] = Array.isArray(value) ? value.length : 0;
    }
    
    return {
      status: 'updated',
      counts,
      message: `Erfolgreich aktualisiert: ${counts.mittel || 0} Mittel, ${counts.awg || 0} Anwendungen`
    };
    
  } catch (error) {
    console.error('Sync failed:', error);
    
    // Write error to sync log
    try {
      await sqlite.appendBvlSyncLog({
        synced_at: new Date().toISOString(),
        ok: 0,
        message: error.message || 'Sync fehlgeschlagen',
        payload_hash: null
      });
    } catch (logError) {
      console.error('Failed to write error log:', logError);
    }
    
    const classified = classifyError(error);
    return {
      status: 'failed',
      counts: {},
      message: classified.message,
      errorType: classified.type
    };
  }
}

/**
 * Transform raw API data to database format
 */
function transformDataset(rawData) {
  const dataset = {
    mittel: [],
    awg: [],
    awg_kultur: [],
    awg_schadorg: [],
    awg_aufwand: [],
    awg_wartezeit: []
  };
  
  // Transform mittel
  if (rawData.mittel && Array.isArray(rawData.mittel)) {
    dataset.mittel = rawData.mittel.map(item => ({
      kennr: item.kennr || '',
      name: item.mittelname || item.name || '',
      formulierung: item.formulierung || '',
      zul_erstmalig: item.zulassung_erstmalig || item.zul_erstmalig || null,
      zul_ende: item.zulassungsende || item.zul_ende || null,
      geringes_risiko: item.geringes_risiko === 'J' ? 1 : 0,
      payload_json: JSON.stringify(item)
    }));
  }
  
  // Transform awg
  if (rawData.awg && Array.isArray(rawData.awg)) {
    dataset.awg = rawData.awg.map(item => ({
      awg_id: item.awg_id || '',
      kennr: item.kennr || '',
      status_json: JSON.stringify(item.status || {}),
      zulassungsende: item.zulassungsende || null
    }));
  }
  
  // Transform awg_kultur
  if (rawData.awg_kultur && Array.isArray(rawData.awg_kultur)) {
    dataset.awg_kultur = rawData.awg_kultur.map(item => ({
      awg_id: item.awg_id || '',
      kultur: item.kultur || '',
      ausgenommen: item.ausgenommen === 'J' ? 1 : 0
    }));
  }
  
  // Transform awg_schadorg
  if (rawData.awg_schadorg && Array.isArray(rawData.awg_schadorg)) {
    dataset.awg_schadorg = rawData.awg_schadorg.map(item => ({
      awg_id: item.awg_id || '',
      schadorg: item.schadorg || '',
      ausgenommen: item.ausgenommen === 'J' ? 1 : 0
    }));
  }
  
  // Transform awg_aufwand
  if (rawData.awg_aufwand && Array.isArray(rawData.awg_aufwand)) {
    dataset.awg_aufwand = rawData.awg_aufwand.map(item => ({
      awg_id: item.awg_id || '',
      sortier_nr: item.sortier_nr || 0,
      aufwand_bedingung: item.aufwandbedingung || item.aufwand_bedingung || '',
      mittel_menge: parseFloat(item.mittel_menge) || 0,
      mittel_einheit: item.mittel_einheit || '',
      wasser_menge: parseFloat(item.wasser_menge) || 0,
      wasser_einheit: item.wasser_einheit || '',
      payload_json: JSON.stringify(item)
    }));
  }
  
  // Transform awg_wartezeit
  if (rawData.awg_wartezeit && Array.isArray(rawData.awg_wartezeit)) {
    dataset.awg_wartezeit = rawData.awg_wartezeit.map(item => ({
      awg_id: item.awg_id || '',
      kultur: item.kultur || '',
      tage: parseInt(item.wartezeit_tage || item.tage) || 0,
      bemerkung_kode: item.bemerkung || item.bemerkung_kode || '',
      payload_json: JSON.stringify(item)
    }));
  }
  
  return dataset;
}

/**
 * Report progress to callback
 */
function reportProgress(callback, current, total, message) {
  if (typeof callback === 'function') {
    callback(current, total, message);
  }
}
