/**
 * BVL Data Synchronization Orchestrator
 * Manages the process of fetching, diffing, and importing BVL data
 */

import { fetchCollection, hashData } from './bvlClient.js';

// Default endpoints to sync (max 6 as per task)
const DEFAULT_ENDPOINTS = [
  'mittel',
  'awg',
  'awg_kultur',
  'awg_schadorg',
  'awg_aufwand',
  'awg_wartezeit'
];

/**
 * Sync BVL data from API to local SQLite database
 * @param {Object} options - Sync options
 * @param {Array<string>} options.endpoints - Endpoints to sync (defaults to DEFAULT_ENDPOINTS)
 * @param {Function} options.onProgress - Progress callback (endpoint, current, total)
 * @param {Object} options.sqliteDriver - SQLite driver instance
 * @returns {Promise<Object>} Sync result { status, counts?, message?, error? }
 */
export async function syncBvlData(options = {}) {
  const {
    endpoints = DEFAULT_ENDPOINTS,
    onProgress = null,
    sqliteDriver
  } = options;
  
  if (!sqliteDriver) {
    throw new Error('sqliteDriver is required');
  }
  
  try {
    // Read last sync hash from metadata
    const lastHash = await sqliteDriver.getBvlMeta('lastSyncHash');
    
    // Fetch data from all endpoints
    const rawData = {};
    const controller = new AbortController();
    
    for (let i = 0; i < endpoints.length; i++) {
      const endpoint = endpoints[i];
      
      if (onProgress) {
        onProgress(endpoint, i + 1, endpoints.length);
      }
      
      try {
        rawData[endpoint] = await fetchCollection(endpoint, {
          signal: controller.signal
        });
      } catch (err) {
        // If one endpoint fails, abort remaining and throw
        controller.abort();
        throw new Error(`Failed to fetch ${endpoint}: ${err.message}`);
      }
    }
    
    // Compute hash of fetched data
    const currentHash = await hashData(rawData);
    
    // Check if data has changed
    if (lastHash === currentHash) {
      // No changes - just log and return
      await sqliteDriver.appendBvlSyncLog({
        synced_at: new Date().toISOString(),
        ok: 1,
        message: 'no-change',
        payload_hash: currentHash
      });
      
      return {
        status: 'no-change',
        message: 'Keine Aktualisierung notwendig. Daten sind bereits aktuell.'
      };
    }
    
    // Transform raw data to database format
    const transformedData = transformBvlData(rawData);
    
    // Import into database
    await sqliteDriver.importBvlDataset(transformedData);
    
    // Update metadata
    await sqliteDriver.setBvlMeta('lastSyncHash', currentHash);
    await sqliteDriver.setBvlMeta('lastSyncIso', new Date().toISOString());
    
    // Log successful sync
    await sqliteDriver.appendBvlSyncLog({
      synced_at: new Date().toISOString(),
      ok: 1,
      message: 'updated',
      payload_hash: currentHash
    });
    
    // Count records
    const counts = {};
    for (const [key, value] of Object.entries(transformedData)) {
      counts[key] = Array.isArray(value) ? value.length : 0;
    }
    
    return {
      status: 'updated',
      counts,
      message: `Daten erfolgreich aktualisiert. ${counts.mittel || 0} Mittel, ${counts.awg || 0} Anwendungen geladen.`
    };
    
  } catch (err) {
    // Log failed sync
    try {
      await sqliteDriver.appendBvlSyncLog({
        synced_at: new Date().toISOString(),
        ok: 0,
        message: err.message || 'unknown error',
        payload_hash: null
      });
    } catch (logErr) {
      console.error('Failed to log sync error:', logErr);
    }
    
    return {
      status: 'failed',
      error: err.message || 'Unbekannter Fehler'
    };
  }
}

/**
 * Transform raw API data to database schema format
 * @param {Object} rawData - Raw data from API endpoints
 * @returns {Object} Transformed data ready for import
 */
function transformBvlData(rawData) {
  const transformed = {
    mittel: [],
    awg: [],
    awg_kultur: [],
    awg_schadorg: [],
    awg_aufwand: [],
    awg_wartezeit: []
  };
  
  // Transform mittel (Pflanzenschutzmittel)
  if (rawData.mittel && Array.isArray(rawData.mittel)) {
    transformed.mittel = rawData.mittel.map(item => ({
      kennr: item.kennr || '',
      name: item.mittelname || '',
      formulierung: item.formulierung || '',
      zul_erstmalig: item.zulassung_erstmalig || null,
      zul_ende: item.zulassungsende || null,
      geringes_risiko: item.geringes_risiko === 'J' ? 1 : 0,
      payload_json: JSON.stringify(item)
    }));
  }
  
  // Transform awg (Anwendungen)
  if (rawData.awg && Array.isArray(rawData.awg)) {
    transformed.awg = rawData.awg.map(item => ({
      awg_id: item.awg_id || '',
      kennr: item.kennr || '',
      status_json: JSON.stringify({
        status: item.status || '',
        wachsstadium_von: item.wachsstadium_von || '',
        wachsstadium_bis: item.wachsstadium_bis || ''
      }),
      zulassungsende: item.zulassungsende || null
    }));
  }
  
  // Transform awg_kultur (Kulturen)
  if (rawData.awg_kultur && Array.isArray(rawData.awg_kultur)) {
    transformed.awg_kultur = rawData.awg_kultur.map(item => ({
      awg_id: item.awg_id || '',
      kultur: item.kultur || '',
      ausgenommen: item.ausgenommen === 'J' ? 1 : 0,
      sortier_nr: item.sortier_nr || 0
    }));
  }
  
  // Transform awg_schadorg (Schadorganismen)
  if (rawData.awg_schadorg && Array.isArray(rawData.awg_schadorg)) {
    transformed.awg_schadorg = rawData.awg_schadorg.map(item => ({
      awg_id: item.awg_id || '',
      schadorg: item.schadorg || '',
      ausgenommen: item.ausgenommen === 'J' ? 1 : 0,
      sortier_nr: item.sortier_nr || 0
    }));
  }
  
  // Transform awg_aufwand (Aufwandmengen)
  if (rawData.awg_aufwand && Array.isArray(rawData.awg_aufwand)) {
    transformed.awg_aufwand = rawData.awg_aufwand.map(item => ({
      awg_id: item.awg_id || '',
      aufwand_bedingung: item.aufwandbedingung || '',
      sortier_nr: item.sortier_nr || 0,
      mittel_menge: parseFloat(item.mittel_menge) || 0,
      mittel_einheit: item.mittel_einheit || '',
      wasser_menge: parseFloat(item.wasser_menge) || 0,
      wasser_einheit: item.wasser_einheit || '',
      payload_json: JSON.stringify(item)
    }));
  }
  
  // Transform awg_wartezeit (Wartezeiten)
  if (rawData.awg_wartezeit && Array.isArray(rawData.awg_wartezeit)) {
    transformed.awg_wartezeit = rawData.awg_wartezeit.map(item => ({
      awg_wartezeit_nr: parseInt(item.awg_wartezeit_nr) || 0,
      awg_id: item.awg_id || '',
      kultur: item.kultur || '',
      sortier_nr: item.sortier_nr || 0,
      tage: parseInt(item.tage) || 0,
      bemerkung_kode: item.bemerkung || '',
      anwendungsbereich: item.anwendungsbereich || '',
      erlaeuterung: item.erlaeuterung || '',
      payload_json: JSON.stringify(item)
    }));
  }
  
  return transformed;
}
