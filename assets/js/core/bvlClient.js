/**
 * BVL API Client
 * Handles API calls to BVL OpenAPI endpoints with pagination and error handling
 */

const BVL_API_BASE = 'https://psm-api.bvl.bund.de/ords/psm/api-v1';
const DEFAULT_TIMEOUT = 30000; // 30 seconds

/**
 * Fetch a complete collection from BVL API with automatic pagination
 * @param {string} endpoint - Endpoint name (e.g., 'mittel', 'awg')
 * @param {Object} options - Options object
 * @param {Object} options.query - Query parameters
 * @param {AbortSignal} options.signal - Abort signal for cancellation
 * @returns {Promise<Array>} Complete collection of items
 */
export async function fetchCollection(endpoint, options = {}) {
  const { query = {}, signal } = options;
  const items = [];
  let nextUrl = buildUrl(endpoint, query);
  
  while (nextUrl) {
    const response = await fetchWithTimeout(nextUrl, {
      headers: {
        'Accept': 'application/json'
      },
      signal,
      redirect: 'follow'
    });
    
    if (!response.ok) {
      throw new Error(`BVL API error: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    
    // Handle response structure
    if (data.items && Array.isArray(data.items)) {
      items.push(...data.items);
    } else if (Array.isArray(data)) {
      items.push(...data);
    }
    
    // Check for next page
    nextUrl = null;
    if (data.links && Array.isArray(data.links)) {
      const nextLink = data.links.find(link => link.rel === 'next');
      if (nextLink && nextLink.href) {
        nextUrl = nextLink.href;
      }
    }
  }
  
  return items;
}

/**
 * Build URL with query parameters
 */
function buildUrl(endpoint, query = {}) {
  const url = new URL(`${BVL_API_BASE}/${endpoint}`);
  for (const [key, value] of Object.entries(query)) {
    if (value !== null && value !== undefined) {
      url.searchParams.append(key, String(value));
    }
  }
  return url.toString();
}

/**
 * Fetch with timeout
 */
async function fetchWithTimeout(url, options = {}) {
  const { signal, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT);
  
  try {
    const combinedSignal = signal ? combineSignals([signal, controller.signal]) : controller.signal;
    const response = await fetch(url, {
      ...fetchOptions,
      signal: combinedSignal
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === 'AbortError') {
      throw new Error('Request timeout or cancelled');
    }
    throw error;
  }
}

/**
 * Combine multiple abort signals
 */
function combineSignals(signals) {
  const controller = new AbortController();
  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort();
      break;
    }
    signal.addEventListener('abort', () => controller.abort(), { once: true });
  }
  return controller.signal;
}

/**
 * Hash data payload for diff detection
 * @param {any} payload - Data to hash
 * @returns {Promise<string>} Hex string of SHA-256 hash
 */
export async function hashData(payload) {
  const text = JSON.stringify(payload);
  const encoder = new TextEncoder();
  const data = encoder.encode(text);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

/**
 * Classify error type for user-friendly messages
 */
export function classifyError(error) {
  if (error.message.includes('timeout') || error.message.includes('cancelled')) {
    return {
      type: 'timeout',
      message: 'Die Anfrage hat zu lange gedauert oder wurde abgebrochen.'
    };
  }
  
  if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
    return {
      type: 'network',
      message: 'Netzwerkfehler: Bitte überprüfen Sie Ihre Internetverbindung.'
    };
  }
  
  if (error.message.includes('404')) {
    return {
      type: 'not_found',
      message: 'Die angeforderte Ressource wurde nicht gefunden.'
    };
  }
  
  if (error.message.includes('5')) {
    return {
      type: 'server',
      message: 'Der BVL-Server hat einen Fehler zurückgegeben.'
    };
  }
  
  return {
    type: 'unknown',
    message: error.message || 'Ein unbekannter Fehler ist aufgetreten.'
  };
}
