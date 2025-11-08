/**
 * BVL API Client
 * Handles fetching data from BVL OpenAPI endpoints with pagination and error handling
 */

const BVL_API_BASE = 'https://psm-api.bvl.bund.de/ords/psm/api-v1';
const REQUEST_TIMEOUT = 30000; // 30 seconds

/**
 * Fetch a complete collection from a BVL endpoint with automatic pagination
 * @param {string} endpoint - The endpoint name (e.g., 'mittel', 'awg')
 * @param {Object} options - Fetch options
 * @param {Object} options.query - Query parameters
 * @param {AbortSignal} options.signal - AbortController signal for timeout
 * @returns {Promise<Array>} Complete array of items from all pages
 */
export async function fetchCollection(endpoint, options = {}) {
  const { query = {}, signal } = options;
  const items = [];
  let nextUrl = buildUrl(endpoint, query);
  
  while (nextUrl) {
    const response = await fetchWithTimeout(nextUrl, signal);
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText || 'API request failed'}`);
    }
    
    let data;
    try {
      data = await response.json();
    } catch (err) {
      throw new Error(`JSON parse error: ${err.message}`);
    }
    
    // Handle Oracle REST API response format
    if (data.items && Array.isArray(data.items)) {
      items.push(...data.items);
    } else if (Array.isArray(data)) {
      items.push(...data);
    } else {
      throw new Error('Unexpected API response format');
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
 * @param {string} endpoint - Endpoint name
 * @param {Object} query - Query parameters
 * @returns {string} Complete URL
 */
function buildUrl(endpoint, query = {}) {
  const url = new URL(`${BVL_API_BASE}/${endpoint}`);
  
  for (const [key, value] of Object.entries(query)) {
    if (value !== null && value !== undefined) {
      url.searchParams.append(key, value);
    }
  }
  
  return url.toString();
}

/**
 * Fetch with timeout and error handling
 * @param {string} url - URL to fetch
 * @param {AbortSignal} externalSignal - External abort signal
 * @returns {Promise<Response>}
 */
async function fetchWithTimeout(url, externalSignal) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
  
  try {
    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json'
      },
      signal: externalSignal || controller.signal,
      redirect: 'follow'
    });
    
    clearTimeout(timeoutId);
    return response;
  } catch (err) {
    clearTimeout(timeoutId);
    
    if (err.name === 'AbortError') {
      throw new Error('Request timeout');
    }
    
    throw new Error(`Network error: ${err.message}`);
  }
}

/**
 * Compute SHA-256 hash of data for diff detection
 * @param {any} data - Data to hash
 * @returns {Promise<string>} Hex-encoded hash
 */
export async function hashData(data) {
  const text = JSON.stringify(data);
  const encoder = new TextEncoder();
  const dataBuffer = encoder.encode(text);
  const hashBuffer = await crypto.subtle.digest('SHA-256', dataBuffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}
