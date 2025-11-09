/**
 * Core utility functions shared across the application.
 * These utilities are used for HTML escaping, number formatting,
 * and asynchronous frame scheduling.
 */

/**
 * Escapes HTML special characters to prevent XSS vulnerabilities.
 * @param {*} value - The value to escape
 * @returns {string} - HTML-safe string
 */
export function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/**
 * Formats a number with fixed decimal places.
 * Returns a fallback value if the input is not a valid number.
 * @param {*} value - The value to format
 * @param {number} fractionDigits - Number of decimal places (default: 2)
 * @param {string} fallback - Fallback value for invalid numbers (default: '–')
 * @returns {string} - Formatted number string
 */
export function formatNumber(value, fractionDigits = 2, fallback = '–') {
  const num = Number.parseFloat(value);
  if (Number.isNaN(num)) {
    return fallback;
  }
  return num.toFixed(fractionDigits);
}

/**
 * Returns a Promise that resolves on the next animation frame.
 * Useful for yielding control to the browser and preventing UI blocking.
 * @returns {Promise<number>} - Promise that resolves with the frame timestamp
 */
export function nextFrame() {
  return new Promise(resolve => requestAnimationFrame(resolve));
}
