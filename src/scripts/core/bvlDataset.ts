// @ts-nocheck
/**
 * BVL Dataset Module
 * Handles manifest-based data fetching from pflanzenschutzliste-data repository
 */

const DEFAULT_MANIFEST_URL =
  "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/manifest.json";
const MANIFEST_STORAGE_KEY = "bvlManifestUrl";

export class ManifestError extends Error {
  constructor(message) {
    super(message);
    this.name = "ManifestError";
  }
}

export class DownloadError extends Error {
  constructor(message, url) {
    super(message);
    this.name = "DownloadError";
    this.url = url;
  }
}

let fflateLoader = null;

async function loadFflate() {
  if (typeof window !== "undefined" && window.fflate) {
    return window.fflate;
  }

  if (!fflateLoader) {
    fflateLoader = (async () => {
      try {
        return await import("fflate");
      } catch (error) {
        return await import(
          "https://cdn.jsdelivr.net/npm/fflate@0.8.1/esm/browser.js"
        );
      }
    })();
  }

  return fflateLoader;
}

/**
 * Get the manifest URL (from localStorage or default)
 */
export function getManifestUrl() {
  try {
    const stored = localStorage.getItem(MANIFEST_STORAGE_KEY);
    if (stored && stored.trim()) {
      return stored.trim();
    }
  } catch (e) {
    console.warn("Could not access localStorage for manifest URL", e);
  }
  return DEFAULT_MANIFEST_URL;
}

/**
 * Set a custom manifest URL in localStorage
 */
export function setManifestUrl(url) {
  try {
    if (url) {
      localStorage.setItem(MANIFEST_STORAGE_KEY, url);
    } else {
      localStorage.removeItem(MANIFEST_STORAGE_KEY);
    }
  } catch (e) {
    console.warn("Could not save manifest URL to localStorage", e);
  }
}

/**
 * Fetch and validate the manifest
 */
export async function fetchManifest(options = {}) {
  const { timeout = 30000 } = options;
  const manifestUrl = getManifestUrl();

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(manifestUrl, {
      signal: controller.signal,
      headers: {
        Accept: "application/json",
      },
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new ManifestError(
        `Failed to fetch manifest: HTTP ${response.status}`
      );
    }

    const manifest = await response.json();

    // Basic validation
    if (!manifest.version) {
      throw new ManifestError("Manifest missing required field: version");
    }
    if (!manifest.files || !Array.isArray(manifest.files)) {
      throw new ManifestError("Manifest missing required field: files");
    }

    // Attach helper metadata for downstream consumers
    try {
      manifest.manifestUrl = manifestUrl;
      const baseCandidate = manifest.baseUrl || manifest.base_url || "./";
      manifest.baseUrlResolved = new URL(baseCandidate, manifestUrl).toString();
    } catch (resolveError) {
      console.warn(
        "Failed to resolve manifest base URL, using default",
        resolveError
      );
      manifest.baseUrlResolved =
        "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/";
    }

    return manifest;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new ManifestError(`Manifest fetch timeout after ${timeout}ms`);
    }
    if (error instanceof ManifestError) {
      throw error;
    }
    throw new ManifestError(`Failed to fetch manifest: ${error.message}`);
  }
}

/**
 * Check if DecompressionStream with brotli is supported
 */
function supportsStreamDecompression(kind) {
  try {
    if (typeof DecompressionStream === "undefined") {
      return false;
    }
    const stream = new DecompressionStream(kind);
    return !!stream;
  } catch (e) {
    return false;
  }
}

/**
 * Select the best file format from manifest
 */
export function selectBestFile(manifest) {
  const files = manifest.files;

  // Prefer .sqlite.br if brotli is supported
  if (supportsStreamDecompression("br")) {
    const brFile = files.find((f) => f.path.endsWith(".sqlite.br"));
    if (brFile) {
      return { file: brFile, format: "brotli" };
    }
  }

  // Prefer gzip next (fallback to JS decompressor if stream API missing)
  const gzFile = files.find((f) => f.path.endsWith(".sqlite.gz"));
  if (gzFile) {
    return { file: gzFile, format: "gzip" };
  }

  // Fallback to plain .sqlite
  const sqliteFile = files.find(
    (f) => f.path.endsWith(".sqlite") && !f.path.includes(".")
  );
  if (sqliteFile) {
    return { file: sqliteFile, format: "plain" };
  }

  // Fallback to .sqlite.zip
  const zipFile = files.find((f) => f.path.endsWith(".sqlite.zip"));
  if (zipFile) {
    return { file: zipFile, format: "zip" };
  }

  throw new ManifestError("No suitable database file found in manifest");
}

/**
 * Download a file with progress tracking
 */
export async function downloadFile(url, options = {}) {
  const { onProgress = null, timeout = 120000 } = options;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new DownloadError(`Download failed: HTTP ${response.status}`, url);
    }

    const contentLength = parseInt(
      response.headers.get("content-length") || "0",
      10
    );
    const reader = response.body.getReader();
    const chunks = [];
    let receivedLength = 0;

    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        break;
      }

      chunks.push(value);
      receivedLength += value.length;

      if (onProgress && contentLength > 0) {
        onProgress({
          loaded: receivedLength,
          total: contentLength,
          percent: Math.round((receivedLength / contentLength) * 100),
        });
      }
    }

    // Concatenate chunks into single Uint8Array
    const result = new Uint8Array(receivedLength);
    let position = 0;
    for (const chunk of chunks) {
      result.set(chunk, position);
      position += chunk.length;
    }

    return result;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new DownloadError(`Download timeout after ${timeout}ms`, url);
    }
    if (error instanceof DownloadError) {
      throw error;
    }
    throw new DownloadError(`Download failed: ${error.message}`, url);
  }
}

/**
 * Decompress brotli-compressed data
 */
async function decompressBrotli(compressedData) {
  try {
    const ds = new DecompressionStream("br");
    const writer = ds.writable.getWriter();
    const reader = ds.readable.getReader();

    // Write compressed data
    writer.write(compressedData);
    writer.close();

    // Read decompressed data
    const chunks = [];
    let totalLength = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      totalLength += value.length;
    }

    // Concatenate chunks
    const result = new Uint8Array(totalLength);
    let position = 0;
    for (const chunk of chunks) {
      result.set(chunk, position);
      position += chunk.length;
    }

    return result;
  } catch (error) {
    throw new Error(`Brotli decompression failed: ${error.message}`);
  }
}

async function decompressGzip(compressedData) {
  try {
    if (supportsStreamDecompression("gzip")) {
      const ds = new DecompressionStream("gzip");
      const writer = ds.writable.getWriter();
      const reader = ds.readable.getReader();

      writer.write(compressedData);
      writer.close();

      const chunks = [];
      let totalLength = 0;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        totalLength += value.length;
      }

      const result = new Uint8Array(totalLength);
      let position = 0;
      for (const chunk of chunks) {
        result.set(chunk, position);
        position += chunk.length;
      }

      return result;
    }

    const fflate = await loadFflate();
    const gunzipSync = fflate.gunzipSync || null;

    if (gunzipSync) {
      return gunzipSync(compressedData);
    }

    if (typeof fflate.gunzip === "function") {
      return await new Promise((resolve, reject) => {
        fflate.gunzip(compressedData, (err, result) => {
          if (err) {
            reject(err);
          } else {
            resolve(result);
          }
        });
      });
    }

    throw new Error("No gzip decompressor available");
  } catch (error) {
    throw new Error(`Gzip decompression failed: ${error.message}`);
  }
}

/**
 * Decompress ZIP-compressed data (assumes single file in archive)
 */
async function decompressZip(compressedData) {
  // For ZIP we'll need JSZip or similar library
  // For now, throw error as this needs external dependency
  throw new Error(
    "ZIP decompression not yet implemented. Please use .sqlite or .sqlite.br format."
  );
}

/**
 * Download and prepare the SQLite database
 */
export async function downloadDatabase(manifest, options = {}) {
  const { onProgress = null } = options;
  let baseUrl =
    manifest.baseUrlResolved ||
    manifest.baseUrl ||
    manifest.base_url ||
    "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/";

  try {
    baseUrl = new URL(baseUrl, manifest.manifestUrl || baseUrl).toString();
  } catch (resolveError) {
    console.warn(
      "Failed to normalize base URL, falling back to default",
      resolveError
    );
    baseUrl =
      "https://abbas-hoseiny.github.io/pflanzenschutzliste-data/latest/";
  }

  // Select best file format
  const { file, format } = selectBestFile(manifest);
  const fileUrl = new URL(file.path, baseUrl).toString();

  if (onProgress) {
    onProgress({
      step: "download",
      message: `Lade Datenbank (${format})...`,
      percent: 0,
    });
  }

  // Download the file
  const compressed = await downloadFile(fileUrl, {
    onProgress: (progress) => {
      if (onProgress) {
        onProgress({
          step: "download",
          message: `Lade Datenbank: ${progress.percent}%`,
          percent: Math.round(progress.percent * 0.7), // Reserve 30% for decompression
          ...progress,
        });
      }
    },
  });

  // Decompress if necessary
  let sqliteData;
  if (format === "brotli") {
    if (onProgress) {
      onProgress({
        step: "decompress",
        message: "Entpacke Datenbank...",
        percent: 70,
      });
    }
    sqliteData = await decompressBrotli(compressed);
  } else if (format === "gzip") {
    if (onProgress) {
      onProgress({
        step: "decompress",
        message: "Entpacke Datenbank...",
        percent: 70,
      });
    }
    sqliteData = await decompressGzip(compressed);
  } else if (format === "zip") {
    if (onProgress) {
      onProgress({
        step: "decompress",
        message: "Entpacke Datenbank...",
        percent: 70,
      });
    }
    sqliteData = await decompressZip(compressed);
  } else {
    sqliteData = compressed;
  }

  if (onProgress) {
    onProgress({
      step: "complete",
      message: "Datenbank heruntergeladen",
      percent: 100,
    });
  }

  return {
    data: sqliteData,
    file,
    format,
    manifest,
  };
}

/**
 * Check if a newer version is available without downloading
 * @param {string|null} currentHash - Current data hash/version
 * @returns {Promise<{available: boolean, manifest: object|null}>}
 */
export async function checkForUpdates(currentHash) {
  try {
    const manifest = await fetchManifest();
    const manifestHash = manifest.hash || manifest.version;

    return {
      available: currentHash !== manifestHash,
      manifest: manifest,
      newVersion: manifestHash,
    };
  } catch (error) {
    console.warn("Failed to check for updates:", error);
    return {
      available: false,
      manifest: null,
      newVersion: null,
      error: error.message,
    };
  }
}
