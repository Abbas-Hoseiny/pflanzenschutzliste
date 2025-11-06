const FILE_OPTIONS = {
  description: 'JSON-Datei',
  accept: { 'application/json': ['.json'] }
};

let fileHandle = null;

async function writeFile(handle, data) {
  const writable = await handle.createWritable();
  await writable.write(JSON.stringify(data, null, 2));
  await writable.close();
}

async function readFile(handle) {
  const file = await handle.getFile();
  return file.text();
}

export function isSupported() {
  return typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function';
}

export async function create(initialData, suggestedName = 'database.json') {
  if (!isSupported()) {
    throw new Error('File System Access API not available');
  }
  fileHandle = await window.showSaveFilePicker({
    suggestedName,
    types: [FILE_OPTIONS]
  });
  await writeFile(fileHandle, initialData);
  return { data: initialData, context: { fileHandle } };
}

export async function open() {
  if (!isSupported()) {
    throw new Error('File System Access API not available');
  }
  const [handle] = await window.showOpenFilePicker({ types: [FILE_OPTIONS] });
  fileHandle = handle;
  const text = await readFile(fileHandle);
  const data = JSON.parse(text);
  return { data, context: { fileHandle } };
}

export async function save(data) {
  if (!fileHandle) {
    throw new Error('Kein Dateihandle vorhanden');
  }
  await writeFile(fileHandle, data);
  return { context: { fileHandle } };
}

export function getContext() {
  return { fileHandle };
}

export function reset() {
  fileHandle = null;
}
