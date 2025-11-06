const subscribers = new Map();
let counter = 0;

export function subscribe(eventName, handler) {
  if (!subscribers.has(eventName)) {
    subscribers.set(eventName, new Map());
  }
  const token = `h_${eventName}_${counter++}`;
  subscribers.get(eventName).set(token, handler);
  return () => {
    const handlers = subscribers.get(eventName);
    if (handlers) {
      handlers.delete(token);
      if (!handlers.size) {
        subscribers.delete(eventName);
      }
    }
  };
}

export function emit(eventName, payload) {
  const handlers = subscribers.get(eventName);
  if (!handlers) {
    return;
  }
  for (const handler of handlers.values()) {
    try {
      handler(payload);
    } catch (err) {
      console.error(`eventBus handler error for ${eventName}`, err);
    }
  }
}

export function clearEventBus() {
  subscribers.clear();
  counter = 0;
}
