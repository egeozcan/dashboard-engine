import { useEffect, useState } from "react";

const CLIENT_ID_KEY = "dash_client_id";

export function getOrCreateClientId(): string {
  try {
    let id = localStorage.getItem(CLIENT_ID_KEY);
    if (!id) {
      id = `cli_${crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2)}`;
      localStorage.setItem(CLIENT_ID_KEY, id);
    }
    return id;
  } catch {
    return "cli_ephemeral";
  }
}

export function clientStateStorageKey(clientId: string, key: string): string {
  return `dash_client_state:${clientId}:${key}`;
}

export function readClientState<T>(key: string, fallback: T): T {
  const clientId = getOrCreateClientId();
  const fullKey = clientStateStorageKey(clientId, key);
  try {
    const raw = localStorage.getItem(fullKey);
    if (raw === null) return fallback;
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

export function writeClientState<T>(key: string, value: T): void {
  const clientId = getOrCreateClientId();
  const fullKey = clientStateStorageKey(clientId, key);
  try {
    localStorage.setItem(fullKey, JSON.stringify(value));
  } catch {
    // ignore quota errors
  }
}

/**
 * Hook for browser-profile Client State shared across all tabs of one Client (§8, AC #9).
 */
export function useClientState<T>(key: string, initial: T): [T, (next: T) => void] {
  const clientId = getOrCreateClientId();
  const fullKey = clientStateStorageKey(clientId, key);
  const [state, setState] = useState<T>(() => readClientState(key, initial));

  useEffect(() => {
    const onStorage = (ev: StorageEvent) => {
      if (ev.key === fullKey && ev.newValue !== null) {
        try {
          setState(JSON.parse(ev.newValue) as T);
        } catch {
          // ignore invalid JSON
        }
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [fullKey]);

  const update = (next: T) => {
    setState(next);
    writeClientState(key, next);
  };

  return [state, update];
}
