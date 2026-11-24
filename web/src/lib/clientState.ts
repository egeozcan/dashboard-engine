import { useEffect, useRef, useState } from "react";

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

/**
 * Client State keys are namespaced by Dashboard and stable `widget_id` (§8), so an
 * Adapter-supplied key such as `pr_table_sort` is never shared by unrelated widgets.
 */
export function scopedClientStateKey(
  scope: { dashboardId?: string | null; widgetId?: string | null },
  key: string
): string {
  return `${scope.dashboardId || "_"}/${scope.widgetId || "_"}/${key}`;
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
 *
 * Re-reads storage when `key` changes, follows writes from other tabs, and resets to
 * `initial` when another tab removes the key or clears storage.
 */
export function useClientState<T>(key: string, initial: T): [T, (next: T) => void] {
  const clientId = getOrCreateClientId();
  const fullKey = clientStateStorageKey(clientId, key);
  const initialRef = useRef(initial);
  initialRef.current = initial;
  const [slot, setSlot] = useState<{ key: string; value: T }>(() => ({
    key: fullKey,
    value: readClientState(key, initial),
  }));

  let current = slot;
  if (slot.key !== fullKey) {
    // Key changed (e.g. a widget's client state key after a re-render): read the new key now.
    current = { key: fullKey, value: readClientState(key, initial) };
    setSlot(current);
  }

  useEffect(() => {
    const onStorage = (ev: StorageEvent) => {
      if (ev.key !== null && ev.key !== fullKey) return;
      if (ev.key === null || ev.newValue === null) {
        // Removed in another tab, or storage cleared.
        setSlot({ key: fullKey, value: initialRef.current });
        return;
      }
      try {
        setSlot({ key: fullKey, value: JSON.parse(ev.newValue) as T });
      } catch {
        // ignore invalid JSON
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, [fullKey]);

  const update = (next: T) => {
    setSlot({ key: fullKey, value: next });
    writeClientState(key, next);
  };

  return [current.value, update];
}
