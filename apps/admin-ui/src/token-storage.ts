// Admin-token persistence. FUR-17 deliberately kept the token in a signal
// only; that decision was explicitly overridden by user request — the token
// must survive page refreshes and vite dev reloads. Storage access lives in
// this tiny module so every reader agrees on the key and tests can exercise
// the logic with a stubbed `globalThis.localStorage` (the vitest env is node,
// which has no Web Storage).

const KEY = "maestro-admin-token";

export const loadToken = (): string | null => {
  try {
    return globalThis.localStorage?.getItem(KEY) ?? null;
  } catch {
    return null;
  }
};

export const saveToken = (token: string): void => {
  try {
    globalThis.localStorage?.setItem(KEY, token);
  } catch {
    // Storage unusable (privacy mode, quota): degrade to in-memory-only.
  }
};

export const clearToken = (): void => {
  try {
    globalThis.localStorage?.removeItem(KEY);
  } catch {
    // Nothing to clear if storage is unusable.
  }
};
