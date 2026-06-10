import { getRedisClient, isRedisEnabled } from "./redis";

const CACHE_PREFIX = process.env.REDIS_KEY_PREFIX?.trim() || "arb-scanner:";
const DEFAULT_TTL_SEC = Number(process.env.REDIS_CACHE_TTL_SEC ?? 30);

type MemoryEntry = { value: string; expiresAt: number };

const memoryStore = new Map<string, MemoryEntry>();

function fullKey(key: string): string {
  return `${CACHE_PREFIX}${key}`;
}

export async function cacheGet<T>(key: string): Promise<T | null> {
  const redisKey = fullKey(key);

  if (isRedisEnabled()) {
    try {
      const raw = await getRedisClient().get(redisKey);
      if (raw) return JSON.parse(raw) as T;
    } catch {
      // fall through to memory store
    }
  }

  const entry = memoryStore.get(redisKey);
  if (!entry || entry.expiresAt <= Date.now()) {
    memoryStore.delete(redisKey);
    return null;
  }
  return JSON.parse(entry.value) as T;
}

export async function cacheSet(
  key: string,
  value: unknown,
  ttlSec = DEFAULT_TTL_SEC,
): Promise<void> {
  const redisKey = fullKey(key);
  const raw = JSON.stringify(value);

  if (isRedisEnabled()) {
    try {
      await getRedisClient().set(redisKey, raw, "EX", ttlSec);
      return;
    } catch {
      // fall through to memory store
    }
  }

  memoryStore.set(redisKey, {
    value: raw,
    expiresAt: Date.now() + ttlSec * 1000,
  });
}
