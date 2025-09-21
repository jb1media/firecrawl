import IORedis from "ioredis";
import { logger } from "../lib/logger";

let redisRateLimitClient: IORedis | null = null;

if (process.env.REDIS_RATE_LIMIT_URL) {
  redisRateLimitClient = new IORedis(process.env.REDIS_RATE_LIMIT_URL);

  // Listen to 'error' events
  redisRateLimitClient.on("error", error => {
    try {
      if (error.message === "ECONNRESET") {
        logger.error("Connection to Redis Session Rate Limit Store timed out.");
      } else if (error.message === "ECONNREFUSED") {
        logger.error("Connection to Redis Session Rate Limit Store refused!");
      } else logger.error(error);
    } catch (error) {}
  });

  redisRateLimitClient.on("reconnecting", () => {
    try {
      if (redisRateLimitClient?.status === "reconnecting") {
        logger.info("Reconnecting to Redis Session Rate Limit Store...");
      } else logger.error("Error reconnecting to Redis Session Rate Limit Store.");
    } catch (error) {}
  });

  redisRateLimitClient.on("connect", err => {
    try {
      if (!err) logger.info("Connected to Redis Session Rate Limit Store!");
    } catch (error) {}
  });
} else {
  logger.warn("⚠️ Redis not configured. Running without rate limiting.");
}

/**
 * Set a value in Redis with an optional expiration time.
 */
const setValue = async (key: string, value: string, expire?: number, nx = false) => {
  if (!redisRateLimitClient) return; // skip if no Redis
  if (expire && !nx) {
    await redisRateLimitClient.set(key, value, "EX", expire);
  } else {
    await redisRateLimitClient.set(key, value);
  }
  if (expire && nx) {
    await redisRateLimitClient.expire(key, expire, "NX");
  }
};

/**
 * Get a value from Redis.
 */
const getValue = async (key: string): Promise<string | null> => {
  if (!redisRateLimitClient) return null;
  return await redisRateLimitClient.get(key);
};

/**
 * Delete a key from Redis.
 */
const deleteKey = async (key: string) => {
  if (!redisRateLimitClient) return;
  await redisRateLimitClient.del(key);
};

export { setValue, getValue, deleteKey };

// Optional eviction connection
export const redisEvictConnection = process.env.REDIS_EVICT_URL
  ? new IORedis(process.env.REDIS_EVICT_URL)
  : null;
