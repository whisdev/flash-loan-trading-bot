import axios from "axios";
import { MarketPrices } from "./types";
import { logger } from "./logger";

// Cache prices to avoid hammering CoinGecko rate limits
let cache: MarketPrices | null = null;
let cacheTimestamp = 0;
const CACHE_TTL_MS = 30_000; // refresh every 30 seconds

const COINGECKO_URL =
  "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,chainlink&vs_currencies=usd";

/**
 * Fetches ETH/USD and LINK/USD from the CoinGecko free API.
 * Falls back to the last cached value if the request fails.
 */
export async function fetchMarketPrices(): Promise<MarketPrices> {
  const now = Date.now();
  if (cache && now - cacheTimestamp < CACHE_TTL_MS) {
    return cache;
  }

  try {
    const response = await axios.get<{
      ethereum: { usd: number };
      chainlink: { usd: number };
    }>(COINGECKO_URL, { timeout: 8_000 });

    cache = {
      ethUSD: response.data.ethereum.usd,
      linkUSD: response.data.chainlink.usd,
    };
    cacheTimestamp = now;

    logger.info(
      `Market prices refreshed — ETH $${cache.ethUSD.toFixed(2)}  LINK $${cache.linkUSD.toFixed(4)}`
    );
    return cache;
  } catch (err) {
    if (cache) {
      logger.warn("CoinGecko request failed — using cached prices.");
      return cache;
    }
    // Hard fallback so the bot doesn't crash on first run
    logger.warn("CoinGecko unreachable on first fetch — using fallback prices.");
    return { ethUSD: 3000, linkUSD: 18 };
  }
}
