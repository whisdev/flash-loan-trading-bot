import axios from "axios";
import { MarketPrices } from "./types";
import { logger } from "./logger";
import { cacheGet, cacheSet } from "./cache/store";

const CACHE_KEY = "market-prices";
const CACHE_TTL_SEC = 30;

type CachedPrices = { prices: MarketPrices; cachedAt: number };

// In-process fallback when Redis and the shared cache layer are unavailable
let memoryFallback: MarketPrices | null = null;

const COINGECKO_URL =
  "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,chainlink&vs_currencies=usd";

/**
 * Fetches ETH/USD and LINK/USD from the CoinGecko free API.
 * Falls back to the last cached value if the request fails.
 */
export async function fetchMarketPrices(): Promise<MarketPrices> {
  const now = Date.now();
  const cached = await cacheGet<CachedPrices>(CACHE_KEY);
  if (cached && now - cached.cachedAt < CACHE_TTL_SEC * 1000) {
    memoryFallback = cached.prices;
    return cached.prices;
  }

  try {
    const response = await axios.get<{
      ethereum: { usd: number };
      chainlink: { usd: number };
    }>(COINGECKO_URL, { timeout: 8_000 });

    const prices: MarketPrices = {
      ethUSD: response.data.ethereum.usd,
      linkUSD: response.data.chainlink.usd,
    };
    memoryFallback = prices;
    await cacheSet(CACHE_KEY, { prices, cachedAt: now }, CACHE_TTL_SEC);

    logger.info(
      `Market prices refreshed — ETH $${prices.ethUSD.toFixed(2)}  LINK $${prices.linkUSD.toFixed(4)}`
    );
    return prices;
  } catch (err) {
    if (memoryFallback) {
      logger.warn("CoinGecko request failed — using cached prices.");
      return memoryFallback;
    }
    // Hard fallback so the bot doesn't crash on first run
    logger.warn("CoinGecko unreachable on first fetch — using fallback prices.");
    return { ethUSD: 3000, linkUSD: 18 };
  }
}
