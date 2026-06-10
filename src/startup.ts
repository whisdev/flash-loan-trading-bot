import { ethers } from "ethers";
import { BOT_CONFIG, providers } from "./config";
import { fetchAllQuotes } from "./scanner";
import { logger } from "./logger";
import { isRedisEnabled, pingRedis } from "./cache/redis";

const CHAIN_CHECKS = [
  { name: "Ethereum", provider: providers.ethereum },
  { name: "Arbitrum", provider: providers.arbitrum },
  { name: "Base", provider: providers.base },
] as const;

export async function validateEnvironment(): Promise<void> {
  logger.info("Running startup checks...");

  if (isRedisEnabled()) {
    const ok = await pingRedis();
    logger[ok ? "success" : "warn"](
      ok ? "Redis cache connected" : "Redis unreachable — using in-memory cache"
    );
  } else {
    logger.info("Redis cache disabled — using in-memory cache");
  }

  await Promise.all(
    CHAIN_CHECKS.map(async ({ name, provider }) => {
      const blockNumber = await provider.getBlockNumber();
      logger.success(`${name} RPC reachable — block #${blockNumber}`);
    })
  );

  if (!BOT_CONFIG.dryRun) {
    if (!process.env.PRIVATE_KEY) {
      throw new Error("PRIVATE_KEY is required when DRY_RUN=false");
    }
    if (!process.env.ARB_CONTRACT_ADDRESS) {
      throw new Error("ARB_CONTRACT_ADDRESS is required when DRY_RUN=false");
    }
    try {
      ethers.getAddress(process.env.ARB_CONTRACT_ADDRESS);
    } catch {
      throw new Error("ARB_CONTRACT_ADDRESS is not a valid checksummed address");
    }
  }

  const amountIn = ethers.parseEther(BOT_CONFIG.amountInETH);
  const quotes = await fetchAllQuotes(amountIn, {
    ethereum: 0,
    arbitrum: 0,
    base: 0,
  });

  const labels = ["Uniswap V3", "Camelot V3", "Aerodrome"];
  quotes.forEach((quote, index) => {
    if (!quote) {
      throw new Error(`${labels[index]} quote check failed — verify RPC URLs and contract addresses`);
    }
    logger.success(
      `${labels[index]} quote OK — ${quote.amountOutFormatted.toFixed(4)} LINK for ${BOT_CONFIG.amountInETH} WETH`
    );
  });
}
