import { ethers } from "ethers";
import {
  providers,
  TOKENS,
  DEX,
  UNISWAP_QUOTER_V2_ABI,
  CAMELOT_QUOTER_ABI,
  AERODROME_ROUTER_ABI,
  CHAINS,
  BOT_CONFIG,
} from "./config";
import { PriceQuote } from "./types";
import { logger } from "./logger";

function readAmountOut(result: unknown): bigint {
  if (typeof result === "bigint") return result;
  if (Array.isArray(result) || (typeof result === "object" && result !== null)) {
    const value = (result as { amountOut?: bigint; 0?: bigint }).amountOut ??
      (result as { 0?: bigint })[0];
    if (typeof value === "bigint") return value;
  }
  throw new Error("Quote response did not include amountOut");
}

async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  retries = BOT_CONFIG.maxRetries,
  delayMs = BOT_CONFIG.retryDelayMs
): Promise<T | null> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt === retries) {
        logger.error(`${label} failed after ${retries} attempts`, err);
        return null;
      }
      await new Promise((r) => setTimeout(r, delayMs * attempt));
    }
  }
  return null;
}

export async function getUniswapQuote(
  amountIn: bigint,
  gasEstimateUSD: number
): Promise<PriceQuote | null> {
  const quoter = new ethers.Contract(
    DEX.uniswapV3.quoter,
    UNISWAP_QUOTER_V2_ABI,
    providers.ethereum
  );

  const result = await withRetry(async () => {
    const params = {
      tokenIn: TOKENS.ethereum.WETH,
      tokenOut: TOKENS.ethereum.LINK,
      amountIn,
      fee: DEX.uniswapV3.fee,
      sqrtPriceLimitX96: 0n,
    };
    return quoter.quoteExactInputSingle.staticCall(params);
  }, "Uniswap V3 quote");

  if (!result) return null;

  const amountOut = readAmountOut(result);
  if (amountOut === 0n) return null;

  return {
    dex: "Uniswap V3",
    chain: "Ethereum",
    chainId: CHAINS.ETHEREUM,
    amountIn,
    amountOut,
    amountOutFormatted: parseFloat(ethers.formatUnits(amountOut, 18)),
    gasEstimateUSD,
    timestamp: Date.now(),
  };
}

export async function getCamelotQuote(
  amountIn: bigint,
  gasEstimateUSD: number
): Promise<PriceQuote | null> {
  const quoter = new ethers.Contract(
    DEX.camelotV3.quoter,
    CAMELOT_QUOTER_ABI,
    providers.arbitrum
  );

  const path = ethers.solidityPacked(
    ["address", "address"],
    [TOKENS.arbitrum.WETH, TOKENS.arbitrum.LINK]
  );

  const result = await withRetry(async () => {
    return quoter.quoteExactInput.staticCall(path, amountIn);
  }, "Camelot V3 quote");

  if (!result) return null;

  const amountOut = readAmountOut(result);
  if (amountOut === 0n) return null;

  return {
    dex: "Camelot V3",
    chain: "Arbitrum",
    chainId: CHAINS.ARBITRUM,
    amountIn,
    amountOut,
    amountOutFormatted: parseFloat(ethers.formatUnits(amountOut, 18)),
    gasEstimateUSD,
    timestamp: Date.now(),
  };
}

export async function getAerodromeQuote(
  amountIn: bigint,
  gasEstimateUSD: number
): Promise<PriceQuote | null> {
  const router = new ethers.Contract(
    DEX.aerodrome.router,
    AERODROME_ROUTER_ABI,
    providers.base
  );

  const result = await withRetry(async () => {
    const routes = [
      {
        from: TOKENS.base.WETH,
        to: TOKENS.base.LINK,
        stable: false,
        factory: DEX.aerodrome.volatileFactory,
      },
    ];
    return router.getAmountsOut(amountIn, routes);
  }, "Aerodrome quote");

  if (!result || !Array.isArray(result) || result.length < 2) return null;

  const amountOut = result[result.length - 1] as bigint;
  if (amountOut === 0n) return null;

  return {
    dex: "Aerodrome",
    chain: "Base",
    chainId: CHAINS.BASE,
    amountIn,
    amountOut,
    amountOutFormatted: parseFloat(ethers.formatUnits(amountOut, 18)),
    gasEstimateUSD,
    timestamp: Date.now(),
  };
}

export async function fetchAllQuotes(amountIn: bigint, gasCosts: {
  ethereum: number;
  arbitrum: number;
  base: number;
}): Promise<(PriceQuote | null)[]> {
  return Promise.all([
    getUniswapQuote(amountIn, gasCosts.ethereum),
    getCamelotQuote(amountIn, gasCosts.arbitrum),
    getAerodromeQuote(amountIn, gasCosts.base),
  ]);
}
