import { ethers } from "ethers";
import {
  providers,
  TOKENS,
  DEX,
  UNISWAP_QUOTER_V2_ABI,
  CAMELOT_QUOTER_ABI,
  AERODROME_ROUTER_ABI,
  CHAINS,
} from "./config";
import { PriceQuote } from "./types";
import { logger } from "./logger";

// ── Retry helper ─────────────────────────────────────────────────────────────

async function withRetry<T>(
  fn: () => Promise<T>,
  label: string,
  retries = 3,
  delayMs = 2000
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

// ── Uniswap V3 ───────────────────────────────────────────────────────────────

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

  if (!result || result.amountOut === 0n) return null;

  return {
    dex: "Uniswap V3",
    chain: "Ethereum",
    chainId: CHAINS.ETHEREUM,
    amountIn,
    amountOut: result.amountOut,
    amountOutFormatted: parseFloat(ethers.formatUnits(result.amountOut, 18)),
    gasEstimateUSD,
    timestamp: Date.now(),
  };
}

// ── Camelot V3 (Algebra — no fee tier) ───────────────────────────────────────

export async function getCamelotQuote(
  amountIn: bigint,
  gasEstimateUSD: number
): Promise<PriceQuote | null> {
  const quoter = new ethers.Contract(
    DEX.camelotV3.quoter,
    CAMELOT_QUOTER_ABI,
    providers.arbitrum
  );

  const result = await withRetry(async () => {
    return quoter.quoteExactInputSingle.staticCall(
      TOKENS.arbitrum.WETH,
      TOKENS.arbitrum.LINK,
      amountIn,
      0n // limitSqrtPrice = 0 means no limit
    );
  }, "Camelot V3 quote");

  if (!result || result.amountOut === 0n) return null;

  return {
    dex: "Camelot V3",
    chain: "Arbitrum",
    chainId: CHAINS.ARBITRUM,
    amountIn,
    amountOut: result.amountOut,
    amountOutFormatted: parseFloat(ethers.formatUnits(result.amountOut, 18)),
    gasEstimateUSD,
    timestamp: Date.now(),
  };
}

// ── Aerodrome (Base) ──────────────────────────────────────────────────────────

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
        stable: false, // volatile pool
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
