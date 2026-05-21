import { ethers } from "ethers";
import chalk from "chalk";
import { BOT_CONFIG } from "./config";
import { ArbitrageOpportunity, PriceQuote } from "./types";
import { logger } from "./logger";
import { fetchMarketPrices } from "./priceFeed";
import { estimateGasCosts } from "./gasEstimator";
import { fetchAllQuotes } from "./scanner";
import { executeArbitrage } from "./executor";
import { sendArbAlert, sendStartupAlert } from "./alerts";
import { validateEnvironment } from "./startup";

// ── Bridge overhead ───────────────────────────────────────────────────────────
// Cross-chain arb always requires bridging on at least one leg.
// Fixed $10 estimate (Stargate/Across bridging fees + slippage).
const BRIDGE_FEE_USD = 10;

// ── Detect best arb opportunity from a set of quotes ─────────────────────────

function findBestOpportunity(
  quotes: PriceQuote[],
  gasCosts: { ethereum: number; arbitrum: number; base: number },
  linkPriceUSD: number,
  ethPriceUSD: number
): ArbitrageOpportunity | null {
  const chainToGas: Record<string, number> = {
    Ethereum: gasCosts.ethereum,
    Arbitrum: gasCosts.arbitrum,
    Base: gasCosts.base,
  };

  let bestOpp: ArbitrageOpportunity | null = null;

  // Check every pair of quotes
  for (let i = 0; i < quotes.length; i++) {
    for (let j = 0; j < quotes.length; j++) {
      if (i === j) continue;

      // More LINK per WETH = cheaper LINK (buy leg). Less LINK = expensive (sell leg).
      if (quotes[i].amountOutFormatted <= quotes[j].amountOutFormatted) continue;

      const buyOn = quotes[i];
      const sellOn = quotes[j];

      const spreadLINK = buyOn.amountOutFormatted - sellOn.amountOutFormatted;
      const spreadUSD = spreadLINK * linkPriceUSD;

      const isCrossChain = buyOn.chainId !== sellOn.chainId;
      const bridgeFee = isCrossChain ? BRIDGE_FEE_USD : 0;
      const totalGasUSD =
        chainToGas[buyOn.chain] + chainToGas[sellOn.chain] + bridgeFee;

      const netProfitUSD = spreadUSD - totalGasUSD;

      if (
        !bestOpp ||
        netProfitUSD > bestOpp.netProfitUSD
      ) {
        bestOpp = {
          buyOn,
          sellOn,
          spreadLINK,
          spreadUSD,
          totalGasUSD,
          netProfitUSD,
          linkPriceUSD,
          ethPriceUSD,
          timestamp: Date.now(),
        };
      }
    }
  }

  return bestOpp;
}

// ── Main scan tick ────────────────────────────────────────────────────────────

async function tick(amountIn: bigint): Promise<void> {
  logger.divider();
  logger.info(`Scanning quotes for ${ethers.formatEther(amountIn)} WETH...`);

  const prices = await fetchMarketPrices();
  const gasCosts = await estimateGasCosts(prices.ethUSD);
  const [uniQuote, camQuote, aeroQuote] = await fetchAllQuotes(amountIn, gasCosts);

  // Print price table
  logger.prices(
    uniQuote?.amountOutFormatted ?? 0,
    camQuote?.amountOutFormatted ?? 0,
    aeroQuote?.amountOutFormatted ?? 0
  );

  const validQuotes = [uniQuote, camQuote, aeroQuote].filter(
    (q): q is PriceQuote => q !== null && q.amountOutFormatted > 0
  );

  if (validQuotes.length < 2) {
    logger.warn("Not enough valid quotes — skipping this tick.");
    return;
  }

  const opp = findBestOpportunity(
    validQuotes,
    gasCosts,
    prices.linkUSD,
    prices.ethUSD
  );

  if (!opp) {
    logger.info("No spread found between any DEX pair.");
    return;
  }

  const isCrossChain = opp.buyOn.chainId !== opp.sellOn.chainId;

  console.log(
    `\n  Buy  ▶  ${chalk.cyan(opp.buyOn.dex)} (${opp.buyOn.chain}) — ${chalk.yellow(opp.buyOn.amountOutFormatted.toFixed(4))} LINK` +
      `\n  Sell ▶  ${chalk.cyan(opp.sellOn.dex)} (${opp.sellOn.chain}) — ${chalk.yellow(opp.sellOn.amountOutFormatted.toFixed(4))} LINK` +
      `\n  Spread  ${chalk.magenta(opp.spreadLINK.toFixed(4))} LINK  ($${opp.spreadUSD.toFixed(2)})` +
      `\n  Gas+Bridge  -$${opp.totalGasUSD.toFixed(2)}  (${isCrossChain ? "cross-chain" : "same-chain"})` +
      `\n  Net Profit  ${opp.netProfitUSD >= BOT_CONFIG.minProfitUSD ? chalk.bold.green : chalk.red}($${opp.netProfitUSD.toFixed(2)})\n`
  );

  if (opp.netProfitUSD >= BOT_CONFIG.minProfitUSD) {
    logger.arb(
      `PROFITABLE ARB FOUND! $${opp.netProfitUSD.toFixed(2)} net — executing...`
    );
    logger.logOpportunity(opp);

    await Promise.all([
      sendArbAlert(opp),
      executeArbitrage(opp, BOT_CONFIG.dryRun),
    ]);
  } else {
    logger.info(
      `Spread $${opp.netProfitUSD.toFixed(2)} — below $${BOT_CONFIG.minProfitUSD} threshold.`
    );
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log(chalk.bold.cyan("\n  ╔══════════════════════════════════════╗"));
  console.log(chalk.bold.cyan("  ║   Cross-Chain Arbitrage Scanner      ║"));
  console.log(chalk.bold.cyan("  ║   Uniswap · Camelot · Aerodrome      ║"));
  console.log(chalk.bold.cyan("  ╚══════════════════════════════════════╝\n"));

  logger.info(`Mode: ${BOT_CONFIG.dryRun ? chalk.yellow("DRY RUN") : chalk.red("LIVE")}`);
  logger.info(`Min profit: $${BOT_CONFIG.minProfitUSD}`);
  logger.info(`Scan interval: ${BOT_CONFIG.scanIntervalMs / 1000}s`);
  logger.info(`Telegram alerts: ${BOT_CONFIG.telegramEnabled ? "enabled" : "disabled"}`);

  const amountIn = ethers.parseEther(BOT_CONFIG.amountInETH);

  await validateEnvironment();
  await sendStartupAlert(BOT_CONFIG.amountInETH, BOT_CONFIG.minProfitUSD);

  // Run immediately then poll
  await tick(amountIn);

  setInterval(async () => {
    await tick(amountIn).catch((err) =>
      logger.error("Unhandled tick error", err)
    );
  }, BOT_CONFIG.scanIntervalMs);
}

main().catch((err) => {
  logger.error("Fatal startup error", err);
  process.exit(1);
});
