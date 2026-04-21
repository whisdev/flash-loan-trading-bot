import TelegramBot from "node-telegram-bot-api";
import { ArbitrageOpportunity } from "./types";
import { logger } from "./logger";

let bot: TelegramBot | null = null;

function getBot(): TelegramBot | null {
  if (!process.env.TELEGRAM_BOT_TOKEN) return null;
  if (!bot) {
    bot = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
  }
  return bot;
}

/**
 * Sends a Telegram message when a profitable arbitrage opportunity is detected.
 */
export async function sendArbAlert(opp: ArbitrageOpportunity): Promise<void> {
  const chatId = process.env.TELEGRAM_CHAT_ID;
  const instance = getBot();
  if (!instance || !chatId) return;

  const profitEmoji = opp.netProfitUSD >= 100 ? "🤑" : "💰";

  const msg = [
    `${profitEmoji} *ARBITRAGE OPPORTUNITY DETECTED*`,
    ``,
    `*Buy on:*  ${opp.buyOn.dex} (${opp.buyOn.chain})`,
    `*Sell on:* ${opp.sellOn.dex} (${opp.sellOn.chain})`,
    ``,
    `*LINK In:*    ${opp.buyOn.amountOutFormatted.toFixed(4)} LINK`,
    `*LINK Out:*   ${opp.sellOn.amountOutFormatted.toFixed(4)} LINK`,
    `*Spread:*     ${opp.spreadLINK.toFixed(4)} LINK  ($${opp.spreadUSD.toFixed(2)})`,
    ``,
    `*Gas & Bridge:* -$${opp.totalGasUSD.toFixed(2)}`,
    `*Net Profit:*   *$${opp.netProfitUSD.toFixed(2)}*`,
    ``,
    `ETH: $${opp.ethPriceUSD.toFixed(2)}  |  LINK: $${opp.linkPriceUSD.toFixed(4)}`,
    `_${new Date(opp.timestamp).toUTCString()}_`,
  ].join("\n");

  try {
    await instance.sendMessage(chatId, msg, { parse_mode: "Markdown" });
    logger.success("Telegram alert sent.");
  } catch (err) {
    logger.error("Telegram send failed", err);
  }
}

/**
 * Sends a summary message when the bot starts up.
 */
export async function sendStartupAlert(
  amountETH: string,
  minProfitUSD: number
): Promise<void> {
  const chatId = process.env.TELEGRAM_CHAT_ID;
  const instance = getBot();
  if (!instance || !chatId) return;

  const msg = [
    `🚀 *Cross-Chain Arb Scanner started*`,
    `Trade size: ${amountETH} WETH`,
    `Min profit threshold: $${minProfitUSD}`,
    `Scanning: Uniswap (ETH) | Camelot (ARB) | Aerodrome (BASE)`,
  ].join("\n");

  try {
    await instance.sendMessage(chatId, msg, { parse_mode: "Markdown" });
  } catch {
    // Startup alert failure is non-critical
  }
}
