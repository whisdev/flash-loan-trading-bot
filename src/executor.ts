import { ethers } from "ethers";
import { FLASH_LOAN_ARB_ABI, AAVE, DEX, TOKENS, providers } from "./config";
import { ArbitrageOpportunity, ExecutionResult } from "./types";
import { logger } from "./logger";

/**
 * Builds a signer from the private key in the environment.
 * Only used when DRY_RUN=false.
 */
function getSigner(chain: "ethereum" | "arbitrum" | "base"): ethers.Wallet {
  const pk = process.env.PRIVATE_KEY;
  if (!pk) throw new Error("PRIVATE_KEY not set in .env");
  return new ethers.Wallet(pk, providers[chain]);
}

/**
 * Simulates the execution in dry-run mode — no tx is sent.
 */
async function simulateExecution(opp: ArbitrageOpportunity): Promise<ExecutionResult> {
  logger.arb(
    `[DRY-RUN] Would execute: buy ${opp.buyOn.amountOutFormatted.toFixed(4)} LINK on ${opp.buyOn.dex}, ` +
      `sell on ${opp.sellOn.dex}. Est. profit $${opp.netProfitUSD.toFixed(2)}`
  );
  return { success: true, netProfitUSD: opp.netProfitUSD };
}

/**
 * Executes a SAME-CHAIN flash loan arb via the deployed FlashLoanArb contract
 * on Ethereum using Aave V3.
 *
 * NOTE: Cross-chain flash loans are not atomically possible. This function
 * handles the case where both the cheapest and most expensive DEX happen to
 * be on Ethereum (or when you add same-chain routing logic). For cross-chain
 * arb, pre-funded capital on each chain is required.
 */
async function executeFlashLoanArb(
  opp: ArbitrageOpportunity,
  contractAddress: string
): Promise<ExecutionResult> {
  if (opp.buyOn.chainId !== opp.sellOn.chainId) {
    logger.warn(
      "Cross-chain flash loans are not atomic. Execution requires pre-funded capital on each chain. " +
        "Logging the opportunity and skipping on-chain execution."
    );
    return { success: false, error: "cross-chain-not-atomic" };
  }

  try {
    const signer = getSigner("ethereum");
    const contract = new ethers.Contract(contractAddress, FLASH_LOAN_ARB_ABI, signer);

    // Minimum LINK we expect out of the sell side (1% slippage tolerance)
    const minAmountOut = (opp.sellOn.amountOut * 99n) / 100n;

    logger.arb(
      `Sending flash loan tx — buy on ${opp.buyOn.dex}, sell on ${opp.sellOn.dex}, ` +
        `minOut=${ethers.formatUnits(minAmountOut, 18)} LINK`
    );

    const tx = await contract.requestFlashLoanArb(
      TOKENS.ethereum.WETH,         // asset to borrow
      opp.buyOn.amountIn,           // 1 WETH
      DEX.uniswapV3.router,         // buy router
      DEX.uniswapV3.router,         // sell router (same chain)
      TOKENS.ethereum.WETH,
      TOKENS.ethereum.LINK,
      DEX.uniswapV3.fee,
      DEX.uniswapV3.fee,
      minAmountOut,
      { gasLimit: 500_000n }
    );

    logger.arb(`Tx submitted: ${tx.hash} — waiting for confirmation...`);
    const receipt = await tx.wait();

    if (receipt.status === 1) {
      logger.success(`Arb executed! Tx: ${tx.hash}`);
      return { success: true, txHash: tx.hash, netProfitUSD: opp.netProfitUSD };
    } else {
      logger.error(`Tx reverted: ${tx.hash}`);
      return { success: false, txHash: tx.hash, error: "tx-reverted" };
    }
  } catch (err) {
    logger.error("Flash loan execution error", err);
    return {
      success: false,
      error: err instanceof Error ? err.message : "unknown-error",
    };
  }
}

/**
 * Entry point called by the main loop when a profitable opportunity is found.
 */
export async function executeArbitrage(
  opp: ArbitrageOpportunity,
  dryRun: boolean
): Promise<ExecutionResult> {
  if (dryRun) return simulateExecution(opp);

  const contractAddress = process.env.ARB_CONTRACT_ADDRESS;
  if (!contractAddress) {
    logger.warn(
      "ARB_CONTRACT_ADDRESS not set — cannot execute. Switch to DRY_RUN=true or deploy FlashLoanArb.sol."
    );
    return { success: false, error: "no-contract-address" };
  }

  return executeFlashLoanArb(opp, contractAddress);
}
