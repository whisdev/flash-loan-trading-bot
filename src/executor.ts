import { ethers } from "ethers";
import {
  FLASH_LOAN_ARB_ABI,
  TOKENS,
  providers,
  getDexExecutionConfig,
  CHAINS,
} from "./config";
import { ArbitrageOpportunity, ExecutionResult } from "./types";
import { logger } from "./logger";

function getSigner(chain: "ethereum" | "arbitrum" | "base"): ethers.Wallet {
  const pk = process.env.PRIVATE_KEY;
  if (!pk) throw new Error("PRIVATE_KEY not set in .env");
  return new ethers.Wallet(pk, providers[chain]);
}

async function simulateExecution(opp: ArbitrageOpportunity): Promise<ExecutionResult> {
  const isCrossChain = opp.buyOn.chainId !== opp.sellOn.chainId;
  logger.arb(
    `[DRY-RUN] Would execute: buy ${opp.buyOn.amountOutFormatted.toFixed(4)} LINK on ${opp.buyOn.dex}, ` +
      `sell on ${opp.sellOn.dex}. Est. profit $${opp.netProfitUSD.toFixed(2)}` +
      (isCrossChain ? " (cross-chain — on-chain flash loan not atomic)" : "")
  );
  return { success: true, netProfitUSD: opp.netProfitUSD };
}

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

  if (opp.buyOn.chainId !== CHAINS.ETHEREUM) {
    logger.warn(
      "Flash loan contract is deployed on Ethereum only. Same-chain execution on L2 is not supported yet."
    );
    return { success: false, error: "flash-loan-ethereum-only" };
  }

  try {
    const buyDex = getDexExecutionConfig(opp.buyOn);
    const sellDex = getDexExecutionConfig(opp.sellOn);
    const signer = getSigner("ethereum");
    const contract = new ethers.Contract(contractAddress, FLASH_LOAN_ARB_ABI, signer);

    const minAmountOut = (opp.buyOn.amountOut * 99n) / 100n;

    logger.arb(
      `Sending flash loan tx — buy on ${opp.buyOn.dex}, sell on ${opp.sellOn.dex}, ` +
        `minOut=${ethers.formatUnits(minAmountOut, 18)} LINK`
    );

    const tx = await contract.requestFlashLoanArb(
      TOKENS.ethereum.WETH,
      opp.buyOn.amountIn,
      buyDex.router,
      sellDex.router,
      TOKENS.ethereum.WETH,
      TOKENS.ethereum.LINK,
      buyDex.fee,
      sellDex.fee,
      minAmountOut,
      { gasLimit: 500_000n }
    );

    logger.arb(`Tx submitted: ${tx.hash} — waiting for confirmation...`);
    const receipt = await tx.wait();

    if (receipt?.status === 1) {
      logger.success(`Arb executed! Tx: ${tx.hash}`);
      return { success: true, txHash: tx.hash, netProfitUSD: opp.netProfitUSD };
    }

    logger.error(`Tx reverted: ${tx.hash}`);
    return { success: false, txHash: tx.hash, error: "tx-reverted" };
  } catch (err) {
    logger.error("Flash loan execution error", err);
    return {
      success: false,
      error: err instanceof Error ? err.message : "unknown-error",
    };
  }
}

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
