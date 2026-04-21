import { ethers } from "ethers";
import { providers, GAS_UNITS } from "./config";
import { GasCosts } from "./types";
import { logger } from "./logger";

/**
 * Fetches the current maxFeePerGas from each chain and converts to a USD cost
 * estimate based on the estimated gas units for a complex swap + flash loan tx.
 *
 * @param ethUSD  Current ETH/USD price
 */
export async function estimateGasCosts(ethUSD: number): Promise<GasCosts> {
  const [ethFee, arbFee, baseFee] = await Promise.all([
    providers.ethereum.getFeeData().catch(() => null),
    providers.arbitrum.getFeeData().catch(() => null),
    providers.base.getFeeData().catch(() => null),
  ]);

  function toUSD(
    feeData: Awaited<ReturnType<typeof providers.ethereum.getFeeData>> | null,
    gasUnits: bigint,
    fallbackGweiUSD: number
  ): number {
    if (!feeData?.maxFeePerGas) return fallbackGweiUSD;
    const gasCostWei = feeData.maxFeePerGas * gasUnits;
    const gasCostETH = parseFloat(ethers.formatEther(gasCostWei));
    return gasCostETH * ethUSD;
  }

  const costs: GasCosts = {
    ethereum: toUSD(ethFee, GAS_UNITS.ethereum, 18),
    arbitrum: toUSD(arbFee, GAS_UNITS.arbitrum, 0.25),
    base: toUSD(baseFee, GAS_UNITS.base, 0.05),
  };

  logger.info(
    `Gas costs — ETH $${costs.ethereum.toFixed(2)}  ARB $${costs.arbitrum.toFixed(3)}  BASE $${costs.base.toFixed(3)}`
  );

  return costs;
}
