import { ethers } from "ethers";
import { providers, GAS_UNITS } from "./config";
import { GasCosts } from "./types";
import { logger } from "./logger";

type FeeData = Awaited<ReturnType<typeof providers.ethereum.getFeeData>>;

function pickGasPrice(feeData: FeeData | null): bigint | null {
  if (!feeData) return null;
  return feeData.maxFeePerGas ?? feeData.gasPrice ?? null;
}

function toUSD(
  feeData: FeeData | null,
  gasUnits: bigint,
  ethUSD: number,
  fallbackUSD: number
): number {
  const gasPrice = pickGasPrice(feeData);
  if (!gasPrice) return fallbackUSD;

  const gasCostETH = parseFloat(ethers.formatEther(gasPrice * gasUnits));
  return gasCostETH * ethUSD;
}

/**
 * Fetches the current gas price from each chain and converts to a USD cost
 * estimate based on the estimated gas units for a complex swap + flash loan tx.
 */
export async function estimateGasCosts(ethUSD: number): Promise<GasCosts> {
  const [ethFee, arbFee, baseFee] = await Promise.all([
    providers.ethereum.getFeeData().catch(() => null),
    providers.arbitrum.getFeeData().catch(() => null),
    providers.base.getFeeData().catch(() => null),
  ]);

  const costs: GasCosts = {
    ethereum: toUSD(ethFee, GAS_UNITS.ethereum, ethUSD, 18),
    arbitrum: toUSD(arbFee, GAS_UNITS.arbitrum, ethUSD, 0.25),
    base: toUSD(baseFee, GAS_UNITS.base, ethUSD, 0.05),
  };

  logger.info(
    `Gas costs — ETH $${costs.ethereum.toFixed(2)}  ARB $${costs.arbitrum.toFixed(3)}  BASE $${costs.base.toFixed(3)}`
  );

  return costs;
}
