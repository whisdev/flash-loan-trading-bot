// ── All shared TypeScript interfaces ────────────────────────────────────────

export interface PriceQuote {
  dex: string;
  chain: string;
  chainId: number;
  amountIn: bigint;
  amountOut: bigint;
  amountOutFormatted: number;
  gasEstimateUSD: number;
  timestamp: number;
}

export interface ArbitrageOpportunity {
  buyOn: PriceQuote;
  sellOn: PriceQuote;
  spreadLINK: number;
  spreadUSD: number;
  totalGasUSD: number;
  netProfitUSD: number;
  linkPriceUSD: number;
  ethPriceUSD: number;
  timestamp: number;
}

export interface MarketPrices {
  ethUSD: number;
  linkUSD: number;
}

export interface GasCosts {
  ethereum: number;
  arbitrum: number;
  base: number;
}

export interface ChainGasFee {
  maxFeePerGas: bigint;
  gasPriceGwei: number;
}

export interface ExecutionResult {
  success: boolean;
  txHash?: string;
  netProfitUSD?: number;
  error?: string;
}
