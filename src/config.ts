import * as dotenv from "dotenv";
import { ethers } from "ethers";
import { checksumAddress } from "./addresses";

dotenv.config();

// ── Chain IDs ────────────────────────────────────────────────────────────────
export const CHAINS = {
  ETHEREUM: 1,
  ARBITRUM: 42161,
  BASE: 8453,
} as const;

function createProvider(url: string, chainId: number): ethers.JsonRpcProvider {
  const network = ethers.Network.from(chainId);
  return new ethers.JsonRpcProvider(url, network, { staticNetwork: network });
}

// ── RPC Providers ────────────────────────────────────────────────────────────
export const providers = {
  ethereum: createProvider(
    process.env.ETH_RPC_URL || "https://ethereum.publicnode.com",
    CHAINS.ETHEREUM
  ),
  arbitrum: createProvider(
    process.env.ARB_RPC_URL || "https://arb1.arbitrum.io/rpc",
    CHAINS.ARBITRUM
  ),
  base: createProvider(
    process.env.BASE_RPC_URL || "https://mainnet.base.org",
    CHAINS.BASE
  ),
};

// ── Token Addresses ───────────────────────────────────────────────────────────
export const TOKENS = {
  ethereum: {
    WETH: checksumAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
    LINK: checksumAddress("0x514910771AF9Ca656af840dff83E8264EcF986CA"),
  },
  arbitrum: {
    WETH: checksumAddress("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"),
    LINK: checksumAddress("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4"),
  },
  base: {
    WETH: checksumAddress("0x4200000000000000000000000000000000000006"),
    LINK: checksumAddress("0x88Fb150BDc53A65fe94Dea0c9BA0a6dAf8C6e196"),
  },
};

// ── DEX Addresses ─────────────────────────────────────────────────────────────
export const DEX = {
  uniswapV3: {
    quoter: checksumAddress("0x61fFE014bA17989E743c5F6cB21bF9697530B21e"),
    router: checksumAddress("0xE592427A0AEce92De3Edee1F18E0157C05861564"),
    fee: 3000,
  },
  camelotV3: {
    quoter: checksumAddress("0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E"),
    router: checksumAddress("0x1F721E2E82F6676FCE4eA07A5958cF098D339e18"),
  },
  aerodrome: {
    router: checksumAddress("0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"),
    volatileFactory: checksumAddress("0x420DD381b31aEf6683db6B902084cB0FFeCE40Da"),
  },
};

export const AAVE = {
  pool: checksumAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"),
  poolAddressesProvider: checksumAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
};

export const GAS_UNITS = {
  ethereum: 350_000n,
  arbitrum: 400_000n,
  base: 400_000n,
};

export const BOT_CONFIG = {
  amountInETH: "1.0",
  scanIntervalMs: 12_000,
  minProfitUSD: parseFloat(process.env.MIN_PROFIT_USD || "30"),
  dryRun: process.env.DRY_RUN !== "false",
  telegramEnabled: !!process.env.TELEGRAM_BOT_TOKEN,
  maxRetries: 3,
  retryDelayMs: 2_000,
};

export const UNISWAP_QUOTER_V2_ABI = [
  "function quoteExactInputSingle((address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96) params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)",
];

export const CAMELOT_QUOTER_ABI = [
  "function quoteExactInput(bytes path, uint256 amountIn) external returns (uint256 amountOut, uint160[] sqrtPriceX96AfterList, uint32[] initializedTicksCrossedList, uint256 gasEstimate)",
];

export const AERODROME_ROUTER_ABI = [
  "function getAmountsOut(uint256 amountIn, (address from, address to, bool stable, address factory)[] routes) external view returns (uint256[] amounts)",
];

export const UNISWAP_SWAP_ROUTER_ABI = [
  "function exactInputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96) params) external payable returns (uint256 amountOut)",
];

export const ERC20_ABI = [
  "function approve(address spender, uint256 amount) external returns (bool)",
  "function balanceOf(address account) external view returns (uint256)",
  "function decimals() external view returns (uint8)",
];

export const FLASH_LOAN_ARB_ABI = [
  "function requestFlashLoanArb(address asset, uint256 amount, address buyRouter, address sellRouter, address tokenIn, address tokenOut, uint24 buyFee, uint24 sellFee, uint256 minAmountOut) external",
  "function withdrawToken(address token, uint256 amount) external",
  "function withdrawETH() external",
  "event ArbExecuted(uint256 profit, uint256 repayAmount)",
];

export function getDexExecutionConfig(quote: { dex: string; chainId: number }): {
  router: string;
  fee: number;
} {
  if (quote.chainId === CHAINS.ETHEREUM && quote.dex.startsWith("Uniswap")) {
    return { router: DEX.uniswapV3.router, fee: DEX.uniswapV3.fee };
  }
  if (quote.chainId === CHAINS.ARBITRUM && quote.dex.startsWith("Camelot")) {
    return { router: DEX.camelotV3.router, fee: 0 };
  }
  if (quote.chainId === CHAINS.BASE && quote.dex.startsWith("Aerodrome")) {
    return { router: DEX.aerodrome.router, fee: 0 };
  }
  throw new Error(`No execution router configured for ${quote.dex} on chain ${quote.chainId}`);
}
