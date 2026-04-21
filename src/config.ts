import * as dotenv from "dotenv";
import { ethers } from "ethers";

dotenv.config();

// ── Chain IDs ────────────────────────────────────────────────────────────────
export const CHAINS = {
  ETHEREUM: 1,
  ARBITRUM: 42161,
  BASE: 8453,
};

// ── RPC Providers ────────────────────────────────────────────────────────────
export const providers = {
  ethereum: new ethers.JsonRpcProvider(
    process.env.ETH_RPC_URL || "https://rpc.ankr.com/eth"
  ),
  arbitrum: new ethers.JsonRpcProvider(
    process.env.ARB_RPC_URL || "https://arb1.arbitrum.io/rpc"
  ),
  base: new ethers.JsonRpcProvider(
    process.env.BASE_RPC_URL || "https://mainnet.base.org"
  ),
};

// ── Token Addresses ───────────────────────────────────────────────────────────
export const TOKENS = {
  ethereum: {
    WETH: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    LINK: "0x514910771AF9Ca656af840dff83E8264EcF986CA",
  },
  arbitrum: {
    WETH: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
    LINK: "0xf97f4df75154aceca141af238c9c90f2d5743265",
  },
  base: {
    WETH: "0x4200000000000000000000000000000000000006",
    LINK: "0x88814edAA52EDBC09c5332463F8B3eD5b0A30e2f",
  },
};

// ── DEX Addresses ─────────────────────────────────────────────────────────────
export const DEX = {
  // Uniswap V3 on Ethereum
  uniswapV3: {
    quoter: "0x61fFE014bA17989E743c5F6cB21bF9697530B21e", // QuoterV2
    router: "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    fee: 3000, // 0.3% — the main WETH/LINK pool
  },
  // Camelot V3 on Arbitrum (Algebra protocol — no fixed fee tiers)
  camelotV3: {
    quoter: "0x0FCEeCA271f28b6d393433C1B5e95B8DC6c61f21",
    router: "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18",
  },
  // Aerodrome on Base (Solidly-style AMM)
  aerodrome: {
    router: "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43",
    volatileFactory: "0x420DD381b31aEf6683db6B902084cB0FFeCE40Da",
  },
};

// ── Aave V3 addresses (Ethereum mainnet) for flash loan ───────────────────────
export const AAVE = {
  pool: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
  poolAddressesProvider: "0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e",
};

// ── Gas units estimate for complex swap + flash loan transactions ──────────────
export const GAS_UNITS = {
  ethereum: 350_000n,
  arbitrum: 2_000_000n, // L2 gas is priced differently
  base: 2_000_000n,
};

// ── Bot settings ──────────────────────────────────────────────────────────────
export const BOT_CONFIG = {
  amountInETH: "1.0",                               // Trade size in WETH
  scanIntervalMs: 12_000,                           // Scan every 12 seconds
  minProfitUSD: parseFloat(process.env.MIN_PROFIT_USD || "30"),
  dryRun: process.env.DRY_RUN === "true",
  telegramEnabled: !!process.env.TELEGRAM_BOT_TOKEN,
  maxRetries: 3,
  retryDelayMs: 2_000,
};

// ── ABIs ──────────────────────────────────────────────────────────────────────

// Uniswap V3 QuoterV2
export const UNISWAP_QUOTER_V2_ABI = [
  "function quoteExactInputSingle((address tokenIn, address tokenOut, uint256 amountIn, uint24 fee, uint160 sqrtPriceLimitX96) params) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)",
];

// Camelot / Algebra QuoterV2 (no fee param — uses dynamic fee)
export const CAMELOT_QUOTER_ABI = [
  "function quoteExactInputSingle(address tokenIn, address tokenOut, uint256 amountIn, uint160 limitSqrtPrice) external returns (uint256 amountOut, uint16 fee, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate)",
];

// Aerodrome Router (Solidly-style)
export const AERODROME_ROUTER_ABI = [
  "function getAmountsOut(uint256 amountIn, (address from, address to, bool stable, address factory)[] routes) external view returns (uint256[] amounts)",
];

// Uniswap V3 SwapRouter (for execution)
export const UNISWAP_SWAP_ROUTER_ABI = [
  "function exactInputSingle((address tokenIn, address tokenOut, uint24 fee, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum, uint160 sqrtPriceLimitX96) params) external payable returns (uint256 amountOut)",
];

// Standard ERC-20 (approve + balance)
export const ERC20_ABI = [
  "function approve(address spender, uint256 amount) external returns (bool)",
  "function balanceOf(address account) external view returns (uint256)",
  "function decimals() external view returns (uint8)",
];

// FlashLoanArb contract ABI (matches contracts/FlashLoanArb.sol)
export const FLASH_LOAN_ARB_ABI = [
  "function requestFlashLoanArb(address asset, uint256 amount, address buyRouter, address sellRouter, address tokenIn, address tokenOut, uint24 buyFee, uint24 sellFee, uint256 minAmountOut) external",
  "function withdrawToken(address token, uint256 amount) external",
  "function withdrawETH() external",
  "event ArbExecuted(uint256 profit, uint256 repayAmount)",
];
