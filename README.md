# Flash loan funded Cross-Chain Arbitrage Scanner

A production-grade TypeScript bot that monitors the `WETH/LINK` price across three DEXs on three different EVM chains simultaneously. When a profitable spread is found (after deducting real on-chain gas and bridging costs), it optionally executes an Aave V3 flash-loan-funded trade via a custom Solidity smart contract.

## Demo

<video src="https://cdn.jsdelivr.net/gh/whisdev/flash-loan-trading-bot@main/logs/video.mp4" controls width="100%"></video>

## Architecture

```
src/
├── index.ts          Main polling loop — orchestrates everything
├── config.ts         All contract addresses, ABIs, and bot settings
├── types.ts          TypeScript interfaces (PriceQuote, ArbitrageOpportunity …)
├── scanner.ts        Fetches live quotes from Uniswap V3, Camelot V3, Aerodrome
├── priceFeed.ts      Fetches ETH/USD + LINK/USD from CoinGecko (with caching)
├── gasEstimator.ts   Queries on-chain gas prices, converts to real USD cost
├── executor.ts       Signs and sends the flash loan transaction to the contract
├── startup.ts        Validates RPC connectivity and live DEX quotes on boot
├── alerts.ts         Sends Telegram notifications on profitable opportunities
└── logger.ts         Colorized terminal + file logging (logs/ directory)

contracts/
└── FlashLoanArb.sol  Aave V3 flash loan contract (deploy on Ethereum mainnet)

logs/
├── arb-scanner.log   Full plain-text log of every scan
├── opportunities.json  JSON log of every profitable opportunity detected
└── video.mp4         Demo recording of the scanner in action
```

## Supported DEXs

| DEX          | Chain    | Protocol      |
|-------------|----------|---------------|
| Uniswap V3  | Ethereum | Concentrated liquidity (0.3% fee tier) |
| Camelot V3  | Arbitrum | Algebra dynamic-fee AMM |
| Aerodrome   | Base     | Solidly volatile AMM |

## Prerequisites

- Node.js 18+
- RPC URLs for Ethereum, Arbitrum, and Base (free tier at [Alchemy](https://alchemy.com))
- (Optional) A deployed `FlashLoanArb.sol` contract and a funded wallet for live mode

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Copy and configure environment variables
cp .env.example .env
#    → Add your RPC URLs (required)
#    → Add PRIVATE_KEY + ARB_CONTRACT_ADDRESS only if running LIVE

# 3. Start in safe dry-run mode (scans only, no real transactions)
npm run start:dry

# 4. Or start live (requires contract deployed + wallet funded)
npm start
```

## How It Works

1. **Price Scanning** — Every 12 seconds the bot queries the on-chain quoter/router contracts for all three DEXs simultaneously using `ethers.js`.
2. **Gas Estimation** — `getFeeData()` is called on each chain provider. The result is multiplied by estimated gas units and the live ETH/USD price from CoinGecko.
3. **Spread Calculation** — The bot checks every pair of DEXs (6 combinations). For each pair it calculates:
   ```
   Net Profit = (sell_LINK - buy_LINK) × LINK_USD - gas_buy - gas_sell - bridge_fee
   ```
4. **Threshold Check** — If `Net Profit > MIN_PROFIT_USD`, the opportunity is logged and an alert is sent via Telegram.
5. **Execution** — If `DRY_RUN=false` and `ARB_CONTRACT_ADDRESS` is set, the bot calls `requestFlashLoanArb()` on the deployed Solidity contract.

## Smart Contract (FlashLoanArb.sol)

The contract uses **Aave V3 Simple Flash Loans** (no Aave collateral required):

```
requestFlashLoanArb()
    └── aavePool.flashLoanSimple()
            └── executeOperation() [Aave callback]
                    ├── buy LINK on cheapest router  (exactInputSingle)
                    ├── sell LINK on expensive router (exactInputSingle)
                    ├── repay Aave principal + 0.05% premium
                    └── keep the profit in the contract
```

### Deploy with Remix

1. Open [remix.ethereum.org](https://remix.ethereum.org)
2. Paste `contracts/FlashLoanArb.sol`
3. Compile with Solidity `0.8.20`
4. Deploy on Ethereum mainnet with MetaMask
5. Copy the deployed address into `.env` as `ARB_CONTRACT_ADDRESS`

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `ETH_RPC_URL` | PublicNode | Ethereum JSON-RPC endpoint |
| `ARB_RPC_URL` | Public Arbitrum | Arbitrum JSON-RPC endpoint |
| `BASE_RPC_URL` | Public Base | Base JSON-RPC endpoint |
| `DRY_RUN` | `true` (safe default) | Scan only — no transactions sent |
| `MIN_PROFIT_USD` | `30` | Minimum net profit to trigger execution |
| `PRIVATE_KEY` | — | Wallet private key (live mode only) |
| `ARB_CONTRACT_ADDRESS` | — | Deployed FlashLoanArb.sol address |
| `TELEGRAM_BOT_TOKEN` | — | Optional Telegram bot token |
| `TELEGRAM_CHAT_ID` | — | Optional Telegram chat ID |

## Terminal Output Example

```
  ╔═════════════════════════════════════════════════╗
  ║   Cross-Chain Flash Loan Arbitrage Scanner      ║
  ║        Uniswap · Camelot · Aerodrome            ║
  ╚═════════════════════════════════════════════════╝

[2024-04-18T10:00:00.000Z] INFO   Mode: DRY RUN
[2024-04-18T10:00:00.000Z] INFO   Min profit: $30
──────────────────────────────────────────────────────────────────────
[2024-04-18T10:00:01.000Z] INFO   Market prices refreshed — ETH $3121.00  LINK $18.4200
[2024-04-18T10:00:02.000Z] INFO   Gas costs — ETH $14.87  ARB $0.021  BASE $0.004
[2024-04-18T10:00:04.000Z] PRICES  Uniswap=185.1023 LINK  Camelot=184.2001 LINK  Aerodrome=186.5042 LINK

  Buy  ▶  Camelot V3 (Arbitrum) — 184.2001 LINK
  Sell ▶  Aerodrome (Base)      — 186.5042 LINK
  Spread  2.3041 LINK  ($42.43)
  Gas+Bridge  -$10.22  (cross-chain)
  Net Profit  ($32.21)

[2024-04-18T10:00:04.000Z]  ARB   PROFITABLE ARB FOUND! $32.21 net — executing...
[2024-04-18T10:00:04.000Z]  ARB   [DRY-RUN] Would execute arb. Est. profit $32.21
```

## Important Notes

- **Cross-chain flash loans are not atomically possible.** The scanner detects cross-chain spreads, but the executor currently only sends same-chain flash loan transactions. For cross-chain execution, pre-funded capital on each chain is required.
- Use Alchemy or Infura RPC URLs — public endpoints can rate-limit under load.
- On startup the bot verifies all three RPC endpoints and fetches a live quote from each DEX before scanning.
- Always run in `DRY_RUN=true` mode first to verify the spread logic before going live.

## Disclaimer

This software is for educational purposes only. Arbitrage is highly competitive. On-chain transactions can fail or be front-run. Never invest more than you can afford to lose.
