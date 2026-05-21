// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

// ── Minimal interfaces (no external package dependency) ───────────────────────

interface IPool {
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params)
        external
        payable
        returns (uint256 amountOut);
}

interface IERC20 {
    function approve(address spender, uint256 amount) external returns (bool);
    function transfer(address to, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
}

// ── FlashLoanArb ──────────────────────────────────────────────────────────────

/**
 * @title FlashLoanArb
 * @notice Borrows WETH from Aave V3, buys LINK on one Uniswap V3 pool,
 *         sells LINK on another Uniswap V3 pool (or compatible router),
 *         repays Aave, and keeps the spread profit.
 *
 * Deploy on Ethereum mainnet. The TypeScript bot calls `requestFlashLoanArb`
 * when a profitable same-chain opportunity is detected.
 */
contract FlashLoanArb {
    // ── State ─────────────────────────────────────────────────────────────────
    address public owner;
    IPool public immutable aavePool;

    // Aave V3 Ethereum mainnet pool
    address private constant AAVE_POOL_ADDRESS =
        0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2;

    // ── Events ────────────────────────────────────────────────────────────────
    event ArbExecuted(uint256 profit, uint256 repayAmount);
    event Withdrawal(address token, uint256 amount);

    // ── Errors ────────────────────────────────────────────────────────────────
    error OnlyOwner();
    error OnlyAavePool();
    error InvalidInitiator();
    error ProfitBelowMinimum(uint256 got, uint256 min);

    // ── Constructor ───────────────────────────────────────────────────────────
    constructor() {
        owner = msg.sender;
        aavePool = IPool(AAVE_POOL_ADDRESS);
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert OnlyOwner();
        _;
    }

    // ── External: initiate flash loan ─────────────────────────────────────────

    /**
     * @param asset         Token to borrow (WETH)
     * @param amount        Amount to borrow (e.g. 1 WETH = 1e18)
     * @param buyRouter     Uniswap-compatible router to buy tokenOut on
     * @param sellRouter    Uniswap-compatible router to sell tokenOut on
     * @param tokenIn       Token we borrow & repay (WETH)
     * @param tokenOut      Token we buy & sell (LINK)
     * @param buyFee        Pool fee for the buy leg (e.g. 3000 = 0.3%)
     * @param sellFee       Pool fee for the sell leg
     * @param minAmountOut  Minimum LINK expected from the buy leg (slippage guard)
     */
    function requestFlashLoanArb(
        address asset,
        uint256 amount,
        address buyRouter,
        address sellRouter,
        address tokenIn,
        address tokenOut,
        uint24 buyFee,
        uint24 sellFee,
        uint256 minAmountOut
    ) external onlyOwner {
        bytes memory params = abi.encode(
            buyRouter,
            sellRouter,
            tokenIn,
            tokenOut,
            buyFee,
            sellFee,
            minAmountOut
        );

        aavePool.flashLoanSimple(
            address(this), // receiver
            asset,         // WETH
            amount,
            params,
            0              // referral code
        );
    }

    // ── Aave callback ─────────────────────────────────────────────────────────

    /**
     * @notice Called by Aave after the flash loan is sent to this contract.
     *         Must repay `amount + premium` of `asset` before returning.
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool) {
        if (msg.sender != AAVE_POOL_ADDRESS) revert OnlyAavePool();
        if (initiator != address(this)) revert InvalidInitiator();

        (
            address buyRouter,
            address sellRouter,
            address tokenIn,
            address tokenOut,
            uint24 buyFee,
            uint24 sellFee,
            uint256 minAmountOut
        ) = abi.decode(params, (address, address, address, address, uint24, uint24, uint256));

        uint256 repayAmount = amount + premium;

        // Step 1: Approve buy router to spend WETH
        IERC20(tokenIn).approve(buyRouter, amount);

        // Step 2: Buy LINK with borrowed WETH
        uint256 linkReceived = ISwapRouter(buyRouter).exactInputSingle(
            ISwapRouter.ExactInputSingleParams({
                tokenIn: tokenIn,
                tokenOut: tokenOut,
                fee: buyFee,
                recipient: address(this),
                deadline: block.timestamp,
                amountIn: amount,
                amountOutMinimum: minAmountOut,
                sqrtPriceLimitX96: 0
            })
        );

        // Step 3: Approve sell router to spend LINK
        IERC20(tokenOut).approve(sellRouter, linkReceived);

        // Step 4: Sell LINK back to WETH — must yield >= repayAmount
        uint256 wethReceived = ISwapRouter(sellRouter).exactInputSingle(
            ISwapRouter.ExactInputSingleParams({
                tokenIn: tokenOut,
                tokenOut: tokenIn,
                fee: sellFee,
                recipient: address(this),
                deadline: block.timestamp,
                amountIn: linkReceived,
                amountOutMinimum: repayAmount, // hard slippage guard
                sqrtPriceLimitX96: 0
            })
        );

        if (wethReceived < repayAmount) {
            revert ProfitBelowMinimum(wethReceived, repayAmount);
        }

        // Step 5: Approve Aave to pull the repayment
        IERC20(asset).approve(AAVE_POOL_ADDRESS, repayAmount);

        uint256 profit = wethReceived - repayAmount;
        emit ArbExecuted(profit, repayAmount);

        return true;
    }

    // ── Owner utilities ───────────────────────────────────────────────────────

    /** Withdraw any ERC-20 token accumulated as profit */
    function withdrawToken(address token, uint256 amount) external onlyOwner {
        IERC20(token).transfer(owner, amount);
        emit Withdrawal(token, amount);
    }

    /** Withdraw ETH (in case ETH is accidentally sent here) */
    function withdrawETH() external onlyOwner {
        payable(owner).transfer(address(this).balance);
    }

    receive() external payable {}
}
