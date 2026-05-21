import { ethers } from "ethers";

/** Normalize any address string to a valid EIP-55 checksum. */
export function checksumAddress(value: string): string {
  return ethers.getAddress(value.toLowerCase());
}
