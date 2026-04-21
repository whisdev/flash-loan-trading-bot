import chalk from "chalk";
import * as fs from "fs";
import * as path from "path";

const LOG_FILE = path.join(__dirname, "../logs/arb-scanner.log");
const OPP_FILE = path.join(__dirname, "../logs/opportunities.json");

// Ensure logs directory exists
const logsDir = path.join(__dirname, "../logs");
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

function ts(): string {
  return chalk.gray(`[${new Date().toISOString()}]`);
}

function writeLine(line: string): void {
  const plain = line.replace(/\x1B\[[0-9;]*m/g, ""); // Strip ANSI colour codes
  fs.appendFileSync(LOG_FILE, plain + "\n");
}

export const logger = {
  info(msg: string): void {
    const line = `${ts()} ${chalk.cyan("INFO")}  ${msg}`;
    console.log(line);
    writeLine(`[INFO]  ${msg}`);
  },

  success(msg: string): void {
    const line = `${ts()} ${chalk.green("✔ OK")}  ${msg}`;
    console.log(line);
    writeLine(`[OK]    ${msg}`);
  },

  warn(msg: string): void {
    const line = `${ts()} ${chalk.yellow("WARN")}  ${msg}`;
    console.warn(line);
    writeLine(`[WARN]  ${msg}`);
  },

  error(msg: string, err?: unknown): void {
    const detail = err instanceof Error ? ` — ${err.message}` : "";
    const line = `${ts()} ${chalk.red("ERROR")} ${msg}${detail}`;
    console.error(line);
    writeLine(`[ERROR] ${msg}${detail}`);
  },

  arb(msg: string): void {
    const line = `${ts()} ${chalk.bgGreen.black(" ARB ")} ${chalk.bold.green(msg)}`;
    console.log(line);
    writeLine(`[ARB]   ${msg}`);
  },

  prices(uni: number, cam: number, aero: number): void {
    console.log(
      `${ts()} ${chalk.magenta("PRICES")}` +
        `  Uniswap=${chalk.yellow(uni.toFixed(4))} LINK` +
        `  Camelot=${chalk.yellow(cam.toFixed(4))} LINK` +
        `  Aerodrome=${chalk.yellow(aero.toFixed(4))} LINK`
    );
  },

  divider(): void {
    console.log(chalk.gray("─".repeat(70)));
  },

  /** Append a profitable opportunity to the JSON log */
  logOpportunity(record: object): void {
    let existing: object[] = [];
    if (fs.existsSync(OPP_FILE)) {
      try {
        existing = JSON.parse(fs.readFileSync(OPP_FILE, "utf8"));
      } catch {
        existing = [];
      }
    }
    existing.push(record);
    fs.writeFileSync(OPP_FILE, JSON.stringify(existing, null, 2));
  },
};
