// Helpers that reach the stack the way the shell scripts do: osctl, psql in
// the postgres container, and scripts/lib/volume.sh for chunk files.
import { execFileSync, spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

export const root = path.resolve(import.meta.dirname, "..", "..");
const compose = ["compose", "-f", path.join(root, "docker-compose.yml")];

/** Runs a bash snippet at the repo root with the scripts' helpers loaded. */
export function sh(script: string, env: NodeJS.ProcessEnv = {}): string {
  const prelude = `set -euo pipefail; root=${JSON.stringify(root)}; compose=(docker compose -f "$root/docker-compose.yml"); source "$root/scripts/lib/volume.sh";`;
  return execFileSync("bash", ["-c", prelude + script], {
    cwd: root,
    env: { ...process.env, ...env },
    encoding: "utf8",
    maxBuffer: 64 << 20,
  }).trim();
}

export function sql(query: string): string {
  return execFileSync("docker", [...compose, "exec", "-T", "postgres", "psql", "-U", "objectstore", "-d", "objectstore", "-tA", "-F", " ", "-c", query], {
    encoding: "utf8",
  }).trim();
}

export function osctl(args: string[], env: NodeJS.ProcessEnv = {}): string {
  return execFileSync(path.join(root, "bin", "osctl"), args, { cwd: root, env: { ...process.env, ...env }, encoding: "utf8" }).trim();
}

export function sha256(file: string): string {
  return createHash("sha256").update(readFileSync(file)).digest("hex");
}

export function randomFile(dir: string, name: string, size: number): string {
  const file = path.join(dir, name);
  writeFileSync(file, size === 0 ? Buffer.alloc(0) : randomBytes(size));
  return file;
}

/** Durations from the coordinator's Compose environment, in milliseconds. */
export function timings() {
  const config = JSON.parse(execFileSync("docker", [...compose, "config", "--format", "json"], { encoding: "utf8" })) as {
    services: Record<string, { environment?: Record<string, string> }>;
  };
  const env = config.services.coordinator?.environment ?? {};
  const ms = (name: string): number => {
    const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(env[name] ?? "");
    if (m === null) {
      throw new Error(`${name} is not a duration: ${env[name]}`);
    }
    const unit = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as "ms" | "s" | "m" | "h"];
    return Number(m[1]) * unit;
  };
  return {
    suspectAfter: ms("COORDINATOR_SUSPECT_AFTER"),
    deadAfter: ms("COORDINATOR_DEAD_AFTER"),
    repairGrace: ms("COORDINATOR_REPAIR_GRACE"),
    repairInterval: ms("COORDINATOR_REPAIR_INTERVAL"),
  };
}

/** The UI polls every 2 s; allow a few polls plus scheduling noise. */
export const POLL_MARGIN = 10_000;

/** Starts demo.sh; resolves with its exit code, keeping its output. */
export function runDemo(pauseSeconds: number) {
  const child = spawn("bash", [path.join(root, "scripts", "demo.sh")], {
    cwd: root,
    env: { ...process.env, DEMO_PAUSE: String(pauseSeconds), NO_COLOR: "1" },
  });
  let output = "";
  child.stdout.on("data", (d: Buffer) => (output += d.toString()));
  child.stderr.on("data", (d: Buffer) => (output += d.toString()));
  const exited = new Promise<number>((resolve) => child.on("close", (code) => resolve(code ?? -1)));
  return { exited, output: () => output, kill: () => child.kill("SIGINT") };
}
