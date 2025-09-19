import {
  Client,
  GatewayIntentBits,
  TextChannel,
  ChannelType,
  EmbedBuilder,
  Message,
  PermissionFlagsBits,
} from "discord.js";
import { spawn, ChildProcessWithoutNullStreams } from "node:child_process";
import * as dotenv from "dotenv";
import * as fs from "node:fs";
import * as path from "node:path";
import { Rcon } from "rcon-client";

dotenv.config();

const DISCORD_TOKEN = mustEnv("DISCORD_TOKEN");
const GUILD_ID = mustEnv("DISCORD_GUILD_ID");
const CHANNEL_ID = mustEnv("DISCORD_CHANNEL_ID");

const RUN_SCRIPT: string = process.env.RUN_SCRIPT ?? "./run.sh";
const MC_WORKDIR: string = process.env.MC_WORKDIR ?? process.cwd();

const BATCH_INTERVAL_MS = toInt(process.env.BATCH_INTERVAL_MS, 3000);
const MAX_LINES_PER_BATCH = toInt(process.env.MAX_LINES_PER_BATCH, 40);

const DISCORD_HARD_LIMIT = 2000;
const SAFETY_HEADROOM = 10;
const MAX_CONTENT = DISCORD_HARD_LIMIT - SAFETY_HEADROOM;

const NO_RESTART_FILE = process.env.NO_RESTART_FILE ?? ".norestart";
const STOP_GRACE_MS = toInt(process.env.STOP_GRACE_MS, 10000);

const RCON_ENABLED =
  (process.env.RCON_ENABLED ?? "false").toLowerCase() === "true";
const RCON_HOST = process.env.RCON_HOST ?? "127.0.0.1";
const RCON_PORT = toInt(process.env.RCON_PORT, 25575);
const RCON_PASSWORD = process.env.RCON_PASSWORD ?? "";
const RCON_PREFIX = process.env.RCON_PREFIX ?? "!rcon ";
const RCON_CONNECT_TIMEOUT_MS = toInt(
  process.env.RCON_CONNECT_TIMEOUT_MS,
  5000
);
const RCON_ALLOWED_ROLE_ID = process.env.RCON_ALLOWED_ROLE_ID ?? "";
const RCON_ALLOWED_USER_IDS = (process.env.RCON_ALLOWED_USER_IDS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const COLORS = {
  blue: 0x3498db,
  green: 0x2ecc71,
  yellow: 0xf1c40f,
  red: 0xe74c3c,
} as const;

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
});

let channel: TextChannel | null = null;
let server: ChildProcessWithoutNullStreams | null = null;
let queue: string[] = [];
let flushTimer: NodeJS.Timeout | null = null;
let shuttingDown = false;

function mustEnv(name: string): string {
  const v = process.env[name];
  if (!v) {
    console.error(`[FATAL] Missing env var ${name}`);
    process.exit(1);
  }
  return v;
}
function toInt(s: string | undefined, dflt: number): number {
  const n = Number(s);
  return Number.isFinite(n) ? n : dflt;
}
function sanitizeLine(line: string): string {
  return line.replaceAll("@", "@\u200B").replaceAll("```", "`\u200B``");
}
function splitToChunks(s: string, max: number): string[] {
  const out: string[] = [];
  for (let i = 0; i < s.length; i += max) out.push(s.slice(i, i + max));
  return out;
}
async function sendEmbed(desc: string, color: number): Promise<void> {
  if (!channel) return;
  const embed = new EmbedBuilder().setDescription(desc).setColor(color);
  try {
    await channel.send({ embeds: [embed] });
  } catch (err) {
    console.error("[Discord] embed send failed:", err);
  }
}

function enqueue(line: string): void {
  queue.push(sanitizeLine(line));
  startFlushTimer();
}

async function flushQueue(force = false): Promise<void> {
  if (!channel || queue.length === 0) {
    if (force && flushTimer) {
      clearInterval(flushTimer);
      flushTimer = null;
    }
    return;
  }

  const sendPacked = async (lines: string[]): Promise<void> => {
    if (lines.length === 0) return;
    const content = lines.join("\n");
    try {
      await channel!.send({ content });
    } catch (err) {
      console.error("[Discord] send failed:", err);
    }
  };

  let current: string[] = [];
  let currentLen = 0;

  const flushCurrent = async () => {
    if (current.length > 0) await sendPacked(current);
    current = [];
    currentLen = 0;
  };

  while (queue.length > 0) {
    const raw = queue.shift();
    if (raw == null) continue;

    if (raw.length > MAX_CONTENT) {
      const chunks = splitToChunks(raw, MAX_CONTENT);
      if (current.length > 0) await flushCurrent();
      for (let i = 0; i < chunks.length; i++) {
        const suffix = chunks.length > 1 ? ` [${i + 1}/${chunks.length}]` : "";
        await sendPacked([chunks[i] + suffix]);
      }
      continue;
    }

    const addLen = (current.length === 0 ? 0 : 1) + raw.length;
    const wouldOverflowChars = currentLen + addLen > MAX_CONTENT;
    const wouldOverflowLines = current.length + 1 > MAX_LINES_PER_BATCH;

    if (wouldOverflowChars || wouldOverflowLines) {
      await flushCurrent();
    }

    current.push(raw);
    currentLen += addLen;
  }

  await flushCurrent();

  if (force && flushTimer) {
    clearInterval(flushTimer);
    flushTimer = null;
  }
}

function startFlushTimer(): void {
  if (flushTimer) return;
  flushTimer = setInterval(() => void flushQueue(false), BATCH_INTERVAL_MS);
}

class RconManager {
  private rcon: Rcon | null = null;
  private connected = false;
  private connecting = false;

  async ensureConnected(): Promise<void> {
    if (!RCON_ENABLED) throw new Error("RCON is disabled");
    if (this.connected && this.rcon) return;
    if (this.connecting) {
      await new Promise((r) => setTimeout(r, 250));
      if (this.connected && this.rcon) return;
    }
    this.connecting = true;
    try {
      this.rcon = await Rcon.connect({
        host: RCON_HOST,
        port: RCON_PORT,
        password: RCON_PASSWORD,
        timeout: RCON_CONNECT_TIMEOUT_MS,
      });
      this.connected = true;

      this.rcon.on("end", () => {
        this.connected = false;
        this.rcon = null;
      });
    } finally {
      this.connecting = false;
    }
  }

  async send(command: string): Promise<string> {
    await this.ensureConnected();
    if (!this.rcon) throw new Error("RCON not connected");
    try {
      const res = await this.rcon.send(command);
      return typeof res === "string" ? res : String(res);
    } catch (e) {
      this.connected = false;
      this.rcon = null;
      throw e;
    }
  }

  async close(): Promise<void> {
    try {
      await this.rcon?.end();
    } catch {}
    this.rcon = null;
    this.connected = false;
  }
}

const rconManager = new RconManager();

async function ensureChannel(): Promise<TextChannel> {
  const guild = await client.guilds.fetch(GUILD_ID);
  if (!guild) throw new Error("Guild not found");

  const ch = await guild.channels.fetch(CHANNEL_ID);
  if (!ch || ch.type !== ChannelType.GuildText) {
    throw new Error(
      "Provided DISCORD_CHANNEL_ID is not a text channel or not accessible."
    );
  }
  return ch as TextChannel;
}

function startServer(): void {
  if (!fs.existsSync(MC_WORKDIR)) {
    console.error(`[FATAL] MC_WORKDIR does not exist: ${MC_WORKDIR}`);
    process.exit(1);
  }

  const scriptPath = path.resolve(MC_WORKDIR, RUN_SCRIPT);
  if (!fs.existsSync(scriptPath)) {
    console.error(`[FATAL] run.sh not found at ${scriptPath}`);
    process.exit(1);
  }

  console.log(`[MC] Starting script: ${scriptPath}`);
  server = spawn("bash", [scriptPath], {
    cwd: MC_WORKDIR,
    env: process.env,
  });

  const push = (prefix: string, data: Buffer) => {
    data
      .toString("utf8")
      .split(/\r?\n/)
      .forEach((line) => {
        if (!line.trim()) return;
        enqueue(`${prefix} ${line}`);
      });
  };

  server.stdout.on("data", (d: Buffer) => push("[OUT]", d));
  server.stderr.on("data", (d: Buffer) => push("[ERR]", d));

  server.on("spawn", () => {
    void sendEmbed(
      "**Minecraft server starting** — log streaming attached.",
      COLORS.green
    );
  });

  server.on("close", (code: number | null, signal: NodeJS.Signals | null) => {
    void sendEmbed(
      `**Minecraft server process ended** (code=\`${code}\`, signal=\`${
        signal ?? "null"
      }\`).`,
      COLORS.red
    );
    void flushQueue(true);
    server = null;
  });
}

process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  if (!server?.stdin.writable) return;
  server.stdin.write(chunk);
});

async function shutdown(): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log("[SYS] Shutting down…");
  await sendEmbed("**Logger shutting down…**", COLORS.yellow);

  try {
    const flagPath = path.join(MC_WORKDIR, NO_RESTART_FILE);
    fs.writeFileSync(flagPath, "1", "utf8");
  } catch (e) {
    console.error("[SYS] Failed to write no-restart flag:", e);
  }

  try {
    if (server?.stdin.writable) {
      server.stdin.write("stop\n");
    }
  } catch (e) {
    console.error("[SYS] Failed to write 'stop' to server stdin:", e);
  }

  const closed = await new Promise<boolean>((resolve) => {
    let done = false;
    const onClose = () => {
      if (!done) {
        done = true;
        resolve(true);
      }
    };
    server?.once("close", onClose);
    setTimeout(() => {
      if (!done) resolve(false);
    }, STOP_GRACE_MS);
  });

  if (!closed) {
    try {
      server?.kill("SIGTERM");
    } catch {}
  }

  try {
    await flushQueue(true);
  } catch {}

  try {
    await rconManager.close();
  } catch {}

  try {
    await client.destroy();
  } catch {}

  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
process.on("unhandledRejection", (e) =>
  console.error("[SYS] Unhandled rejection:", e)
);

client.on("messageCreate", async (msg: Message) => {
  try {
    if (!RCON_ENABLED) return;
    if (msg.author.bot) return;
    if (msg.channelId !== CHANNEL_ID) return;

    const content = msg.content ?? "";
    if (!content.startsWith(RCON_PREFIX)) return;

    if (RCON_ALLOWED_USER_IDS.length > 0) {
      if (!RCON_ALLOWED_USER_IDS.includes(msg.author.id)) {
        await msg.reply("❌ You are not allowed to use RCON here.");
        return;
      }
    }
    if (RCON_ALLOWED_ROLE_ID) {
      const member = await msg.guild?.members
        .fetch(msg.author.id)
        .catch(() => null);
      const hasRole = member?.roles.cache.has(RCON_ALLOWED_ROLE_ID);
      if (!hasRole) {
        await msg.reply("❌ You are not allowed to use RCON here.");
        return;
      }
    }

    const command = content.slice(RCON_PREFIX.length).trim();
    if (!command) {
      await msg.reply(`ℹ️ Usage: \`${RCON_PREFIX}<minecraft command>\``);
      return;
    }

    if (
      "sendTyping" in msg.channel &&
      typeof msg.channel.sendTyping === "function"
    ) {
      await msg.channel.sendTyping();
    }

    const result = await rconManager.send(command);
    const reply = (result ?? "").toString().trim() || "✅ Executed (no output)";

    const chunks = splitToChunks("```" + reply + "```", DISCORD_HARD_LIMIT - 1);
    for (const c of chunks) {
      await msg.reply({ content: c });
    }
  } catch (err: any) {
    console.error("[RCON] Error:", err);
    try {
      await msg.reply(
        `❌ RCON error: \`${(err?.message ?? String(err)).slice(0, 1800)}\``
      );
    } catch {}
  }
});

(async () => {
  await client.login(DISCORD_TOKEN);
  channel = await ensureChannel();
  await sendEmbed("**Logger online** — launching Minecraft...", COLORS.blue);
  startServer();
})();
