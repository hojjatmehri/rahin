// ============================================================
// File: src/core/RahinJobScheduler.js
// Purpose: Smart time-based job orchestrator with cron + queue
// Author:  Hojjat Mehri (Stable v2 - Advanced)
// ============================================================

import fs from "fs";
import path from "path";
import moment from "moment-timezone";
import cron from "node-cron";
import { fileURLToPath, pathToFileURL } from "url";
import { withDbRetry } from "file:///E:/Projects/AtighgashtAI/lib/db/dbRetryQueue.js";
import { acquireGlobalLock, releaseGlobalLock } from "file:///E:/Projects/rahin/src/lib/db/jobLock.js";

const TZ = "Asia/Tehran";
const MOD = "[RahinJobScheduler]";

export class RahinJobScheduler {
  constructor(configPath) {
    this.configPath = configPath;
    this.jobQueue = [];
    this.jobMap = {};
    this.isRunning = false;
    this.loadJobs(configPath);
  }

  // ------------------------------------------------------------
  // بارگذاری پیکربندی از فایل JSON
  // ------------------------------------------------------------
  loadJobs(configPath) {
    if (!fs.existsSync(configPath)) {
      console.error(`${MOD} ❌ Config file not found: ${configPath}`);
      return;
    }

    const raw = fs.readFileSync(configPath, "utf8");
    const jobs = JSON.parse(raw).filter(j => j.enabled);

    for (const job of jobs) {
      this.jobMap[job.name] = {
        ...job,
        status: "idle",
        lastRun: null,
        nextRun: null,
        attempts: 0,
      };
      if (job.cron) this.registerCron(job);
      else this.enqueue(job);
    }

    console.log(`${MOD} ✅ Loaded ${jobs.length} active jobs from config.`);
  }

  // ------------------------------------------------------------
  // ثبت زمان‌بندی بر اساس cron
  // ------------------------------------------------------------
  registerCron(job) {
    console.log(`${MOD} ⏰ Cron registered for ${job.name}: ${job.cron}`);
    cron.schedule(
      job.cron,
      () => this.enqueue(job),
      { timezone: TZ }
    );
  }

  // ------------------------------------------------------------
  // افزودن Job به صف اجرای ترتیبی
  // ------------------------------------------------------------
  enqueue(job) {
    if (this.jobQueue.find(j => j.name === job.name)) return;
    this.jobQueue.push(job);
    console.log(`${MOD} ➕ Enqueued: ${job.name}`);
    this.runNext();
  }

  // ------------------------------------------------------------
  // اجرای بعدی در صف (اگر در حال اجرا نیست)
  // ------------------------------------------------------------
  async runNext() {
    if (this.isRunning || this.jobQueue.length === 0) return;

    const job = this.jobQueue.shift();
    this.isRunning = true;
    await this.runJob(job);
    this.isRunning = false;

    // فاصلهٔ بین Jobها
    const gap = job.delayAfterMin || 5;
    console.log(`${MOD} 🕓 Waiting ${gap} min before next job...`);
    setTimeout(() => this.runNext(), gap * 60 * 1000);
  }

  // ------------------------------------------------------------
  // اجرای تکی Job با retry و قفل
  // ------------------------------------------------------------
  async runJob(job) {
    const name = job.name;
    const script = job.script;
    const logFile = job.logFile || `E:/Projects/rahin/logs/${name}.log`;
    const startTime = moment().tz(TZ).format("YYYY-MM-DD HH:mm:ss");

    this.jobMap[name].status = "running";
    this.jobMap[name].lastRun = startTime;
    this.jobMap[name].attempts++;

    fs.appendFileSync(logFile, `\n\n=== ${startTime} START ${name} ===\n`);

    console.log(`${MOD} 🚀 Running job: ${name}`);

    if (!acquireGlobalLock(name)) {
      console.warn(`${MOD} 🔒 Skipped due to global lock: ${name}`);
      fs.appendFileSync(logFile, `⚠️ Skipped due to global lock.\n`);
      this.jobMap[name].status = "skipped";
      return;
    }
    

    try {
        const moduleUrl = pathToFileURL(path.resolve(script)).href;

        let mod;
        try {
          mod = await import(moduleUrl);
        } catch (e) {
          console.error(`${MOD} ❌ Failed to import module for ${name}:`, e.message);
          throw e;
        }
        
        const fn =
          mod.main ||
          mod.default?.main ||
          (typeof mod.default === "function" ? mod.default : null);
        
        if (typeof fn !== "function") {
          throw new Error(`No valid main() export found in ${name}`);
        }
        
      await withDbRetry(fn, {
        jobName: name,
        retries: job.retries || 10,
        initialDelayMs: 1500,
        backoffFactor: 1.6
      });

      this.jobMap[name].status = "success";
      const doneAt = moment().tz(TZ).format("YYYY-MM-DD HH:mm:ss");
      fs.appendFileSync(logFile, `✅ Completed at ${doneAt}\n`);
      console.log(`${MOD} ✅ Completed: ${name}`);
    } catch (e) {
      this.jobMap[name].status = "failed";
      fs.appendFileSync(logFile, `❌ Error: ${e.message}\n`);
      console.error(`${MOD} ❌ ${name} failed:`, e.message);

      // Retry با backoff در سطح Scheduler
      if ((job.retries || 3) > 0) {
        const retryDelay = (job.retryDelayMin || 10) * 60 * 1000;
        console.log(`${MOD} 🔁 Requeueing ${name} after ${job.retryDelayMin || 10} min...`);
        setTimeout(() => this.enqueue(job), retryDelay);
      }
    } finally {
      releaseGlobalLock(name);
    }
  }

  // ------------------------------------------------------------
  // شروع Scheduler
  // ------------------------------------------------------------
  start() {
    console.log(`${MOD} 🔁 Scheduler started (${moment().tz(TZ).format("YYYY-MM-DD HH:mm")})`);
    this.runNext();
  }
}
