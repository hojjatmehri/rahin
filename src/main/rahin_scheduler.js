import { RahinJobScheduler } from "../core/RahinJobScheduler.js";

const scheduler = new RahinJobScheduler("E:/Projects/rahin/src/config/jobs.json");
scheduler.start();
// اجرای فوری همه jobها برای تست
for (const job of Object.values(scheduler.jobMap)) {
    scheduler.enqueue(job);
  }
  