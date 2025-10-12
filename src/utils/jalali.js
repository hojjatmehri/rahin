// src/utils/jalali.js
import moment from "moment-timezone";
import jalaali from "jalaali-js";

const TZ = "Asia/Tehran";

/** تبدیل تاریخ میلادی (Tehran) به {jy, jm, jd} جلالی */
export function toJalaliParts(date) {
  const d = moment.tz(date, TZ).toDate();
  const gy = d.getFullYear(), gm = d.getMonth() + 1, gd = d.getDate();
  return jalaali.toJalaali(gy, gm, gd);
}

/** شروع ماه جلالی جاری در Tehran → ISO */
export function startOfCurrentJMonthISO() {
  const now = moment.tz(TZ);
  const { jy, jm } = toJalaliParts(now);
  const g = jalaali.toGregorian(jy, jm, 1);
  return moment.tz({ year: g.gy, month: g.gm - 1, day: g.gd, hour: 0, minute: 0, second: 0 }, TZ).toISOString();
}

/** شروع ماه جلالی قبلی در Tehran → ISO */
export function startOfPrevJMonthISO() {
  const now = moment.tz(TZ);
  const { jy, jm } = toJalaliParts(now);
  const pY = jm === 1 ? (jy - 1) : jy;
  const pM = jm === 1 ? 12 : (jm - 1);
  const g = jalaali.toGregorian(pY, pM, 1);
  return moment.tz({ year: g.gy, month: g.gm - 1, day: g.gd, hour: 0, minute: 0, second: 0 }, TZ).toISOString();
}

/** اکنون Tehran به ISO */
export function nowTehISO() {
  return moment.tz(TZ).toISOString();
}

/** نقطهٔ «روز جاریِ ماه قبل» (PrevMonth-to-Date) در تهران → ISO */
export function prevMonthToNowISO() {
  const now = moment.tz(TZ);
  const { jy, jm, jd } = toJalaliParts(now);
  const pY = jm === 1 ? (jy - 1) : jy;
  const pM = jm === 1 ? 12 : (jm - 1);

  // اگر ماه قبل کوتاه‌تر از jd فعلی بود، به آخر ماه قبل clamp می‌کنیم
  const monthLen = jalaali.jalaaliMonthLength(pY, pM);
  const d = Math.min(jd, monthLen);

  const gStart = jalaali.toGregorian(pY, pM, 1);
  const gNowEq = jalaali.toGregorian(pY, pM, d);

  const startPrev = moment.tz({ year: gStart.gy, month: gStart.gm - 1, day: gStart.gd, hour: 0 }, TZ).toISOString();
  const endPrevEqNow = moment.tz({ year: gNowEq.gy, month: gNowEq.gm - 1, day: gNowEq.gd, hour: now.hour(), minute: now.minute(), second: now.second() }, TZ).toISOString();
  return { startPrev, endPrevEqNow };
}
