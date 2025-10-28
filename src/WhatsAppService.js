// ============================================================
// File: src/WhatsAppService.js
// Purpose: Wrapper for UltraMsg WhatsApp API (Singleton instance)
// Author: Hojjat Mehri
// ============================================================

import '../logger.js';
import axios from 'axios';
import qs from 'qs';
import fs from 'fs';
import path from 'path';
import moment from 'moment-timezone';

// ---------- تنظیمات عمومی ----------
const MOD = '[WhatsAppService]';
const TZ = 'Asia/Tehran';
const log = (...a) => console.log(MOD, moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss'), '|', ...a);
const err = (...a) => console.error(MOD, moment().tz(TZ).format('YYYY-MM-DD HH:mm:ss'), '|', ...a);

// ---------- نرمال‌سازی شماره ----------
function normalizeRecipient(to = '') {
  const s = String(to).trim();
  if (/@g\.us$/i.test(s)) return s; // گروه
  const digits = s.replace(/[^\d]/g, '');
  if (!digits) return s;
  if (digits.startsWith('98')) return digits;
  if (digits.startsWith('0')) return '98' + digits.slice(1);
  if (digits.startsWith('9')) return '98' + digits;
  return digits;
}

// ============================================================
// کلاس اصلی
// ============================================================
class WhatsAppService {
  /**
   * @param {string} ultramsgInstance
   * @param {string} ultramsgToken
   * @param {string} baseUrl (اختیاری)
   */
  constructor(ultramsgInstance, ultramsgToken, baseUrl = 'https://api.ultramsg.com') {
    if (!ultramsgInstance || !ultramsgToken) {
      throw new Error('WhatsAppService: instanceId و token الزامی‌اند');
    }

    this.instance = ultramsgInstance;
    this.token = ultramsgToken;
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.http = axios.create({
      baseURL: `${this.baseUrl}/${this.instance}`,
      timeout: 15000,
      validateStatus: s => s >= 200 && s < 300
    });

    log(`🔌 UltraMsg Service initialized for instance: ${this.instance}`);
  }

  // ---------- Contacts ----------
  async blockNumber(chatId) {
    const payload = qs.stringify({ token: this.token, chatId });
    const { data } = await this.http.post('/contacts/block', payload, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    });
    return data;
  }

  // ---------- Messages ----------
  async sendMessage(to, body) {
    const dest = normalizeRecipient(to);
    if (!body) {
      log(`⚠️ پیام خالی برای ${dest} ارسال نشد.`);
      return { skipped: true };
    }
    log(`📤 ارسال پیام به ${dest}`);
    return this._sendForm('/messages/chat', { to: dest, body });
  }

  async sendImage(to, imageUrl, caption = '') {
    return this._sendForm('/messages/image', { to: normalizeRecipient(to), image: imageUrl, caption });
  }

  async sendImageBase64(to, imagePathOrUrl, caption = '') {
    let imagePayload = imagePathOrUrl;
    if (fs.existsSync(imagePathOrUrl)) {
      const ext = (path.extname(imagePathOrUrl).toLowerCase().replace('.', '') || 'jpeg');
      const buf = fs.readFileSync(imagePathOrUrl);
      imagePayload = `data:image/${ext};base64,${buf.toString('base64')}`;
    }
    return this._sendForm('/messages/image', { to: normalizeRecipient(to), image: imagePayload, caption });
  }

  async sendFile(to, filename, documentUrl, caption = '') {
    return this._sendForm('/messages/document', {
      to: normalizeRecipient(to),
      filename,
      document: documentUrl,
      caption
    });
  }

  async sendLocation(to, address, lat, lng) {
    return this._sendForm('/messages/location', {
      to: normalizeRecipient(to),
      address, lat, lng
    });
  }

  // ---------- Low-level helper ----------
  async _sendForm(endpoint, obj) {
    if (!obj?.to) throw new Error('destination (to) is required');

    const payload = qs.stringify({ token: this.token, ...obj });
    try {
      const { data } = await this.http.post(endpoint, payload, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      });

      if (data?.id || data?.sent) log(`✅ ارسال موفق (${endpoint}) → message_id=${data.id || '?'} برای ${obj.to}`);
      else log(`⚠️ پاسخ UltraMsg غیرمنتظره بود →`, data);

      return data;
    } catch (e) {
      const res = e.response;
      const detail = res?.data || res?.statusText || e.message;
      err(`❌ خطا در ارسال (${endpoint}) → ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`);
      throw e;
    }
  }
}

// ============================================================
// ایجاد singleton برای استفاده مستقیم
// ============================================================
const instanceId = process.env.ULTRAMSG_INSTANCE_ID;
const token = process.env.ULTRAMSG_TOKEN;

let instance = null;
try {
  if (instanceId && token) {
    instance = new WhatsAppService(instanceId, token);
  } else {
    err('⚠️ مقادیر ULTRAMSG_INSTANCE_ID یا ULTRAMSG_TOKEN تنظیم نشده‌اند.');
  }
} catch (e) {
  err('❌ WhatsAppService init failed:', e.message);
}

// ============================================================
// خروجی نهایی برای استفاده مستقیم (instance یا mock)
// ============================================================
const WhatsAppSingleton = instance || {
  sendMessage: async (to, msg) => {
    log(`(MOCK) پیام به ${to}:\n${msg}`);
    return { mocked: true };
  }
};

export default WhatsAppSingleton;
