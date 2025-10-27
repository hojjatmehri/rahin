import '../logger.js';
// src/WhatsAppService.js
import axios from 'axios';
import qs from 'qs';
import fs from 'fs';
import path from 'path';

function normalizeRecipient(to = '') {
  const s = String(to).trim();
  if (/@g\.us$/i.test(s)) return s;        // گروه
  const digits = s.replace(/[^\d]/g, '');
  if (!digits) return s;
  if (digits.startsWith('98')) return digits;
  if (digits.startsWith('0'))  return '98' + digits.slice(1);
  if (digits.startsWith('9'))  return '98' + digits;
  return digits;
}

export default class WhatsAppService {
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
    return this._sendForm('/messages/chat', { to: normalizeRecipient(to), body });
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
    if ('body' in obj && !obj.body) return { skipped: true, reason: 'empty body' };

    const payload = qs.stringify({ token: this.token, ...obj });
    try {
      const { data } = await this.http.post(endpoint, payload, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
      });
      // console.log('✅', endpoint, data);
      return data;
    } catch (err) {
      const res = err.response;
      const detail = res?.data || res?.statusText || err.message;
      // console.error('❌', endpoint, detail);
      throw new Error(`UltraMSG ${endpoint} failed: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`);
    }
  }
}

