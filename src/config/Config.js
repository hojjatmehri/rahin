// src/config/Config.js
// ساخت سرویس‌های آماده با استفاده از env.js

import env from './env.js';
import Kavenegar from 'kavenegar';
import sqlite3 from 'sqlite3';
import WhatsAppService from '../WhatsAppService.js';
import OpenAI from 'openai';
import express from 'express';
import bodyParser from 'body-parser';

// ========================== OpenAI ==========================
export const openai = new OpenAI({ apiKey: env.OPENAI_API_KEY });

// ========================== WhatsApp ==========================
export const waService = new WhatsAppService(
    env.ULTRAMSG_INSTANCE_ID,
    env.ULTRAMSG_TOKEN
);

// ========================== Kavenegar ==========================
export const api = Kavenegar.KavenegarApi({
    apikey: env.KAVENEGAR_API_KEY
});

// ========================== SQLite ==========================
export const db = new sqlite3.Database(
    env.SQLITE_DB_PATH,
    sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
);

// ========================== Express ==========================
export const app = express();

app.use(
    bodyParser.json({
        verify: (req, res, buf) => {
            req.rawBody = buf.toString(); // ذخیره نسخه خام داده‌ها برای وبهوک
        },
    })
);

app.use(bodyParser.urlencoded({ extended: true }));

// ========================== Exports ==========================
export const config = {
    // Keys
    OPENAI_API_KEY: env.OPENAI_API_KEY,
    NOVINHUB_API_KEY: env.NOVINHUB_API_KEY,
    DIDAR_API_KEY: env.DIDAR_API_KEY,
    KAVENEGAR_API_KEY: env.KAVENEGAR_API_KEY,
    ULTRAMSG_INSTANCE_ID: env.ULTRAMSG_INSTANCE_ID,
    ULTRAMSG_TOKEN: env.ULTRAMSG_TOKEN,

    // Base URLs
    NOVINHUB_BASE_URL: env.NOVINHUB_BASE_URL,
    DIDAR_BASE_URL: env.DIDAR_BASE_URL,
    KAVENEGAR_BASE_URL: env.KAVENEGAR_BASE_URL,
    ULTRAMSG_BASE_URL: env.ULTRAMSG_BASE_URL,

    // WhatsApp
    WEBHOOK_SECRET: env.WEBHOOK_SECRET,
    WHATSAPP_GROUP_ID: env.WHATSAPP_GROUP_ID,
    WHATSAPP_DEST_MOBILE: env.WHATSAPP_DEST_MOBILE,

    // Database
    TABLE_NAME: env.TABLE_NAME,
    SQLITE_DB_PATH: env.SQLITE_DB_PATH,
    db,

    // Google Sheets
    GOOGLE_SHEET_ID: env.GOOGLE_SHEET_ID,
    GOOGLE_SHEET_AUTH: {
        email: env.GOOGLE_SHEET_AUTH_EMAIL,
        key: env.GOOGLE_SHEET_AUTH_KEY,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    },

    // Services
    api,
    waService,
    openai,

    // Express app
    app,
};
