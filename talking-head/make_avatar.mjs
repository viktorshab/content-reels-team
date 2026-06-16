// make_avatar.mjs — Говорящая голова (финальная добивка контент-контура М6)
//
// Что делает: берёт чистый текст под озвучку (из выбранного сценария) → HeyGen API v2
// (русский голос + аватар) → видео 9:16 → materials/M6/m6_avatar_reel.mp4.
//
// Откуда берёт текст (любой из вариантов):
//   node make_avatar.mjs --text-file materials/M6/avatar_text.txt
//   node make_avatar.mjs --text "Ты не деревянный. У тебя просто зажаты не те мышцы..."
//   (без аргументов берёт materials/M6/avatar_text.txt)
//
// Ключ HEYGEN_API_KEY — из окружения или из .env (./.env, затем C:/AI-projects/.env).
// Кошелёк HeyGen API отдельный от web-кредитов (минимум ~$5).
// По умолчанию делаем БЕСПЛАТНОЕ превью с watermark (кошелёк НЕ тратится).
// Чистовое видео (тратит кошелёк) — только если явно добавить флаг --final:
//   node make_avatar.mjs --text-file ... --final
//
// Можно запинить выбор: HEYGEN_VOICE_ID / HEYGEN_AVATAR_ID в окружении.

import fs from 'node:fs';
import path from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const API = 'https://api.heygen.com';
const OUT_DIR = 'materials/M6';
const OUT_MP4 = path.join(OUT_DIR, 'm6_avatar_reel.mp4');
// Режим (превью/чистовое) задаётся ниже по флагу --final; по умолчанию — бесплатное превью.

// ── текст под озвучку ──
function readArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--text-file') out.textFile = argv[++i];
    else if (argv[i] === '--text') out.text = argv[++i];
    else if (argv[i] === '--final') out.final = true;
  }
  return out;
}
const args = readArgs(process.argv.slice(2));
// Превью (бесплатно, watermark) по умолчанию; чистовое (тратит кошелёк) — только с флагом --final.
const TEST = !args.final;
let TEXT = args.text;
if (!TEXT) {
  const file = args.textFile || path.join(OUT_DIR, 'avatar_text.txt');
  try {
    TEXT = fs.readFileSync(file, 'utf-8').trim();
  } catch {
    console.error(`❌ Нет текста для озвучки. Дай его так: --text-file <путь> или --text "..."\n   (или положи текст в ${path.join(OUT_DIR, 'avatar_text.txt')})`);
    process.exit(1);
  }
}
if (!TEXT || TEXT.length < 10) { console.error('❌ Текст под озвучку пустой или слишком короткий.'); process.exit(1); }
if (TEXT.length > 4800) { console.error('❌ Текст длиннее лимита HeyGen (~5000 символов). Сократи.'); process.exit(1); }

// ── ключ ──
function readKey(name) {
  if (process.env[name]) return process.env[name];
  for (const p of ['.env', 'C:/AI-projects/.env']) {
    try {
      const m = fs.readFileSync(p, 'utf-8').match(new RegExp('^' + name + '=(.*)$', 'm'));
      if (m) return m[1].trim().replace(/^["']|["']$/g, '');
    } catch { /* нет файла — пробуем следующий */ }
  }
  return null;
}
const KEY = readKey('HEYGEN_API_KEY');
if (!KEY) {
  console.error('❌ HEYGEN_API_KEY не найден. Заведи ключ на developers.heygen.com (Settings → API),\n   пополни API-кошелёк (от $5) и добавь HEYGEN_API_KEY=... в .env.');
  process.exit(1);
}
const HEADERS = { 'X-Api-Key': KEY, 'Content-Type': 'application/json' };

async function withRetry(fn, label, max = 3) {
  let last;
  for (let i = 0; i < max; i++) {
    try { return await fn(); }
    catch (e) {
      last = e;
      const s = e.status;
      if (!(!s || s === 429 || (s >= 500 && s < 600)) || i === max - 1) break;
      const d = 2000 * 2 ** i;
      console.warn(`  ⚠ ${label}: попытка ${i + 1} (${e.message}), retry ${d}ms`);
      await new Promise(r => setTimeout(r, d));
    }
  }
  throw last;
}

async function api(method, urlPath, body) {
  const res = await fetch(API + urlPath, { method, headers: HEADERS, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok) { const t = await res.text(); const e = new Error(`${method} ${urlPath} → ${res.status}: ${t.slice(0, 300)}`); e.status = res.status; throw e; }
  return res.json();
}

function pickVoice(voices) {
  if (process.env.HEYGEN_VOICE_ID) return { voice_id: process.env.HEYGEN_VOICE_ID, name: '(pinned)' };
  const ru = voices.filter(v => /russian|русск/i.test(v.language || ''));
  const pool = ru.length ? ru : voices.filter(v => /multilingual/i.test((v.language || '') + (v.name || '')));
  if (!pool.length) return null;
  const female = pool.filter(v => /female|женск/i.test(v.gender || ''));
  console.log(`  русских голосов: ${ru.length}; беру ${(female[0] || pool[0]).voice_id}`);
  return female[0] || pool[0];
}
function pickAvatar(avatars) {
  if (process.env.HEYGEN_AVATAR_ID) return { avatar_id: process.env.HEYGEN_AVATAR_ID, avatar_name: '(pinned)' };
  const female = avatars.filter(a => /female|женск/i.test(a.gender || ''));
  const pool = female.length ? female : avatars;
  return pool[0];
}

async function main() {
  console.log('1) Списки голосов и аватаров…');
  const [vRes, aRes] = await Promise.all([
    withRetry(() => api('GET', '/v2/voices'), 'voices'),
    withRetry(() => api('GET', '/v2/avatars'), 'avatars'),
  ]);
  const voices = vRes.data?.voices || vRes.voices || [];
  const avatars = aRes.data?.avatars || aRes.avatars || [];
  const voice = pickVoice(voices);
  const avatar = pickAvatar(avatars);
  if (!voice) { console.error('❌ Не нашёл русский голос. Запинь HEYGEN_VOICE_ID.'); process.exit(1); }
  if (!avatar) { console.error('❌ Не нашёл аватар. Запинь HEYGEN_AVATAR_ID.'); process.exit(1); }
  console.log(`  голос: ${voice.voice_id} | аватар: ${avatar.avatar_id} (${avatar.avatar_name})`);

  console.log(TEST ? '2) Генерация ПРЕВЬЮ 9:16 (бесплатно, watermark; для чистового добавь --final)…' : '2) Генерация ЧИСТОВОГО видео 9:16 (тратит кошелёк HeyGen)…');
  const gen = await withRetry(() => api('POST', '/v2/video/generate', {
    test: TEST,
    caption: false,
    dimension: { width: 720, height: 1280 },
    video_inputs: [{
      character: { type: 'avatar', avatar_id: avatar.avatar_id, avatar_style: 'normal' },
      voice: { type: 'text', input_text: TEXT, voice_id: voice.voice_id, speed: 1.0 },
      background: { type: 'color', value: '#EFE9E1' },
    }],
  }), 'generate');
  const videoId = gen.data?.video_id || gen.video_id;
  if (!videoId) { console.error('❌ Нет video_id:', JSON.stringify(gen)); process.exit(1); }
  console.log(`  video_id: ${videoId}${TEST ? ' (TEST — watermark)' : ''}`);

  console.log('3) Жду готовности (каждые 15 сек)…');
  let url = null;
  for (let i = 0; i < 80; i++) {
    await new Promise(r => setTimeout(r, 15000));
    const st = await withRetry(() => api('GET', `/v1/video_status.get?video_id=${videoId}`), 'status');
    const d = st.data || st;
    console.log(`  [${i + 1}] ${d.status}`);
    if (d.status === 'completed') { url = d.video_url; break; }
    if (d.status === 'failed') { console.error('❌ failed:', JSON.stringify(d.error || d)); process.exit(1); }
  }
  if (!url) { console.error('❌ Не дождался за ~20 мин. video_id:', videoId); process.exit(1); }

  console.log('4) Скачиваю mp4…');
  const res = await withRetry(() => fetch(url), 'download');
  if (!res.ok) { console.error('❌ Скачивание:', res.status); process.exit(1); }
  fs.mkdirSync(OUT_DIR, { recursive: true });
  await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(OUT_MP4));
  const mb = (fs.statSync(OUT_MP4).size / 1024 / 1024).toFixed(1);
  console.log(`\n✅ Готово → ${OUT_MP4} (${mb} MB)`);
}

main().catch(e => { console.error(`❌ ${e.message}`); process.exit(1); });
