#!/usr/bin/env node
/**
 * AssemblyAI транскрибация с speaker diarization.
 *
 * Usage:
 *   node transcribe.mjs --input "C:/path/to/file.mp4"
 *   node transcribe.mjs --input "audio.mp3" --language ru
 *   node transcribe.mjs --input "video.mkv" --no-speakers
 *   node transcribe.mjs --input "zoom.mp4" --summary
 *
 * Флаги:
 *   --input <path>       путь к файлу (mp3/wav/m4a/flac/ogg/opus/mp4/mov/mkv/avi/webm/m4v)
 *   --language <code>    ru | en | auto (default: auto = language_detection)
 *   --no-speakers        выключить speaker diarization (по умолчанию ВКЛ)
 *   --summary            запросить LLM-summary + chapters от AssemblyAI
 *   --keep-audio         не удалять временный mp3 после обработки
 *
 * Выход:
 *   C:/AI-projects/транскрибация видео-аудио в txt файлы/_raw_<метка>_<baseName>.txt   — читаемый текст
 *   C:/AI-projects/транскрибация видео-аудио в txt файлы/_raw_<метка>_<baseName>.json  — полный JSON-ответ AssemblyAI
 */

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';

// .env подгружаем сами (чистый Node, без зависимостей): берём ключи из C:/AI-projects/.env,
// если их ещё нет в окружении. Так ученику не нужно вручную «подгружать переменные».
loadDotEnv('C:/AI-projects/.env');
function loadDotEnv(envPath) {
  try {
    for (const line of fs.readFileSync(envPath, 'utf-8').split(/\r?\n/)) {
      const m = line.match(/^\s*([A-Za-z0-9_]+)\s*=\s*(.*?)\s*$/);
      if (m && process.env[m[1]] === undefined) {
        process.env[m[1]] = m[2].replace(/^['"]|['"]$/g, '');
      }
    }
  } catch { /* нет .env — работаем по тому, что уже есть в окружении */ }
}

const API_KEY = process.env.ASSEMBLYAI_API_KEY;
if (!API_KEY) {
  console.error('❌ ASSEMBLYAI_API_KEY не найден. Впиши его в файл C:/AI-projects/.env (строка ASSEMBLYAI_API_KEY=...).');
  process.exit(1);
}

const FFMPEG = process.env.FFMPEG_PATH || 'ffmpeg';
const VIDEO_EXT = new Set(['.mp4', '.mov', '.mkv', '.avi', '.webm', '.m4v', '.flv', '.wmv']);
const AUDIO_EXT = new Set(['.mp3', '.wav', '.m4a', '.flac', '.ogg', '.opus', '.aac', '.wma']);

const args = parseArgs(process.argv.slice(2));
if (!args.input) {
  console.error('❌ Нужен --input <path>');
  process.exit(1);
}

const inputPath = path.resolve(args.input);
if (!fs.existsSync(inputPath)) {
  console.error(`❌ Файл не найден: ${inputPath}`);
  process.exit(1);
}

const ext = path.extname(inputPath).toLowerCase();
const baseName = path.basename(inputPath, ext);
const isVideo = VIDEO_EXT.has(ext);
const isAudio = AUDIO_EXT.has(ext);
if (!isVideo && !isAudio) {
  console.error(`❌ Неизвестное расширение ${ext}. Поддерживаются: ${[...VIDEO_EXT, ...AUDIO_EXT].join(', ')}`);
  process.exit(1);
}

run().catch((err) => {
  console.error(`❌ ${err.message}`);
  if (err.stack) console.error(err.stack);
  process.exit(1);
});

async function run() {
  let audioPath = inputPath;
  let tempAudio = null;

  if (isVideo) {
    tempAudio = path.join(os.tmpdir(), `transcribe-${Date.now()}-${baseName}.mp3`);
    console.log(`🎬 Извлекаю аудио из видео → ${tempAudio}`);
    await extractAudio(inputPath, tempAudio);
    audioPath = tempAudio;
    const stats = fs.statSync(tempAudio);
    console.log(`✅ Аудио готово (${(stats.size / 1024 / 1024).toFixed(1)} MB)`);
  }

  console.log(`⬆  Загружаю на AssemblyAI...`);
  const uploadUrl = await uploadFile(audioPath);
  console.log(`✅ Uploaded: ${uploadUrl}`);

  console.log(`📝 Создаю transcript...`);
  const transcriptId = await createTranscript(uploadUrl);
  console.log(`✅ ID: ${transcriptId}`);

  console.log(`⏳ Жду завершения (опрос каждые 10 сек)...`);
  const result = await pollTranscript(transcriptId);
  console.log(`✅ Transcript готов (${result.audio_duration}s, язык: ${result.language_code})`);

  const outDir = 'C:/AI-projects/транскрибация видео-аудио в txt файлы';
  fs.mkdirSync(outDir, { recursive: true });

  const tempBase = `_raw_${timestamp()}_${sanitize(baseName).slice(0, 60)}`;
  const txtPath = path.join(outDir, `${tempBase}.txt`);
  const jsonPath = path.join(outDir, `${tempBase}.json`);

  fs.writeFileSync(txtPath, formatText(result), 'utf-8');
  fs.writeFileSync(jsonPath, JSON.stringify(result, null, 2), 'utf-8');

  console.log(`\n📄 Текст (временное имя):  ${txtPath}`);
  console.log(`📊 JSON:                    ${jsonPath}`);
  console.log(`\n⚠  СЛЕДУЮЩИЙ ШАГ: Claude прочитает .txt, придумает содержательное название и переименует оба файла в формат "YYYY-MM-DD HH-MM — <описание>.{txt,json}"`);

  if (tempAudio && !args['keep-audio']) {
    fs.unlinkSync(tempAudio);
  }
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith('--')) {
        out[key] = next;
        i++;
      } else {
        out[key] = true;
      }
    }
  }
  return out;
}

function extractAudio(input, output) {
  return new Promise((resolve, reject) => {
    const proc = spawn(FFMPEG, [
      '-y',
      '-i', input,
      '-vn',
      '-acodec', 'libmp3lame',
      '-ar', '16000',
      '-ac', '1',
      '-b:a', '64k',
      output,
    ], { stdio: ['ignore', 'ignore', 'pipe'] });
    let err = '';
    proc.stderr.on('data', (d) => { err += d.toString(); });
    proc.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exit ${code}: ${err.slice(-500)}`));
    });
  });
}

async function uploadFile(filePath) {
  const stream = fs.createReadStream(filePath);
  const stats = fs.statSync(filePath);

  const res = await withRetry(() => fetch('https://api.assemblyai.com/v2/upload', {
    method: 'POST',
    headers: {
      authorization: API_KEY,
      'content-type': 'application/octet-stream',
      'content-length': stats.size.toString(),
    },
    body: Readable.toWeb(stream),
    duplex: 'half',
  }));

  if (!res.ok) throw new Error(`upload failed ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.upload_url;
}

async function createTranscript(audioUrl) {
  const body = {
    audio_url: audioUrl,
    speech_models: ['universal-2'],
    speaker_labels: !args['no-speakers'],
    punctuate: true,
    format_text: true,
  };

  if (args.language && args.language !== 'auto') {
    body.language_code = args.language;
  } else {
    body.language_detection = true;
  }

  if (args.summary) {
    body.summarization = true;
    body.summary_model = 'informative';
    body.summary_type = 'bullets';
    body.auto_chapters = true;
  }

  const res = await withRetry(() => fetch('https://api.assemblyai.com/v2/transcript', {
    method: 'POST',
    headers: {
      authorization: API_KEY,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  }));
  if (!res.ok) throw new Error(`create transcript failed ${res.status}: ${await res.text()}`);
  const data = await res.json();
  return data.id;
}

async function pollTranscript(id) {
  const url = `https://api.assemblyai.com/v2/transcript/${id}`;
  while (true) {
    const res = await withRetry(() => fetch(url, { headers: { authorization: API_KEY } }));
    if (!res.ok) throw new Error(`poll failed ${res.status}: ${await res.text()}`);
    const data = await res.json();
    if (data.status === 'completed') return data;
    if (data.status === 'error') throw new Error(`AssemblyAI error: ${data.error}`);
    process.stdout.write('.');
    await sleep(10000);
  }
}

function formatText(result) {
  const lines = [];
  lines.push(`# Транскрибация`);
  lines.push(`Длительность: ${formatTime(result.audio_duration)}`);
  lines.push(`Язык: ${result.language_code || 'unknown'}`);
  lines.push(`Spoken words: ${result.words?.length || 0}`);
  lines.push('');

  if (result.summary) {
    lines.push(`## Summary`);
    lines.push(result.summary);
    lines.push('');
  }

  if (result.chapters?.length) {
    lines.push(`## Chapters`);
    for (const ch of result.chapters) {
      lines.push(`[${formatTime(ch.start / 1000)}] ${ch.headline}`);
      if (ch.summary) lines.push(`  ${ch.summary}`);
    }
    lines.push('');
  }

  lines.push(`## Текст`);
  lines.push('');

  if (result.utterances?.length) {
    for (const u of result.utterances) {
      lines.push(`[${formatTime(u.start / 1000)}] Speaker ${u.speaker}:`);
      lines.push(u.text);
      lines.push('');
    }
  } else {
    lines.push(result.text || '(пусто)');
  }

  return lines.join('\n');
}

function formatTime(sec) {
  sec = Math.floor(sec);
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h) return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  return `${m}:${String(s).padStart(2, '0')}`;
}

function today() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function timestamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

function sanitize(s) {
  return s.replace(/[<>:"/\\|?*\x00-\x1f]/g, '').replace(/\s+/g, ' ').trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function withRetry(fn, { maxAttempts = 3, baseDelay = 2000 } = {}) {
  let lastErr;
  for (let i = 0; i < maxAttempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const status = err.status;
      const retriable = !status || status === 429 || (status >= 500 && status < 600);
      if (!retriable) throw err;
      if (i === maxAttempts - 1) break;
      const delay = baseDelay * Math.pow(2, i);
      console.warn(`\n⚠  Попытка ${i + 1} неудачна (${err.message}), retry через ${delay}ms`);
      await sleep(delay);
    }
  }
  throw lastErr;
}
