// collect_reels.mjs — Сборщик рилсов (роль 1 контент-конвейера М6)
//
// Что делает:
//   1) гонит Apify-актор apify/instagram-scraper по 2-3 аккаунтам-референсам
//      (универсальный: принимает ссылки на профили/хэштеги);
//   2) сохраняет сырой JSON выгрузки (чтобы можно было перепроверить любую цифру);
//   3) отбирает топ роликов по просмотрам ВНУТРИ КАЖДОГО аккаунта (а не общим списком —
//      чтобы крупный аккаунт не вытеснил сильные ролики мелкого, как требует методика урока);
//   4) качает их видео по прямой ссылке videoUrl в materials/M6/reels_raw/<автор>_<id>.mp4
//      (важно: Apify сам mp4 на диск НЕ кладёт, он отдаёт только videoUrl);
//   5) собирает готовую таблицу статистики materials/M6/m6_reels_stats.md (выход роли 1);
//   6) печатает таблицу отобранных рилсов в консоль.
//
// Токены НЕ зашиты в код — читаются из C:/AI-projects/.env (APIFY_API_TOKENS / APIFY_API_TOKEN).
//
// Запуск:  node collect_reels.mjs
// Перед запуском:  npm i apify-client dotenv

import { config } from 'dotenv';
import { ApifyClient } from 'apify-client';
import { writeFile, mkdir } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

// .env лежит в корне C:/AI-projects — токены там, рядом с этим скриптом их нет
config({ path: 'C:/AI-projects/.env' });

// ─────────────────────────────────────────────────────────────────────────────
// ПАРАМЕТРЫ — здесь ученик правит под себя. Всё остальное трогать не нужно.
// ─────────────────────────────────────────────────────────────────────────────

// Аккаунты-референсы: 2-3 профиля в своей нише, чьи рилсы разбираем.
// Это ПЛЕЙСХОЛДЕРЫ под сквозную нишу М6 (онлайн-студия растяжки) — замени на реальные.
// Можно ссылку на профиль (https://instagram.com/<имя>/)
// или на хэштег ниши (https://instagram.com/explore/tags/<тег>/).
const PROFILES = [
  'https://instagram.com/<аккаунт-референс-по-растяжке-1>/',
  'https://instagram.com/<аккаунт-референс-по-растяжке-2>/',
  // 'https://instagram.com/explore/tags/растяжка/',  // вариант: хэштег ниши
];

// Можно не трогать список выше, а передать аккаунты прямо при запуске:
//   node collect_reels.mjs https://instagram.com/<имя>/  [ещё ссылки...]
// Если ссылки-аргументы переданы — берём их, иначе список PROFILES выше.
const argProfiles = process.argv.slice(2).filter((a) => a.startsWith('http'));
const SOURCES = argProfiles.length ? argProfiles : PROFILES;

// Сколько рилсов тянуть с каждого источника. 30-50 — чтобы было из чего выбирать.
const RESULTS_LIMIT = 40;

// Сколько лучших роликов брать в работу всего (скачать + расшифровать дальше).
// Отбираем поровну по аккаунтам (топ внутри каждого), чтобы крупный не вытеснил мелкого.
// Обычно 10-15; на запись демо хватает 10.
const TOP_N = 10;

// Actor ID — универсальный Instagram-скрейпер Apify (принимает directUrls: профили и хэштеги).
// Рилсы среди постов отбираются ниже по числу просмотров (videoPlayCount есть только у видео).
// Если Apify переименует актор и будет 404 — найди актуальный на apify.com/store.
const ACTOR_ID = 'apify/instagram-scraper';

// Куда складываем сырой JSON выгрузки (служебный адрес для перепроверки цифр).
const JSON_DIR = 'C:/AI-projects/_outputs/apify/instagram-scraper';

// Куда качаем видео отобранных роликов (рабочая папка ученика).
const RAW_DIR = 'materials/M6/reels_raw';

// Куда складываем готовую таблицу статистики — главный выход роли 1 для контент-аналитика.
const STATS_FILE = 'materials/M6/m6_reels_stats.md';

// ─────────────────────────────────────────────────────────────────────────────
// Токены: пул из .env, ротация на 401/402/429 (как в apify.md Виктора).
// ─────────────────────────────────────────────────────────────────────────────

const tokens = (process.env.APIFY_API_TOKENS || process.env.APIFY_API_TOKEN || '')
  .split(',')
  .map((t) => t.trim())
  .filter(Boolean);

if (tokens.length === 0) {
  console.error(
    'Не найдены токены Apify. Проверь, что в C:/AI-projects/.env есть APIFY_API_TOKENS или APIFY_API_TOKEN.'
  );
  process.exit(1);
}

let tokenIdx = 0;

// Запуск актора с ротацией пула токенов. Возвращает массив items (рилсы с метриками).
async function callActorWithRotation(actorId, input) {
  for (let attempt = 0; attempt < tokens.length; attempt++) {
    const client = new ApifyClient({ token: tokens[tokenIdx] });
    try {
      // ВАЖНО: НЕ .call() — он стримит логи актора в stdout и под обёрткой dotenvx
      // роняет node (exit 127) ДО получения данных (актор при этом отрабатывает).
      // Запускаем через start(), ждём завершения и читаем dataset отдельным запросом —
      // этот путь логи не стримит и не падает.
      const started = await client.actor(actorId).start(input);
      const runClient = client.run(started.id);
      let info = await runClient.get();
      while (info && (info.status === 'RUNNING' || info.status === 'READY')) {
        await runClient.waitForFinish({ waitSecs: 60 });
        info = await runClient.get();
      }
      if (!info || info.status !== 'SUCCEEDED') {
        throw new Error(`Прогон актора завершился со статусом ${info?.status || 'неизвестно'}`);
      }
      const { items } = await client.dataset(info.defaultDatasetId).listItems();
      console.log(`Токен #${tokenIdx + 1} OK — собрано ${items.length} рилсов`);
      return items;
    } catch (err) {
      const status = err?.statusCode || err?.response?.status;
      if ([401, 402, 429].includes(status)) {
        console.warn(`Токен #${tokenIdx + 1} вернул ${status} (лимит/кредиты), пробую следующий...`);
        tokenIdx = (tokenIdx + 1) % tokens.length;
        continue;
      }
      // Другие ошибки (404 актор не найден, кривой input, сеть) — прокидываем наверх
      throw err;
    }
  }
  throw new Error(`Все ${tokens.length} токенов Apify исчерпаны (rate-limit / закончились кредиты).`);
}

// Сохранить сырой JSON выгрузки. Падение здесь не должно ронять весь прогон —
// данные уже на руках, поэтому ошибку логируем, но дальше идём.
async function saveRawJson(items) {
  const date = new Date().toISOString().slice(0, 10);
  try {
    await mkdir(JSON_DIR, { recursive: true });
    const path = `${JSON_DIR}/${date}.json`;
    await writeFile(path, JSON.stringify(items, null, 2), 'utf8');
    console.log(`Сырой JSON сохранён: ${path}`);
  } catch (err) {
    console.warn(`Не удалось сохранить сырой JSON (${err.message}). Данные всё равно в памяти, продолжаю.`);
  }
}

// Отобрать топ по просмотрам ВНУТРИ КАЖДОГО аккаунта, а не общим списком.
// Зачем: иначе крупный аккаунт займёт весь топ и вытеснит сильные ролики мелкого —
// ровно та ошибка, от которой предостерегает методика урока («отбирай по каждому
// аккаунту отдельно»). Берём round-robin: топ-1 с каждого автора, потом топ-2 и т.д.,
// пока не наберём n. Рилсы без числа просмотров оседают в конце своей группы.
function pickTop(items, n) {
  const views = (r) => (typeof r?.videoPlayCount === 'number' ? r.videoPlayCount : -1);
  const byAuthor = new Map();
  for (const r of items) {
    const a = r?.ownerUsername || 'unknown';
    if (!byAuthor.has(a)) byAuthor.set(a, []);
    byAuthor.get(a).push(r);
  }
  for (const arr of byAuthor.values()) arr.sort((a, b) => views(b) - views(a));
  const authors = [...byAuthor.keys()];
  const maxRank = Math.max(0, ...authors.map((a) => byAuthor.get(a).length));
  const result = [];
  for (let rank = 0; rank < maxRank && result.length < n; rank++) {
    for (const a of authors) {
      const arr = byAuthor.get(a);
      if (arr[rank]) { result.push(arr[rank]); if (result.length >= n) break; }
    }
  }
  return result;
}

// Достать id рилса для имени файла: из shortCode, иначе из url, иначе из времени.
function reelId(reel) {
  if (reel?.shortCode) return String(reel.shortCode);
  const m = typeof reel?.url === 'string' ? reel.url.match(/\/reel\/([^/?]+)/) || reel.url.match(/\/p\/([^/?]+)/) : null;
  if (m) return m[1];
  return `reel_${Date.now()}`;
}

// Имя файла без запрещённых символов: <автор>_<id>.mp4
function safeFileName(reel) {
  const author = (reel?.ownerUsername || 'unknown').replace(/[^\w.-]/g, '_');
  const id = reelId(reel).replace(/[^\w.-]/g, '_');
  return `${author}_${id}.mp4`;
}

// Скачать одно видео по videoUrl потоком в файл (без загрузки целиком в память).
// Прямые ссылки Instagram живут недолго — поэтому качаем сразу после сбора.
async function downloadVideo(reel) {
  const fileName = safeFileName(reel);
  const dest = `${RAW_DIR}/${fileName}`;

  if (!reel?.videoUrl) {
    console.warn(`  ! ${fileName}: у рилса нет videoUrl — пропускаю (нечего качать).`);
    return false;
  }

  try {
    const res = await fetch(reel.videoUrl);
    if (!res.ok || !res.body) {
      console.warn(`  ! ${fileName}: ответ ${res.status} (ссылка могла протухнуть) — пропускаю.`);
      return false;
    }
    await pipeline(Readable.fromWeb(res.body), createWriteStream(dest));
    console.log(`  + ${fileName}`);
    return true;
  } catch (err) {
    console.warn(`  ! ${fileName}: не скачалось (${err.message}) — пропускаю, остальные качаю дальше.`);
    return false;
  }
}

// Ячейка markdown-таблицы: убрать переносы строк и вертикальную черту.
function mdCell(s) {
  return String(s ?? '—').replace(/\|/g, '/').replace(/\r?\n/g, ' ').trim() || '—';
}

// Записать готовую таблицу статистики m6_reels_stats.md — главный выход роли 1.
// Контент-аналитик (роль 3) берёт её как топливо: по просмотрам видно, что зашло.
async function writeStatsTable(top) {
  const num = (v) => (typeof v === 'number' ? v : 'нет данных');
  const rows = top.map((r, i) => {
    const date = typeof r?.timestamp === 'string' ? r.timestamp.slice(0, 10) : '—';
    const dur = typeof r?.videoDuration === 'number' ? `${Math.round(r.videoDuration)}` : '—';
    const caption = mdCell((r?.caption || '').slice(0, 60));
    const video = r?.videoUrl ? 'есть (в JSON)' : '—';
    return `| ${i + 1} | ${mdCell(r?.ownerUsername)} | ${mdCell(r?.url)} | ${date} | ${num(r?.videoPlayCount)} | ${num(r?.likesCount)} | ${num(r?.commentsCount)} | ${num(r?.sharesCount)} | ${dur} | ${caption} | ${video} |`;
  });
  const header =
    '# m6_reels_stats\n\n' +
    '> Таблица собрана скриптом collect_reels.mjs. Главная метрика — Просмотры: по ней видно,\n' +
    '> что зашло. Прямая ссылка на видео (videoUrl) живёт недолго — она есть в сыром JSON,\n' +
    '> а сами видео уже скачаны в reels_raw/. Эту таблицу берёт контент-аналитик (роль 3).\n\n' +
    '| № | Автор | Ссылка на рилс | Дата | Просмотры | Лайки | Комм. | Репосты | Длина, с | Подпись (начало) | Видео |\n' +
    '|---|---|---|---|---|---|---|---|---|---|---|\n';
  try {
    await mkdir('materials/M6', { recursive: true });
    await writeFile(STATS_FILE, header + rows.join('\n') + '\n', 'utf8');
    console.log(`Таблица статистики собрана: ${STATS_FILE}`);
  } catch (err) {
    console.warn(`Не удалось записать таблицу ${STATS_FILE} (${err.message}). Данные есть в сыром JSON.`);
  }
}

// Печать итоговой таблицы отобранных рилсов в консоль.
function printTable(top) {
  console.log('\n=== Отобрано в работу (топ по просмотрам внутри каждого аккаунта) ===');
  console.log('Автор'.padEnd(22), 'Просмотры'.padEnd(12), 'Лайки'.padEnd(9), 'Комм.'.padEnd(8), 'Длина', ' Ссылка');
  for (const r of top) {
    const views = typeof r?.videoPlayCount === 'number' ? r.videoPlayCount : 'нет данных';
    const likes = typeof r?.likesCount === 'number' ? r.likesCount : '—';
    const comments = typeof r?.commentsCount === 'number' ? r.commentsCount : '—';
    const dur = typeof r?.videoDuration === 'number' ? `${Math.round(r.videoDuration)}с` : '—';
    console.log(
      String(r?.ownerUsername || 'unknown').padEnd(22),
      String(views).padEnd(12),
      String(likes).padEnd(9),
      String(comments).padEnd(8),
      String(dur).padEnd(6),
      r?.url || ''
    );
  }
  console.log(
    '\nДальше: видео из materials/M6/reels_raw/ отдай транскрибатору (transcribe.mjs) → reels_text/.\n' +
    'Таблица materials/M6/m6_reels_stats.md уже собрана — её возьмёт контент-аналитик (роль 3).'
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Главный сценарий
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Источники (${SOURCES.length}):`);
  SOURCES.forEach((p) => console.log(`  - ${p}`));
  console.log(`Тяну по ${RESULTS_LIMIT} рилсов с источника, в работу беру топ-${TOP_N} — поровну по аккаунтам (топ внутри каждого).\n`);

  // 1) Сбор метрик через актор
  let items;
  try {
    items = await callActorWithRotation(ACTOR_ID, {
      directUrls: SOURCES,
      resultsType: 'posts',
      resultsLimit: RESULTS_LIMIT,
    });
  } catch (err) {
    console.error(`Сбор не удался: ${err.message}`);
    process.exit(1);
  }

  if (!Array.isArray(items) || items.length === 0) {
    console.error('Актор вернул 0 рилсов. Проверь ссылки-референсы (публичные ли аккаунты) и лимит.');
    process.exit(1);
  }

  // 2) Сохранить сырой JSON (не критично к падению)
  await saveRawJson(items);

  // 3) Отобрать топ-N по просмотрам
  const top = pickTop(items, TOP_N);

  // 4) Подготовить папку и скачать видео отобранных роликов
  try {
    await mkdir(RAW_DIR, { recursive: true });
  } catch (err) {
    console.error(`Не удалось создать папку ${RAW_DIR}: ${err.message}`);
    process.exit(1);
  }

  console.log(`\nКачаю видео топ-${top.length} рилсов в ${RAW_DIR}/ ...`);
  let downloaded = 0;
  for (const reel of top) {
    const ok = await downloadVideo(reel);
    if (ok) downloaded++;
  }
  console.log(`\nСкачано видео: ${downloaded} из ${top.length}.`);
  if (downloaded < top.length) {
    console.warn('Часть видео не скачалась (обычно протухшие ссылки Instagram). Перезапусти сбор — ссылки обновятся.');
  }

  // 5) Собрать готовую таблицу статистики (главный выход роли 1)
  await writeStatsTable(top);

  // 6) Печать таблицы отобранных в консоль
  printTable(top);
}

main().catch((err) => {
  console.error(`Непредвиденная ошибка: ${err.message}`);
  process.exit(1);
});
