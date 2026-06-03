const TABLE_LABELS = {
  stock_daily: "股票日线", stock_weekly: "股票周线", stock_monthly: "股票月线",
  etf_daily: "ETF日线", etf_weekly: "ETF周线", etf_monthly: "ETF月线",
  stock_info: "股票基础信息", etf_info: "ETF基础信息", trade_calendar: "交易日历",
  factor_rps_daily: "股票RPS日频因子", factor_update_log: "股票因子更新日志",
  etf_factor_rps_daily: "ETF RPS日频因子", etf_factor_update_log: "ETF因子更新日志"
};
const COLUMN_LABELS = {
  code: "代码", name: "名称", date: "日期", open: "开盘", high: "最高", low: "最低",
  close: "收盘", preclose: "前收", volume: "成交量", amount: "成交额", adjustflag: "复权",
  turn: "换手率", tradestatus: "交易状态", pctChg: "涨跌幅", isST: "ST", update_time: "更新时间",
  updated_at: "更新时间", is_trading_day: "交易日", market: "市场", universe: "股票池",
  factor_version: "因子版本", factor_name: "因子", start_date: "开始日期", end_date: "结束日期",
  status: "状态", message: "消息", asset_type: "资产", ret_5: "RET5", ret_20: "RET20", ret_50: "RET50", ret_120: "RET120", ret_250: "RET250", rps_5: "RPS5", rps_10: "RPS10", rps_20: "RPS20",
  rps_50: "RPS50", rps_120: "RPS120", rps_250: "RPS250"
};
COLUMN_LABELS.rps_score = "RPS总分";
const PAGE_META = {
  overview: ["DATA ASSET OVERVIEW", "数据资产总览"], market: ["MARKET SNAPSHOT", "市场快照"],
  research: ["SECURITY RESEARCH", "证券研究"], browser: ["DATA EXPLORER", "数据浏览"],
  sync: ["SYNC CENTER", "同步中心"], factors: ["FACTOR LAYER", "因子中心"],
  quality: ["QUALITY & OBSERVABILITY", "质量与日志"]
};
const state = { page: "overview", dashboard: null, browserPage: 1, browserPages: 1, browserSort: { table: "", column: "", order: "desc" }, securityMap: {}, taskOpen: false };
const $ = (id) => document.getElementById(id);
const qsa = (selector) => [...document.querySelectorAll(selector)];
const icon = (name) => `<svg><use href="#icon-${name}"/></svg>`;
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
const fmt = (value, digits = 2) => value == null || value === "" ? "--" : Number(value).toLocaleString("zh-CN", { maximumFractionDigits: digits });
const fmtCount = (value) => Number(value || 0).toLocaleString("zh-CN");
const compact = (value) => {
  const number = Number(value || 0);
  if (Math.abs(number) >= 1e8) return `${(number / 1e8).toFixed(2)} 亿`;
  if (Math.abs(number) >= 1e4) return `${(number / 1e4).toFixed(1)} 万`;
  return fmt(number);
};
const tableFor = (asset, frequency) => `${asset}_${frequency}`;
const pctClass = (value) => Number(value) > 0 ? "rise" : Number(value) < 0 ? "fall" : "";
const pct = (value) => `${Number(value || 0) > 0 ? "+" : ""}${fmt(value)}%`;
const RPS_BROWSER_TABLES = new Set(["factor_rps_daily", "etf_factor_rps_daily"]);
const NUMERIC_FACTOR_COLUMNS = new Set(["ret_5","ret_20","ret_50","ret_120","ret_250","rps_score","rps_5","rps_10","rps_20","rps_50","rps_120","rps_250"]);
const DATE_COLUMNS = new Set(["date","start_date","end_date","updated_at","update_time","started_at","finished_at","generated_at","latest_trade_date","min_date","max_date"]);
const MONTH_NUMBERS = { jan: "01", feb: "02", mar: "03", apr: "04", may: "05", jun: "06", jul: "07", aug: "08", sep: "09", oct: "10", nov: "11", dec: "12" };
function isDateColumn(column) {
  const name = String(column || "").toLowerCase();
  return DATE_COLUMNS.has(name) || name.endsWith("_date") || name.endsWith("_at") || name.endsWith("_time");
}
function compactDate(value) {
  if (value == null || value === "") return "--";
  const text = String(value).trim();
  let match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (match) return `${match[1]}${match[2]}${match[3]}`;
  match = text.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (match) return text;
  match = text.match(/^\w{3},\s+(\d{2})\s+([A-Za-z]{3})\s+(\d{4})/);
  if (match) return `${match[3]}${MONTH_NUMBERS[match[2].toLowerCase()] || match[2]}${match[1]}`;
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime()) && /(?:GMT|T|\d{4}-\d{2}-\d{2})/.test(text)) {
    return `${parsed.getUTCFullYear()}${String(parsed.getUTCMonth() + 1).padStart(2, "0")}${String(parsed.getUTCDate()).padStart(2, "0")}`;
  }
  return text;
}
function compactRange(start, end) {
  return start && end ? `${compactDate(start)} ~ ${compactDate(end)}` : "--";
}
function isSortableBrowserColumn(table, column) {
  return RPS_BROWSER_TABLES.has(table) && column === "rps_score";
}
async function api(url, options = {}) {
  const response = await fetch(url, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.msg || body.message || `请求失败：HTTP ${response.status}`);
  return body;
}
function toast(message, type = "ok") {
  const item = document.createElement("div");
  item.className = `toast ${type}`;
  item.textContent = message;
  $("toast-box").appendChild(item);
  setTimeout(() => item.remove(), 4200);
}
function isNearLogBottom(element, threshold = 32) {
  return element.scrollHeight - element.scrollTop - element.clientHeight <= threshold;
}
function updateLogText(element, text, followLatest = true) {
  const shouldFollow = followLatest && isNearLogBottom(element);
  const previousTop = element.scrollTop;
  element.textContent = text;
  if (shouldFollow) element.scrollTop = element.scrollHeight;
  else element.scrollTop = Math.min(previousTop, Math.max(0, element.scrollHeight - element.clientHeight));
}
function issueHtml(item) {
  const symbol = item.level === "warning" || item.level === "error" ? "alert" : "info";
  return `<div class="issue ${esc(item.level)}">${icon(symbol)}<div><b>${esc(item.title)}</b><p>${esc(item.detail)}</p></div></div>`;
}
function emptyHtml(message) { return `<div class="empty-state"><p>${esc(message)}</p></div>`; }
function setPage(page) {
  state.page = page;
  qsa(".page").forEach((item) => item.classList.toggle("active", item.id === `page-${page}`));
  qsa("[data-page]").forEach((item) => item.classList.toggle("active", item.dataset.page === page));
  $("page-eyebrow").textContent = PAGE_META[page][0];
  $("page-title").textContent = PAGE_META[page][1];
  $("sidebar").classList.remove("open");
  $("scrim").classList.remove("open");
  if (page === "overview") loadDashboard();
  if (page === "market") loadMarket();
  if (page === "browser") loadBrowserTable(1);
  if (page === "factors") loadFactors();
  if (page === "quality") loadQuality();
}
function metricHtml(label, value, caption = "") {
  return `<div class="metric"><span>${esc(label)}</span><strong>${esc(value)}</strong><small>${esc(caption)}</small></div>`;
}
async function loadDashboard(force = false) {
  try {
    const dashboard = await api(`/api/dashboard${force ? "?refresh=1" : ""}`);
    state.dashboard = dashboard;
    renderDashboard(dashboard);
    updateTaskIndicator(Boolean(dashboard.task));
  } catch (error) {
    toast(`总览加载失败：${error.message}`, "error");
  }
}
function renderDashboard(data) {
  $("health-score").textContent = data.health_score;
  $("health-ring").style.setProperty("--score-angle", `${data.health_score * 3.6}deg`);
  $("latest-date").textContent = compactDate(data.summary.latest_trade_date);
  const databases = Object.values(data.databases || {});
  const healthy = databases.every((item) => item.status === "ok");
  $("db-pill").innerHTML = `<span class="status-light ${healthy ? "" : "error"}"></span><span>${healthy ? "行情库与因子库已连接" : "数据库连接异常"}</span>`;
  $("db-mini-grid").innerHTML = databases.map((db) => `<div class="db-mini"><b>${esc(db.label || (db.asset_type === "stock" ? "股票库" : "ETF 库"))}</b>${db.status === "ok" ? "连接正常" : "连接异常"} · ${db.tables.length} 张表</div>`).join("");
  $("metric-grid").innerHTML = [
    metricHtml("股票日线", compact(data.summary.stock_daily_count), "本地行情明细"),
    metricHtml("ETF 日线", compact(data.summary.etf_daily_count), "本地基金行情"),
    metricHtml("股票证券数", fmtCount(data.summary.stock_count), "股票基础信息"),
    metricHtml("ETF 数量", fmtCount(data.summary.etf_count), "ETF 基础信息")
  ].join("");
  const byTable = Object.fromEntries(data.tables.map((item) => [item.table, item]));
  const rows = [
    ["股票行情", ["stock_daily", "stock_weekly", "stock_monthly"]],
    ["ETF 行情", ["etf_daily", "etf_weekly", "etf_monthly"]],
    ["股票因子", ["factor_rps_daily", "factor_update_log"]],
    ["ETF 因子", ["etf_factor_rps_daily", "etf_factor_update_log"]]
  ];
  $("freshness-grid").innerHTML = rows.map(([label, tables]) => {
    return `<div class="fresh-row"><b>${label}</b>${tables.map((table) => {
      const item = byTable[table] || {};
      return `<div class="fresh-cell ${item.count ? "" : "empty"}"><span>${esc(TABLE_LABELS[table])}</span><b>${esc(item.max_date ? compactDate(item.max_date) : (item.count ? `${fmtCount(item.count)} 条` : "待补齐"))}</b></div>`;
    }).join("")}</div>`;
  }).join("");
  $("overview-issues").innerHTML = data.issues.length ? data.issues.slice(0, 4).map(issueHtml).join("") : issueHtml({ level: "info", title: "没有发现明显问题", detail: "本地数据资产状态良好。" });
}
async function loadMarket() {
  const asset = $("#market-assets .active")?.dataset.value || "stock";
  const table = tableFor(asset, $("market-frequency").value);
  const selectedDate = $("market-date").value;
  const datePart = selectedDate ? `&date=${encodeURIComponent(selectedDate)}` : "";
  try {
    const [rise, fall] = await Promise.all([
      api(`/api/top_movers?table=${table}&direction=rise&limit=10${datePart}`),
      api(`/api/top_movers?table=${table}&direction=fall&limit=10${datePart}`)
    ]);
    const snapshotDate = rise.date || selectedDate;
    const distribution = await api(`/api/distribution?table=${table}&field=pctChg&bins=12${snapshotDate ? `&date=${encodeURIComponent(snapshotDate)}` : ""}`);
    if (snapshotDate) $("market-date").value = snapshotDate;
    $("market-summary").innerHTML = [
      ["上涨", fmtCount(distribution.rise_count), "rise"], ["下跌", fmtCount(distribution.fall_count), "fall"],
      ["平盘", fmtCount(distribution.flat_count), ""], ["平均涨跌幅", pct(distribution.avg_pct_chg), pctClass(distribution.avg_pct_chg)]
    ].map(([label, value, cls]) => `<div class="market-stat"><span>${label}</span><strong class="${cls}">${value}</strong></div>`).join("");
    drawBarChart($("distribution-chart"), distribution.bins || [], distribution.counts || []);
    renderMovers("rise-table", rise.data || []);
    renderMovers("fall-table", fall.data || []);
    const broken = Number(distribution.rise_count || 0) === 0 && Number(distribution.fall_count || 0) === 0 && Number(distribution.flat_count || 0) > 0;
    const displaySnapshotDate = snapshotDate ? compactDate(snapshotDate) : "";
    $("market-diagnostic").innerHTML = broken
      ? issueHtml({ level: "warning", title: `${displaySnapshotDate || "当前日期"} 的涨跌幅全部为 0`, detail: "涨跌榜和市场宽度可能失真。建议检查 pctChg 字段映射后再使用该快照进行判断。" })
      : issueHtml({ level: "info", title: "快照分布可用", detail: `当前快照覆盖 ${fmtCount((distribution.rise_count || 0) + (distribution.fall_count || 0) + (distribution.flat_count || 0))} 个证券。` });
  } catch (error) {
    toast(`市场快照加载失败：${error.message}`, "error");
  }
}
function renderMovers(id, rows) {
  $(id).innerHTML = rows.length ? `<table><thead><tr><th>代码</th><th>名称</th><th>收盘</th><th>涨跌幅</th></tr></thead><tbody>${rows.map((row) => `<tr class="clickable-row" data-security="${esc(row.code)}" data-name="${esc(row.name)}"><td>${esc(row.code)}</td><td>${esc(row.name)}</td><td>${fmt(row.close)}</td><td class="${pctClass(row.pctChg)}">${pct(row.pctChg)}</td></tr>`).join("")}</tbody></table>` : emptyHtml("暂无排行数据");
  qsa(`#${id} .clickable-row`).forEach((row) => row.addEventListener("click", () => openResearch(row.dataset.security, row.dataset.name)));
}
function drawBarChart(canvas, labels, values) {
  const ctx = setupCanvas(canvas);
  const { width, height } = canvas.getBoundingClientRect();
  ctx.clearRect(0, 0, width, height);
  if (!values.length) return drawEmptyCanvas(ctx, width, height, "暂无分布数据");
  const pad = { l: 38, r: 12, t: 16, b: 42 }, max = Math.max(...values, 1), gap = 5;
  const barWidth = Math.max(4, (width - pad.l - pad.r - gap * (values.length - 1)) / values.length);
  ctx.font = "11px Bahnschrift"; ctx.fillStyle = "#6c7d79"; ctx.strokeStyle = "#dce3dc";
  ctx.beginPath(); ctx.moveTo(pad.l, pad.t); ctx.lineTo(pad.l, height - pad.b); ctx.lineTo(width - pad.r, height - pad.b); ctx.stroke();
  values.forEach((value, index) => {
    const barHeight = (height - pad.t - pad.b) * Number(value) / max;
    const x = pad.l + index * (barWidth + gap), y = height - pad.b - barHeight;
    ctx.fillStyle = "#176b67"; ctx.fillRect(x, y, barWidth, barHeight);
    if (index % Math.max(1, Math.floor(values.length / 4)) === 0) {
      ctx.save(); ctx.translate(x, height - 20); ctx.rotate(-.35); ctx.fillStyle = "#6c7d79"; ctx.fillText(labels[index] || "", 0, 0); ctx.restore();
    }
  });
  ctx.fillStyle = "#6c7d79"; ctx.fillText(fmtCount(max), 4, pad.t + 4); ctx.fillText("0", 20, height - pad.b + 4);
}
function setupCanvas(canvas) {
  const ratio = window.devicePixelRatio || 1, rect = canvas.getBoundingClientRect();
  canvas.width = Math.max(1, rect.width * ratio); canvas.height = Math.max(1, Number(canvas.getAttribute("height")) * ratio);
  canvas.style.height = `${Number(canvas.getAttribute("height"))}px`;
  const ctx = canvas.getContext("2d"); ctx.scale(ratio, ratio); return ctx;
}
function drawEmptyCanvas(ctx, width, height, message) { ctx.fillStyle = "#6c7d79"; ctx.font = "13px Microsoft YaHei UI"; ctx.fillText(message, Math.max(12, width / 2 - 45), height / 2); }
let searchTimer;
async function loadSecurityOptions() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(async () => {
    try {
      const keyword = $("research-input").value.trim();
      const rows = await api(`/api/security_search?asset_type=${$("research-asset").value}&keyword=${encodeURIComponent(keyword)}&limit=20`);
      state.securityMap = Object.fromEntries(rows.flatMap((row) => [[row.code, row], [`${row.code} ${row.name}`, row], [row.name, row]]));
      $("security-options").innerHTML = rows.map((row) => `<option value="${esc(row.code)} ${esc(row.name)}"></option>`).join("");
    } catch {}
  }, 180);
}
function openResearch(code, name = "") {
  $("research-input").value = name ? `${code} ${name}` : code;
  setPage("research");
  loadResearch();
}
async function loadResearch() {
  const raw = $("research-input").value.trim();
  const match = state.securityMap[raw];
  const code = match?.code || raw.split(/\s+/)[0];
  if (!code) return toast("请先输入证券代码或名称", "warning");
  const table = tableFor($("research-asset").value, $("research-frequency").value);
  const range = $("research-range").value;
  let start = "";
  if (range !== "all") {
    const date = new Date(); date.setDate(date.getDate() - Number(range)); start = date.toISOString().slice(0, 10);
  }
  try {
    const data = await api(`/api/kline?table=${table}&code=${encodeURIComponent(code)}${start ? `&start=${start}` : ""}`);
    if (!data.dates.length) throw new Error("当前条件下没有行情数据");
    $("research-empty").classList.add("hidden"); $("research-content").classList.remove("hidden");
    $("research-name").textContent = data.name || code; $("research-code").textContent = code;
    $("research-caption").textContent = `${TABLE_LABELS[table]} · ${compactDate(data.dates[0])} 至 ${compactDate(data.dates[data.dates.length - 1])}`;
    const last = data.ohlc.length - 1, closes = data.ohlc.map((item) => Number(item[1])), highs = data.ohlc.map((item) => Number(item[3])), lows = data.ohlc.map((item) => Number(item[2]));
    $("research-metrics").innerHTML = [
      metricHtml("最新收盘", fmt(closes[last]), compactDate(data.dates[last])), metricHtml("区间最高", fmt(Math.max(...highs)), "当前筛选区间"),
      metricHtml("区间最低", fmt(Math.min(...lows)), "当前筛选区间"), metricHtml("最新成交额", compact(data.amounts[last]), "最近交易日")
    ].join("");
    drawKline($("kline-chart"), data);
    $("research-recent").innerHTML = data.dates.slice(-12).reverse().map((date, reverseIndex) => {
      const index = data.dates.length - 1 - reverseIndex, row = data.ohlc[index];
      return `<tr><td>${esc(compactDate(date))}</td><td>${fmt(row[0])}</td><td>${fmt(row[3])}</td><td>${fmt(row[2])}</td><td>${fmt(row[1])}</td><td>${fmtCount(data.volumes[index])}</td><td>${compact(data.amounts[index])}</td></tr>`;
    }).join("");
  } catch (error) {
    toast(`证券研究加载失败：${error.message}`, "error");
  }
}
function drawKline(canvas, data) {
  const ctx = setupCanvas(canvas), rect = canvas.getBoundingClientRect(), width = rect.width, height = Number(canvas.getAttribute("height"));
  ctx.clearRect(0, 0, width, height);
  const visible = Math.min(120, data.ohlc.length), offset = data.ohlc.length - visible, rows = data.ohlc.slice(offset);
  if (!rows.length) return drawEmptyCanvas(ctx, width, height, "暂无 K 线数据");
  const volumes = data.volumes.slice(offset), values = rows.flatMap((row) => [Number(row[2]), Number(row[3])]).filter(Number.isFinite);
  const min = Math.min(...values), max = Math.max(...values), pad = { l: 52, r: 12, t: 16, b: 26 }, volumeH = 72, priceBottom = height - pad.b - volumeH - 16;
  const xStep = (width - pad.l - pad.r) / visible, candleW = Math.max(2, Math.min(7, xStep * .66));
  const y = (value) => pad.t + (max - Number(value)) / Math.max(.0001, max - min) * (priceBottom - pad.t);
  ctx.strokeStyle = "#dce3dc"; ctx.fillStyle = "#6c7d79"; ctx.font = "11px Bahnschrift";
  for (let i = 0; i <= 4; i++) { const py = pad.t + (priceBottom - pad.t) * i / 4; ctx.beginPath(); ctx.moveTo(pad.l, py); ctx.lineTo(width - pad.r, py); ctx.stroke(); ctx.fillText((max - (max - min) * i / 4).toFixed(2), 5, py + 4); }
  rows.forEach((row, index) => {
    const x = pad.l + xStep * index + xStep / 2, open = y(row[0]), close = y(row[1]), low = y(row[2]), high = y(row[3]), up = Number(row[1]) >= Number(row[0]);
    ctx.strokeStyle = up ? "#c8493a" : "#218568"; ctx.fillStyle = ctx.strokeStyle;
    ctx.beginPath(); ctx.moveTo(x, high); ctx.lineTo(x, low); ctx.stroke(); ctx.fillRect(x - candleW / 2, Math.min(open, close), candleW, Math.max(1, Math.abs(close - open)));
  });
  const maxVolume = Math.max(...volumes.map(Number).filter(Number.isFinite), 1);
  volumes.forEach((value, index) => { const h = Number(value || 0) / maxVolume * volumeH; ctx.fillStyle = "rgba(23,107,103,.36)"; ctx.fillRect(pad.l + xStep * index, height - pad.b - h, Math.max(1, xStep - 1), h); });
  [["5", "#cd7940"], ["10", "#3a7f92"], ["20", "#9a6853"], ["60", "#627651"]].forEach(([period, color]) => {
    const series = (data.mas[period] || []).slice(offset); ctx.strokeStyle = color; ctx.lineWidth = 1.2; ctx.beginPath(); let started = false;
    series.forEach((value, index) => { if (value == null) return; const x = pad.l + xStep * index + xStep / 2, py = y(value); if (!started) { ctx.moveTo(x, py); started = true; } else ctx.lineTo(x, py); }); ctx.stroke();
  });
}
async function loadBrowserTable(page = 1) {
  state.browserPage = Math.max(1, page);
  const table = $("browser-table").value;
  const params = new URLSearchParams({ table, page: state.browserPage - 1, page_size: 50, keyword: $("browser-keyword").value.trim(), start: $("browser-start").value, end: $("browser-end").value });
  if (state.browserSort.table === table && state.browserSort.column) {
    params.set("sort", state.browserSort.column);
    params.set("order", state.browserSort.order);
  }
  $("browser-tbody").innerHTML = `<tr><td>正在加载...</td></tr>`;
  try {
    const data = await api(`/api/table?${params}`);
    const rows = Array.isArray(data.rows) ? data.rows : [];
    const columns = Array.isArray(data.columns) && data.columns.length ? data.columns : (rows[0] ? Object.keys(rows[0]) : []);
    const total = Number.isFinite(Number(data.total)) ? Number(data.total) : rows.length;
    state.browserPages = Math.max(1, Math.ceil(total / 50));
    state.browserPage = Math.min(state.browserPage, state.browserPages);
    $("browser-result").textContent = `找到 ${fmtCount(total)} 条记录`;
    $("browser-page").textContent = `第 ${state.browserPage} / ${state.browserPages} 页`;
    $("browser-prev").disabled = state.browserPage <= 1; $("browser-next").disabled = state.browserPage >= state.browserPages;
    $("browser-thead").innerHTML = `<tr>${columns.map((column) => {
      const sortable = isSortableBrowserColumn(table, column);
      const active = sortable && state.browserSort.table === table && state.browserSort.column === column;
      const indicator = active ? (state.browserSort.order === "asc" ? "↑" : "↓") : "↕";
      return `<th class="${sortable ? "sortable" : ""} ${active ? "active" : ""}" ${sortable ? `data-sort-column="${esc(column)}" title="按${esc(COLUMN_LABELS[column] || column)}排序"` : ""}>${esc(COLUMN_LABELS[column] || column)}${sortable ? `<span class="sort-indicator">${indicator}</span>` : ""}</th>`;
    }).join("")}</tr>`;
    qsa("#browser-thead th[data-sort-column]").forEach((header) => header.addEventListener("click", () => {
      const column = header.dataset.sortColumn;
      const sameSort = state.browserSort.table === table && state.browserSort.column === column;
      state.browserSort = { table, column, order: sameSort && state.browserSort.order === "desc" ? "asc" : "desc" };
      loadBrowserTable(1);
    }));
    const marketTable = ["stock_daily","stock_weekly","stock_monthly","etf_daily","etf_weekly","etf_monthly","factor_rps_daily","etf_factor_rps_daily"].includes(table);
    $("browser-tbody").innerHTML = rows.length ? rows.map((row) => `<tr class="${marketTable ? "clickable-row" : ""}" ${marketTable ? `data-security="${esc(row.code)}" data-name="${esc(row.name)}"` : ""}>${columns.map((column) => `<td class="${column === "pctChg" ? pctClass(row[column]) : ""}">${esc(formatCell(column, row[column]))}</td>`).join("")}</tr>`).join("") : `<tr><td colspan="${Math.max(columns.length, 1)}">暂无数据</td></tr>`;
    qsa("#browser-tbody .clickable-row").forEach((row) => row.addEventListener("click", () => openResearch(row.dataset.security, row.dataset.name)));
  } catch (error) {
    $("browser-result").textContent = "查询失败";
    $("browser-page").textContent = "第 1 / 1 页";
    $("browser-prev").disabled = true;
    $("browser-next").disabled = true;
    $("browser-tbody").innerHTML = `<tr><td>${esc(error.message)}</td></tr>`;
    toast(`数据查询失败：${error.message}`, "error");
  }
}
function formatCell(column, value) {
  if (value == null) return "--";
  if (isDateColumn(column)) return compactDate(value);
  if (column === "amount") return compact(value);
  if (column === "pctChg") return pct(value);
  if (["open","high","low","close","preclose","turn"].includes(column) || NUMERIC_FACTOR_COLUMNS.has(column)) return fmt(value, 4);
  if (column === "volume") return fmtCount(value);
  return value;
}
function exportData() {
  const params = new URLSearchParams({ table: $("browser-table").value, keyword: $("browser-keyword").value.trim(), start: $("browser-start").value, end: $("browser-end").value });
  window.open(`/api/export?${params}`, "_blank"); toast("已开始导出当前筛选结果");
}
async function startTask(endpoint, body = {}, danger = "") {
  if (danger && !confirm(`${danger}耗时较长，可能重建大量本地数据。确定继续吗？`)) return;
  try {
    const result = await api(endpoint, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    if (!result.success) throw new Error(result.message || "任务启动失败");
    toast(result.message || "任务已启动"); openTaskDrawer(); await loadTaskDrawer();
  } catch (error) { toast(error.message, "error"); }
}
async function loadTaskDrawer() {
  try {
    const [progress, history, logs] = await Promise.all([api("/api/progress"), api("/api/task_history"), api("/api/logs?limit=100")]);
    const running = Boolean(progress.is_running || progress.backend_task_running || history.current);
    updateTaskIndicator(running);
    if (running) {
      const percent = progress.total ? Math.min(100, Math.round(progress.processed / progress.total * 100)) : 0;
      $("task-current").innerHTML = `<div class="current-task"><div class="task-title-row"><h3>${esc(history.current?.name || progress.task_name || "后台任务")}</h3><span>${percent}%</span></div><div class="progress-track"><div class="progress-fill" style="width:${percent}%"></div></div><div class="task-meta"><span>${fmtCount(progress.processed)}/${fmtCount(progress.total)}</span><span>成功 ${fmtCount(progress.success)}</span><span>失败 ${fmtCount(progress.error)}</span><span>${progress.speed ? `${fmt(progress.speed)} 只/秒` : "正在准备"}</span></div><div class="recipe-actions" style="margin-top:13px"><button class="btn danger" id="stop-current-task">停止任务</button></div></div>`;
      $("stop-current-task").addEventListener("click", stopTask);
    } else $("task-current").innerHTML = emptyHtml("当前没有运行中的后台任务");
    $("task-history").innerHTML = history.history.length ? history.history.map((item) => `<div class="history-item"><div><b>${esc(item.name)}</b><span>${esc(compactDate(item.started_at))} · ${esc(item.message || "")}</span></div><div><b>${esc(item.status)}</b><span>${item.duration_seconds != null ? `${item.duration_seconds}s` : ""}</span></div></div>`).join("") : emptyHtml("暂无历史记录");
    $("task-log-state").textContent = logs.is_running ? "运行中" : "空闲";
    updateLogText($("task-log"), [...(logs.logs || []), ...(logs.error_logs || []).slice(-15)].join("\n") || "暂无日志");
  } catch (error) { $("task-current").innerHTML = emptyHtml(error.message); }
}
function openTaskDrawer() { state.taskOpen = true; $("task-drawer").classList.add("open"); $("scrim").classList.add("open"); loadTaskDrawer(); }
function closeTaskDrawer() { state.taskOpen = false; $("task-drawer").classList.remove("open"); $("scrim").classList.remove("open"); }
function updateTaskIndicator(running) { $("open-task-drawer").classList.toggle("running", running); }
async function stopTask() {
  if (!confirm("确定停止当前任务吗？停止后临时数据不会合并到主数据库。")) return;
  try { const result = await api("/api/stop_task", { method: "POST" }); toast(result.message, result.success ? "ok" : "warning"); loadTaskDrawer(); } catch (error) { toast(error.message, "error"); }
}
async function loadFactors() {
  await loadDashboard();
  const stock = state.dashboard.tables.find((item) => item.table === "factor_rps_daily") || {};
  const etf = state.dashboard.tables.find((item) => item.table === "etf_factor_rps_daily") || {};
  $("factor-metrics").innerHTML = [
    metricHtml("股票因子记录", fmtCount(stock.count), stock.count ? "已生成" : "尚未计算"),
    metricHtml("股票覆盖区间", compactRange(stock.min_date, stock.max_date), "股票资产池"),
    metricHtml("ETF 因子记录", fmtCount(etf.count), etf.count ? "已生成" : "尚未计算"),
    metricHtml("ETF 覆盖区间", compactRange(etf.min_date, etf.max_date), "ETF 资产池")
  ].join("");
  try {
    const [stockLogs, etfLogs] = await Promise.all([api("/api/table?table=factor_update_log&page=0&page_size=20"), api("/api/table?table=etf_factor_update_log&page=0&page_size=20").catch(() => ({ columns: [], rows: [] }))]);
    const columns = ["asset_type", ...(stockLogs.columns || etfLogs.columns || [])];
    const rows = [...(stockLogs.rows || []).map((row) => ({ asset_type: "股票", ...row })), ...(etfLogs.rows || []).map((row) => ({ asset_type: "ETF", ...row }))];
    $("factor-thead").innerHTML = `<tr>${columns.map((column) => `<th>${esc(COLUMN_LABELS[column] || column)}</th>`).join("")}</tr>`;
    $("factor-tbody").innerHTML = rows.length ? rows.map((row) => `<tr>${columns.map((column) => `<td>${esc(formatCell(column, row[column]))}</td>`).join("")}</tr>`).join("") : `<tr><td colspan="${Math.max(1, columns.length)}">暂无因子计算记录</td></tr>`;
  } catch (error) { $("factor-tbody").innerHTML = `<tr><td>${esc(error.message)}</td></tr>`; }
}
async function loadQuality() {
  try {
    const [quality, logs] = await Promise.all([api("/api/data_quality"), api("/api/logs?limit=220")]);
    $("quality-score").textContent = quality.health_score;
    $("quality-issues").innerHTML = quality.issues.length ? quality.issues.map(issueHtml).join("") : issueHtml({ level: "info", title: "没有发现明显问题", detail: "本地数据状态良好。" });
    $("quality-tables").innerHTML = quality.tables.map((item) => `<tr><td>${esc(item.label)}</td><td>${item.asset_type === "stock" ? "股票" : "ETF"}${item.db_role === "factor" ? "因子" : "行情"}</td><td>${fmtCount(item.count)}</td><td>${esc(compactDate(item.min_date))}</td><td>${esc(compactDate(item.max_date))}</td><td>${item.count ? "可用" : "待补齐"}</td></tr>`).join("");
    renderQualityLogs(logs);
  } catch (error) { toast(`质量诊断加载失败：${error.message}`, "error"); }
}
async function repairLatestFields() {
  if (!confirm("将使用上一交易日收盘价和股票基础信息修复最新日的前收盘价、涨跌幅和 ST 标记。换手率仍需在 QMT 启动后通过增量更新补齐。确定继续吗？")) return;
  try {
    const data = await api("/api/repair_latest_derived_fields", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ asset_type: "stock", frequency: "d", confirm: true })
    });
    toast(`已修复 ${data.latest_date ? compactDate(data.latest_date) : "最新日"}：前收 ${fmtCount(data.preclose_repaired)} 条，涨跌幅 ${fmtCount(data.pctchg_repaired)} 条，ST ${fmtCount(data.st_repaired)} 条`);
    await loadDashboard(true);
    await loadQuality();
  } catch (error) { toast(error.message, "error"); }
}
let latestLogs = [];
function renderQualityLogs(logs) {
  latestLogs = [...(logs.logs || []), ...(logs.error_logs || []).map((line) => `[ERROR] ${line}`)];
  const filter = $("log-filter").value;
  const shown = latestLogs.filter((line) => filter === "all" || line.toLowerCase().includes(filter));
  updateLogText($("quality-log"), shown.join("\n") || "暂无符合条件的日志", $("log-autoscroll").checked);
}
function openDeleteModal() {
  $("delete-modal").classList.remove("hidden"); $("delete-table").innerHTML = $("browser-table").innerHTML; $("delete-table").value = $("browser-table").value;
  $("delete-code").value = ""; $("delete-start").value = $("browser-start").value; $("delete-end").value = $("browser-end").value; $("confirm-delete").disabled = true;
}
function closeDeleteModal() { $("delete-modal").classList.add("hidden"); }
function deletePayload() { return { table: $("delete-table").value, code: $("delete-code").value.trim(), start: $("delete-start").value, end: $("delete-end").value }; }
async function previewDelete() {
  try { const data = await api("/api/delete_preview", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(deletePayload()) }); $("delete-preview").textContent = `当前条件将删除 ${fmtCount(data.count)} 条记录。请确认范围无误。`; $("confirm-delete").disabled = data.count <= 0; }
  catch (error) { $("delete-preview").textContent = error.message; $("confirm-delete").disabled = true; }
}
async function confirmDelete() {
  const payload = deletePayload(); if (!confirm("删除操作无法撤销。确定删除预览范围内的数据吗？")) return;
  try { const data = await api("/api/delete", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...payload, confirm: true }) }); toast(`已删除 ${fmtCount(data.deleted)} 条记录`); closeDeleteModal(); loadBrowserTable(1); }
  catch (error) { toast(error.message, "error"); }
}
function wireEvents() {
  qsa("[data-page]").forEach((item) => item.addEventListener("click", () => setPage(item.dataset.page)));
  qsa("[data-page-link]").forEach((item) => item.addEventListener("click", () => setPage(item.dataset.pageLink)));
  qsa("[data-task]").forEach((item) => item.addEventListener("click", () => startTask(item.dataset.task, JSON.parse(item.dataset.body || "{}"), item.dataset.danger || "")));
  qsa("[data-quick-task]").forEach((item) => item.addEventListener("click", () => startTask("/api/daily_to_latest", { target: "stock" })));
  $("refresh-page").addEventListener("click", () => setPage(state.page)); $("open-task-drawer").addEventListener("click", openTaskDrawer); $("close-task-drawer").addEventListener("click", closeTaskDrawer);
  $("scrim").addEventListener("click", () => { closeTaskDrawer(); $("sidebar").classList.remove("open"); }); $("mobile-menu").addEventListener("click", () => { $("sidebar").classList.add("open"); $("scrim").classList.add("open"); });
  qsa("#market-assets button").forEach((item) => item.addEventListener("click", () => { qsa("#market-assets button").forEach((button) => button.classList.remove("active")); item.classList.add("active"); $("market-date").value = ""; loadMarket(); }));
  $("market-frequency").addEventListener("change", () => { $("market-date").value = ""; loadMarket(); }); $("load-market").addEventListener("click", loadMarket);
  $("research-input").addEventListener("input", loadSecurityOptions); $("research-asset").addEventListener("change", loadSecurityOptions); $("load-research").addEventListener("click", loadResearch); $("research-input").addEventListener("keydown", (event) => { if (event.key === "Enter") loadResearch(); });
  $("query-data").addEventListener("click", () => loadBrowserTable(1)); $("reset-query").addEventListener("click", () => { $("browser-keyword").value = ""; $("browser-start").value = ""; $("browser-end").value = ""; loadBrowserTable(1); });
  $("browser-keyword").addEventListener("keydown", (event) => { if (event.key === "Enter") loadBrowserTable(1); }); $("browser-table").addEventListener("change", () => loadBrowserTable(1));
  $("browser-prev").addEventListener("click", () => loadBrowserTable(state.browserPage - 1)); $("browser-next").addEventListener("click", () => loadBrowserTable(state.browserPage + 1)); $("export-data").addEventListener("click", exportData);
  $("log-filter").addEventListener("change", () => renderQualityLogs({ logs: latestLogs, error_logs: [] })); $("copy-log").addEventListener("click", async () => { await navigator.clipboard.writeText($("quality-log").textContent); toast("日志已复制"); });
  $("repair-latest-fields").addEventListener("click", repairLatestFields);
  $("open-delete-modal").addEventListener("click", openDeleteModal); qsa("[data-close-modal]").forEach((item) => item.addEventListener("click", closeDeleteModal)); $("preview-delete").addEventListener("click", previewDelete); $("confirm-delete").addEventListener("click", confirmDelete);
}
document.addEventListener("DOMContentLoaded", async () => {
  wireEvents(); await loadDashboard(); loadSecurityOptions();
  setInterval(() => { if (state.taskOpen) loadTaskDrawer(); }, 1600);
  setInterval(() => { if (state.page === "quality") loadQuality(); }, 15000);
});
