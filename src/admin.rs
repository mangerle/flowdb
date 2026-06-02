pub const ADMIN_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FlowDB Admin</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; padding: 12px; font-size: 13px; }
h1 { color: #38bdf8; font-size: 20px; margin-bottom: 8px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 8px; margin-bottom: 12px; }
.card { background: #1e293b; border-radius: 8px; padding: 10px 14px; border: 1px solid #334155; }
.card .label { font-size: 10px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
.card .value { font-size: 22px; font-weight: 700; color: #f1f5f9; }
.card .value.green { color: #4ade80; }
.card .value.blue { color: #38bdf8; }
.card .value.yellow { color: #fbbf24; }
.card .value.red { color: #f87171; }
.section { background: #1e293b; border-radius: 8px; padding: 12px 14px; border: 1px solid #334155; margin-bottom: 12px; }
.section h2 { font-size: 14px; color: #38bdf8; margin-bottom: 8px; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #334155; }
th { color: #94a3b8; font-weight: 600; font-size: 11px; }
td { color: #cbd5e1; }
.bar-container { background: #334155; border-radius: 4px; height: 6px; overflow: hidden; }
.bar { height: 100%; border-radius: 4px; transition: width 0.3s; }
.bar.green { background: #4ade80; }
.bar.blue { background: #38bdf8; }
.bar.yellow { background: #fbbf24; }
.actions { display: flex; gap: 8px; flex-wrap: wrap; }
.btn { padding: 6px 14px; border: none; border-radius: 6px; font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
.btn:hover { transform: translateY(-1px); }
.btn.flush { background: #3b82f6; color: white; }
.btn.gc { background: #f59e0b; color: #0f172a; }
.btn.compact { background: #8b5cf6; color: white; }
.btn.query { background: #3b82f6; color: white; }
.btn.delete { background: #ef4444; color: white; }
.btn.patch { background: #10b981; color: white; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
#toast { position: fixed; top: 12px; right: 12px; padding: 8px 16px; border-radius: 6px; font-size: 12px; font-weight: 600; display: none; z-index: 999; }
#toast.success { display: block; background: #4ade80; color: #0f172a; }
#toast.error { display: block; background: #f87171; color: white; }
.refresh-info { font-size: 11px; color: #64748b; margin-bottom: 8px; }
.tabs { display: flex; gap: 0; margin-bottom: 10px; border-bottom: 1px solid #334155; }
.tab { padding: 6px 16px; cursor: pointer; font-size: 12px; font-weight: 600; color: #94a3b8; border-bottom: 2px solid transparent; transition: all 0.15s; }
.tab:hover { color: #e2e8f0; }
.tab.active { color: #38bdf8; border-bottom-color: #38bdf8; }
.tab-content { display: none; }
.tab-content.active { display: block; }
.form-row { display: flex; gap: 8px; margin-bottom: 8px; flex-wrap: wrap; align-items: end; }
.form-field { display: flex; flex-direction: column; gap: 3px; }
.form-field label { font-size: 10px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.3px; }
.form-field input { padding: 6px 8px; background: #0f172a; border: 1px solid #334155; border-radius: 4px; color: #e2e8f0; font-size: 12px; min-width: 100px; }
.form-field input:focus { outline: none; border-color: #38bdf8; }
.form-field input.small { min-width: 80px; width: 120px; }
.op-status { font-size: 11px; color: #94a3b8; margin-bottom: 6px; }
.latency-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; }
.latency-group { margin-bottom: 6px; }
.latency-group .title { font-size: 10px; color: #64748b; text-transform: uppercase; margin-bottom: 3px; }
.latency-row { display: flex; gap: 10px; }
.latency-item { flex: 1; text-align: center; }
.latency-item .pct { font-size: 10px; color: #94a3b8; }
.latency-item .val { font-size: 16px; font-weight: 700; color: #38bdf8; }
.query-table-wrap { max-height: 350px; overflow: auto; }
.query-table-wrap table { font-size: 11px; }
.query-table-wrap th { position: sticky; top: 0; background: #1e293b; }
</style>
</head>
<body>
<h1>FlowDB Admin</h1>
<div class="refresh-info">Auto-refresh every 2s | Last update: <span id="lastUpdate">-</span></div>

<div class="grid">
  <div class="card"><div class="label">Records Written</div><div class="value blue" id="totalWritten">0</div></div>
  <div class="card"><div class="label">Records Read</div><div class="value blue" id="totalRead">0</div></div>
  <div class="card"><div class="label">Bytes Written</div><div class="value" id="bytesWritten">0</div></div>
  <div class="card"><div class="label">Records Expired</div><div class="value yellow" id="totalExpired">0</div></div>
  <div class="card"><div class="label">Uptime</div><div class="value green" id="uptime">0s</div></div>
  <div class="card"><div class="label">SSTables</div><div class="value" id="sstCount">0</div></div>
</div>

<div class="grid">
  <div class="card"><div class="label">MemTable Records</div><div class="value" id="mtRecords">0</div></div>
  <div class="card"><div class="label">MemTable Bytes</div><div class="value" id="mtBytes">0</div></div>
  <div class="card"><div class="label">Frozen MemTables</div><div class="value" id="frozenCount">0</div></div>
  <div class="card"><div class="label">SSTable Bytes</div><div class="value" id="sstBytes">0</div></div>
  <div class="card"><div class="label">WAL Bytes</div><div class="value" id="walBytes">0</div></div>
  <div class="card"><div class="label">Cache Hit Rate</div><div class="value green" id="cacheHit">0%</div></div>
  <div class="card"><div class="label">Compression</div><div class="value" id="compRatio">-</div></div>
  <div class="card"><div class="label">Index Entries</div><div class="value" id="idxEntries">0</div></div>
  <div class="card"><div class="label">Time Buckets</div><div class="value" id="timeBuckets">0</div></div>
</div>

<div class="grid">
  <div class="card">
    <div class="label">Write Latency</div>
    <div style="display:flex;gap:10px;margin-top:4px">
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P50</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="wP50">-</span></div>
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P90</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="wP90">-</span></div>
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P99</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="wP99">-</span></div>
    </div>
  </div>
  <div class="card">
    <div class="label">Query Latency</div>
    <div style="display:flex;gap:10px;margin-top:4px">
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P50</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="qP50">-</span></div>
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P90</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="qP90">-</span></div>
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P99</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="qP99">-</span></div>
    </div>
  </div>
  <div class="card">
    <div class="label">Flush Latency</div>
    <div style="display:flex;gap:10px;margin-top:4px">
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P50</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="fP50">-</span></div>
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P90</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="fP90">-</span></div>
      <div style="flex:1;text-align:center"><span style="font-size:10px;color:#94a3b8">P99</span><br><span style="font-size:16px;font-weight:700;color:#38bdf8" id="fP99">-</span></div>
    </div>
  </div>
</div>

<div class="section">
  <h2>Actions</h2>
  <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <button class="btn flush" onclick="doAction('flush')">Flush</button>
    <button class="btn gc" onclick="doAction('gc')">GC</button>
    <button class="btn compact" onclick="doAction('compact')">Compact</button>
    <table style="width:auto;margin-left:auto;font-size:11px">
      <tr><th>Flushes</th><th>GC</th><th>Compact</th><th>Purged</th><th>HTTP</th><th>UDP Recv</th><th>UDP Drop</th></tr>
      <tr><td id="totalFlushes">0</td><td id="totalGc">0</td><td id="totalCompact">0</td><td id="purgedGc">0</td><td id="httpReqs">0</td><td id="udpRecv">0</td><td id="udpDrop">0</td></tr>
    </table>
  </div>
</div>

<div class="section">
  <div class="tabs">
    <div class="tab active" onclick="switchTab('query')">Query</div>
    <div class="tab" onclick="switchTab('delete')">Delete</div>
    <div class="tab" onclick="switchTab('patch')">Patch</div>
  </div>

  <div id="tab-query" class="tab-content active">
    <div class="form-row">
      <div class="form-field">
        <label>Prefix</label>
        <input id="qPrefix" placeholder="e.g. call-">
      </div>
      <div class="form-field">
        <label>Key Start</label>
        <input id="qKeyStart" placeholder="start" class="small">
      </div>
      <div class="form-field">
        <label>Key End</label>
        <input id="qKeyEnd" placeholder="end" class="small">
      </div>
      <div class="form-field">
        <label>Time Start (μs)</label>
        <input id="qTsStart" type="number" placeholder="ts_start" class="small">
      </div>
      <div class="form-field">
        <label>Time End (μs)</label>
        <input id="qTsEnd" type="number" placeholder="ts_end" class="small">
      </div>
      <div class="form-field">
        <button class="btn query" onclick="doQuery()" style="margin-top:13px">Query</button>
      </div>
    </div>
    <div class="op-status" id="queryStatus"></div>
    <div class="query-table-wrap">
      <table id="queryTable" style="display:none">
        <thead><tr><th>Key</th><th>Timestamp</th><th>Expire At</th><th>Value (base64)</th></tr></thead>
        <tbody id="queryBody"></tbody>
      </table>
    </div>
  </div>

  <div id="tab-delete" class="tab-content">
    <div class="form-row">
      <div class="form-field">
        <label>Key</label>
        <input id="delKey" placeholder="record key">
      </div>
      <div class="form-field">
        <label>Timestamp (μs)</label>
        <input id="delTs" type="number" placeholder="ts">
      </div>
      <div class="form-field">
        <button class="btn delete" onclick="doDelete()" style="margin-top:13px">Delete</button>
      </div>
    </div>
    <div class="op-status" id="deleteStatus"></div>
  </div>

  <div id="tab-patch" class="tab-content">
    <div class="form-row">
      <div class="form-field">
        <label>Key</label>
        <input id="patKey" placeholder="record key">
      </div>
      <div class="form-field">
        <label>Timestamp (μs)</label>
        <input id="patTs" type="number" placeholder="ts">
      </div>
      <div class="form-field" style="flex:1">
        <label>Value</label>
        <input id="patValue" placeholder="new value (plain text)">
      </div>
      <div class="form-field" style="flex:1">
        <label>or Value (base64)</label>
        <input id="patValueB64" placeholder="new value (base64)">
      </div>
      <div class="form-field">
        <button class="btn patch" onclick="doPatch()" style="margin-top:13px">Patch</button>
      </div>
    </div>
    <div class="op-status" id="patchStatus"></div>
  </div>
</div>

<div id="toast"></div>

<script>
let currentTab = 'query';

function switchTab(tab) {
  currentTab = tab;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelector(`.tab:nth-child(${tab === 'query' ? 1 : tab === 'delete' ? 2 : 3})`).classList.add('active');
  document.getElementById('tab-' + tab).classList.add('active');
}

function fmt(n) {
  if (n >= 1e9) return (n/1e9).toFixed(2)+'G';
  if (n >= 1e6) return (n/1e6).toFixed(2)+'M';
  if (n >= 1e3) return (n/1e3).toFixed(1)+'K';
  return n.toString();
}
function fmtBytes(n) {
  if (n >= 1e9) return (n/1e9).toFixed(2)+' GB';
  if (n >= 1e6) return (n/1e6).toFixed(2)+' MB';
  if (n >= 1e3) return (n/1e3).toFixed(1)+' KB';
  return n+' B';
}
function fmtUptime(s) {
  if (s >= 86400) return Math.floor(s/86400)+'d '+Math.floor((s%86400)/3600)+'h';
  if (s >= 3600) return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m';
  if (s >= 60) return Math.floor(s/60)+'m '+(s%60)+'s';
  return s+'s';
}
async function refresh() {
  try {
    let r = await fetch('/stats');
    let s = await r.json();
    document.getElementById('totalWritten').textContent = fmt(s.total_records_written);
    document.getElementById('totalRead').textContent = fmt(s.total_records_read);
    document.getElementById('bytesWritten').textContent = fmtBytes(s.total_bytes_written);
    document.getElementById('totalExpired').textContent = fmt(s.total_records_expired);
    document.getElementById('uptime').textContent = fmtUptime(s.uptime_secs);
    document.getElementById('sstCount').textContent = s.sstable_count;
    document.getElementById('mtRecords').textContent = fmt(s.memtable_records);
    document.getElementById('mtBytes').textContent = fmtBytes(s.memtable_bytes);
    document.getElementById('frozenCount').textContent = s.frozen_memtable_count;
    document.getElementById('sstBytes').textContent = fmtBytes(s.sstable_bytes);
    document.getElementById('walBytes').textContent = fmtBytes(s.wal_bytes);
    document.getElementById('cacheHit').textContent = (s.block_cache_hit_rate*100).toFixed(1)+'%';
    document.getElementById('compRatio').textContent = (s.compression_ratio*100).toFixed(1)+'%';
    document.getElementById('idxEntries').textContent = fmt(s.block_meta_index_entries);
    document.getElementById('timeBuckets').textContent = fmt(s.time_index_buckets);
    document.getElementById('wP50').textContent = fmt(s.write_latency_p50_us);
    document.getElementById('wP90').textContent = fmt(s.write_latency_p90_us);
    document.getElementById('wP99').textContent = fmt(s.write_latency_p99_us);
    document.getElementById('qP50').textContent = fmt(s.query_latency_p50_us);
    document.getElementById('qP90').textContent = fmt(s.query_latency_p90_us);
    document.getElementById('qP99').textContent = fmt(s.query_latency_p99_us);
    document.getElementById('fP50').textContent = fmt(s.flush_latency_p50_us);
    document.getElementById('fP90').textContent = fmt(s.flush_latency_p90_us);
    document.getElementById('fP99').textContent = fmt(s.flush_latency_p99_us);
    document.getElementById('totalFlushes').textContent = fmt(s.total_flushes);
    document.getElementById('totalGc').textContent = fmt(s.total_gc_runs);
    document.getElementById('totalCompact').textContent = fmt(s.total_compaction_runs);
    document.getElementById('purgedGc').textContent = fmt(s.records_purged_by_gc);
    document.getElementById('httpReqs').textContent = fmt(s.http_requests_total);
    document.getElementById('udpRecv').textContent = fmt(s.udp_packets_received);
    document.getElementById('udpDrop').textContent = fmt(s.udp_packets_dropped);
    document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
  } catch(e) { console.error(e); }
}
async function doAction(action) {
  let btns = document.querySelectorAll('.btn');
  btns.forEach(b => b.disabled = true);
  let toast = document.getElementById('toast');
  try {
    let r = await fetch('/admin/'+action, {method:'POST'});
    if (r.ok) {
      let msg = action.charAt(0).toUpperCase()+action.slice(1)+' triggered';
      toast.textContent = msg;
      toast.className = 'success';
    } else {
      toast.textContent = 'Error: '+r.status;
      toast.className = 'error';
    }
  } catch(e) {
    toast.textContent = 'Error: '+e.message;
    toast.className = 'error';
  }
  setTimeout(() => { toast.className = ''; }, 2000);
  await refresh();
  btns.forEach(b => b.disabled = false);
}
async function doQuery() {
  let params = new URLSearchParams();
  let prefix = document.getElementById('qPrefix').value.trim();
  let keyStart = document.getElementById('qKeyStart').value.trim();
  let keyEnd = document.getElementById('qKeyEnd').value.trim();
  let tsStart = document.getElementById('qTsStart').value.trim();
  let tsEnd = document.getElementById('qTsEnd').value.trim();
  if (prefix) params.set('prefix', prefix);
  if (keyStart) params.set('key_start', keyStart);
  if (keyEnd) params.set('key_end', keyEnd);
  if (tsStart) params.set('ts_start', tsStart);
  if (tsEnd) params.set('ts_end', tsEnd);
  let statusEl = document.getElementById('queryStatus');
  let tableEl = document.getElementById('queryTable');
  let bodyEl = document.getElementById('queryBody');
  statusEl.textContent = 'Loading...';
  try {
    let r = await fetch('/admin/query?' + params.toString());
    let data = await r.json();
    statusEl.textContent = data.count + ' record(s) found';
    bodyEl.innerHTML = '';
    for (let rec of data.records) {
      let tr = document.createElement('tr');
      let decoded = '';
      try { decoded = atob(rec.value); } catch(e) { decoded = rec.value; }
      tr.innerHTML = '<td>' + escHtml(rec.key) + '</td><td>' + rec.ts + '</td><td>' + rec.expire_at + '</td><td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + escHtml(decoded) + '">' + escHtml(decoded) + '</td>';
      bodyEl.appendChild(tr);
    }
    tableEl.style.display = data.count > 0 ? '' : 'none';
  } catch(e) {
    statusEl.textContent = 'Error: ' + e.message;
    tableEl.style.display = 'none';
  }
}
async function doDelete() {
  let key = document.getElementById('delKey').value.trim();
  let ts = document.getElementById('delTs').value.trim();
  if (!key || !ts) { document.getElementById('deleteStatus').textContent = 'Key and timestamp required'; return; }
  let statusEl = document.getElementById('deleteStatus');
  statusEl.textContent = 'Deleting...';
  try {
    let r = await fetch('/admin/delete', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({key, ts: parseInt(ts)})});
    let data = await r.json();
    statusEl.textContent = data.message;
    if (r.ok) showToast('Deleted: ' + key, 'success');
    else showToast('Error: ' + data.message, 'error');
  } catch(e) {
    statusEl.textContent = 'Error: ' + e.message;
    showToast('Error: ' + e.message, 'error');
  }
}
async function doPatch() {
  let key = document.getElementById('patKey').value.trim();
  let ts = document.getElementById('patTs').value.trim();
  let value = document.getElementById('patValue').value;
  let valueB64 = document.getElementById('patValueB64').value.trim();
  if (!key || !ts) { document.getElementById('patchStatus').textContent = 'Key and timestamp required'; return; }
  let body = {key, ts: parseInt(ts)};
  if (valueB64) body.value_base64 = valueB64;
  else if (value) body.value = value;
  let statusEl = document.getElementById('patchStatus');
  statusEl.textContent = 'Patching...';
  try {
    let r = await fetch('/admin/patch', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
    let data = await r.json();
    statusEl.textContent = data.message;
    if (r.ok) showToast('Patched: ' + key, 'success');
    else showToast('Error: ' + data.message, 'error');
  } catch(e) {
    statusEl.textContent = 'Error: ' + e.message;
    showToast('Error: ' + e.message, 'error');
  }
}
function showToast(msg, type) {
  let t = document.getElementById('toast');
  t.textContent = msg;
  t.className = type;
  setTimeout(() => { t.className = ''; }, 2000);
}
function escHtml(s) {
  let d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>"##;
