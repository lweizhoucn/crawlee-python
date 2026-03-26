"""Embedded HTML / CSS / JS for the Crawlee admin dashboard."""

from __future__ import annotations

DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Crawlee Dashboard</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Segoe UI',sans-serif;background:#f0f2f5;color:#222}
  header{background:#1a1a2e;color:#fff;padding:16px 24px;display:flex;align-items:center;gap:12px}
  header h1{font-size:1.4rem;font-weight:700}
  header span{font-size:.85rem;opacity:.7}
  .container{max-width:1100px;margin:24px auto;padding:0 16px}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
  .card{background:#fff;border-radius:8px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
  .card .label{font-size:.78rem;color:#888;text-transform:uppercase;letter-spacing:.05em}
  .card .value{font-size:2rem;font-weight:700;margin-top:4px;color:#1a1a2e}
  .panel{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:24px}
  .panel-header{padding:16px 20px;border-bottom:1px solid #f0f0f0;display:flex;align-items:center;justify-content:space-between}
  .panel-header h2{font-size:1rem;font-weight:600}
  .panel-body{padding:16px 20px}
  table{width:100%;border-collapse:collapse;font-size:.88rem}
  th{text-align:left;padding:8px 10px;font-weight:600;background:#fafafa;border-bottom:1px solid #eee}
  td{padding:8px 10px;border-bottom:1px solid #f5f5f5;vertical-align:middle}
  tr:last-child td{border-bottom:none}
  .badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.75rem;font-weight:600}
  .badge-pending{background:#fff3cd;color:#856404}
  .badge-running{background:#d1ecf1;color:#0c5460}
  .badge-done{background:#d4edda;color:#155724}
  .badge-failed{background:#f8d7da;color:#721c24}
  .badge-paused{background:#e2e3e5;color:#383d41}
  .badge-active{background:#d4edda;color:#155724}
  .btn{display:inline-block;padding:6px 14px;border-radius:6px;font-size:.83rem;cursor:pointer;border:none;font-weight:500;transition:opacity .15s}
  .btn:hover{opacity:.85}
  .btn-primary{background:#0d6efd;color:#fff}
  .btn-danger{background:#dc3545;color:#fff}
  .btn-sm{padding:4px 10px;font-size:.78rem}
  .form-row{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:12px}
  .form-group{display:flex;flex-direction:column;gap:4px;flex:1;min-width:180px}
  label{font-size:.82rem;font-weight:500;color:#555}
  input,select,textarea{border:1px solid #ced4da;border-radius:6px;padding:7px 10px;font-size:.9rem;width:100%}
  input:focus,select:focus,textarea:focus{outline:none;border-color:#0d6efd;box-shadow:0 0 0 2px rgba(13,110,253,.15)}
  .alert{padding:10px 14px;border-radius:6px;margin-bottom:14px;font-size:.88rem}
  .alert-success{background:#d4edda;color:#155724}
  .alert-error{background:#f8d7da;color:#721c24}
  .workers-dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px}
  .dot-active{background:#28a745}
  .dot-inactive{background:#dc3545}
  #live-indicator{width:10px;height:10px;border-radius:50%;background:#28a745;display:inline-block;margin-left:8px;animation:pulse 1.5s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
</style>
</head>
<body>
<header>
  <h1>🕷️ Crawlee Dashboard</h1>
  <span id="server-time"></span>
  <span id="live-indicator" title="Live updates active"></span>
</header>

<div class="container">

  <!-- Summary cards -->
  <div class="cards">
    <div class="card"><div class="label">Total Tasks</div><div class="value" id="stat-total">-</div></div>
    <div class="card"><div class="label">Running</div><div class="value" id="stat-running" style="color:#0c5460">-</div></div>
    <div class="card"><div class="label">Done</div><div class="value" id="stat-done" style="color:#155724">-</div></div>
    <div class="card"><div class="label">Failed</div><div class="value" id="stat-failed" style="color:#721c24">-</div></div>
    <div class="card"><div class="label">Workers Online</div><div class="value" id="stat-workers" style="color:#0d6efd">-</div></div>
  </div>

  <!-- Create Task -->
  <div class="panel">
    <div class="panel-header"><h2>Create New Crawl Task</h2></div>
    <div class="panel-body">
      <div id="task-alert" style="display:none"></div>
      <div class="form-row">
        <div class="form-group">
          <label>Task Name</label>
          <input id="f-name" type="text" placeholder="My crawl task"/>
        </div>
        <div class="form-group">
          <label>Crawler Type</label>
          <select id="f-type">
            <option value="http">HTTP</option>
            <option value="beautifulsoup">BeautifulSoup</option>
            <option value="parsel">Parsel</option>
            <option value="playwright">Playwright</option>
          </select>
        </div>
        <div class="form-group">
          <label>Max Requests</label>
          <input id="f-max" type="number" placeholder="unlimited" min="1"/>
        </div>
      </div>
      <div class="form-row">
        <div class="form-group" style="flex:2;min-width:100%">
          <label>Start URLs (one per line)</label>
          <textarea id="f-urls" rows="3" placeholder="https://example.com&#10;https://other.com"></textarea>
        </div>
      </div>
      <div class="form-row" style="gap:8px;margin-bottom:0">
        <button class="btn btn-primary" onclick="createTask()">🚀 Run on local worker</button>
        <button class="btn btn-primary" style="background:#6f42c1" onclick="publishTask()">📡 Publish to distributed workers</button>
      </div>
    </div>
  </div>

  <!-- Tasks table -->
  <div class="panel">
    <div class="panel-header">
      <h2>Tasks</h2>
      <button class="btn btn-sm btn-primary" onclick="loadTasks()">⟳ Refresh</button>
    </div>
    <div class="panel-body" style="padding:0">
      <table>
        <thead>
          <tr><th>Name</th><th>Type</th><th>Status</th><th>Finished</th><th>Failed</th><th>Req/min</th><th>Created</th><th>Actions</th></tr>
        </thead>
        <tbody id="tasks-tbody"><tr><td colspan="8" style="text-align:center;padding:20px;color:#888">Loading…</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- Workers table -->
  <div class="panel">
    <div class="panel-header">
      <h2>Registered Workers</h2>
      <button class="btn btn-sm btn-primary" onclick="loadWorkers()">⟳ Refresh</button>
    </div>
    <div class="panel-body" style="padding:0">
      <table>
        <thead>
          <tr><th>Worker ID</th><th>Hostname</th><th>Status</th><th>Tasks Processed</th><th>Last Heartbeat</th></tr>
        </thead>
        <tbody id="workers-tbody"><tr><td colspan="5" style="text-align:center;padding:20px;color:#888">Loading…</td></tr></tbody>
      </table>
    </div>
  </div>

</div>

<script>
const API = '';

function fmtDate(s){if(!s)return '-';const d=new Date(s);return d.toLocaleString();}
function badge(status){return `<span class="badge badge-${status}">${status}</span>`;}

async function apiFetch(path,opts={}){
  const r=await fetch(API+path,opts);
  if(!r.ok){const t=await r.text();throw new Error(t||r.statusText);}
  return r.json();
}

async function loadStats(){
  try{
    const d=await apiFetch('/api/stats');
    document.getElementById('stat-total').textContent=d.total;
    document.getElementById('stat-running').textContent=d.running;
    document.getElementById('stat-done').textContent=d.done;
    document.getElementById('stat-failed').textContent=d.failed;
    document.getElementById('stat-workers').textContent=d.workers_online;
  }catch(e){console.warn('stats',e);}
}

async function loadTasks(){
  try{
    const tasks=await apiFetch('/api/tasks');
    const tbody=document.getElementById('tasks-tbody');
    if(!tasks.length){tbody.innerHTML='<tr><td colspan="8" style="text-align:center;padding:20px;color:#888">No tasks yet</td></tr>';return;}
    tbody.innerHTML=tasks.map(t=>`
      <tr>
        <td><strong>${esc(t.name)}</strong></td>
        <td>${esc(t.crawler_type)}</td>
        <td>${badge(t.status)}</td>
        <td>${t.requests_finished}</td>
        <td>${t.requests_failed}</td>
        <td>${t.requests_per_minute.toFixed(1)}</td>
        <td>${fmtDate(t.created_at)}</td>
        <td>
          ${t.status==='running'?`<button class="btn btn-sm" style="background:#ffc107;color:#333" onclick="pauseTask('${t.id}')">⏸</button>`:''}
          ${t.status==='paused'?`<button class="btn btn-sm btn-primary" onclick="resumeTask('${t.id}')">▶</button>`:''}
          <button class="btn btn-sm btn-danger" onclick="stopTask('${t.id}')">■</button>
        </td>
      </tr>
    `).join('');
  }catch(e){console.warn('tasks',e);}
}

async function loadWorkers(){
  try{
    const workers=await apiFetch('/api/workers');
    const tbody=document.getElementById('workers-tbody');
    if(!workers.length){tbody.innerHTML='<tr><td colspan="5" style="text-align:center;padding:20px;color:#888">No workers registered</td></tr>';return;}
    tbody.innerHTML=workers.map(w=>`
      <tr>
        <td><code>${esc(w.worker_id)}</code></td>
        <td>${esc(w.hostname)}</td>
        <td><span class="workers-dot dot-${w.status==='active'?'active':'inactive'}"></span>${w.status}</td>
        <td>${w.tasks_processed}</td>
        <td>${fmtDate(w.last_heartbeat)}</td>
      </tr>
    `).join('');
  }catch(e){console.warn('workers',e);}
}

function esc(s){const d=document.createElement('div');d.textContent=s||'';return d.innerHTML;}

function getForm(){
  const name=document.getElementById('f-name').value.trim();
  const type=document.getElementById('f-type').value;
  const maxRaw=document.getElementById('f-max').value;
  const urls=document.getElementById('f-urls').value.split('\\n').map(s=>s.trim()).filter(Boolean);
  if(!name||!urls.length)throw new Error('Task name and at least one URL are required');
  return{name,crawler_type:type,start_urls:urls,max_requests:maxRaw?parseInt(maxRaw,10):null,extra:{}};
}

function showAlert(msg,ok){
  const el=document.getElementById('task-alert');
  el.className='alert '+(ok?'alert-success':'alert-error');
  el.textContent=msg;
  el.style.display='block';
  setTimeout(()=>{el.style.display='none';},4000);
}

async function createTask(){
  try{
    const task=getForm();
    await apiFetch('/api/tasks',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(task)});
    showAlert('Task created successfully!',true);
    loadTasks();loadStats();
  }catch(e){showAlert('Error: '+e.message,false);}
}

async function publishTask(){
  try{
    const task=getForm();
    await apiFetch('/api/publish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({task})});
    showAlert('Task published to distributed workers!',true);
    loadTasks();loadStats();
  }catch(e){showAlert('Error: '+e.message,false);}
}

async function pauseTask(id){
  try{await apiFetch(`/api/tasks/${id}/pause`,{method:'POST'});loadTasks();}
  catch(e){alert('Failed to pause: '+e.message);}
}
async function resumeTask(id){
  try{await apiFetch(`/api/tasks/${id}/resume`,{method:'POST'});loadTasks();}
  catch(e){alert('Failed to resume: '+e.message);}
}
async function stopTask(id){
  if(!confirm('Stop this task?'))return;
  try{await apiFetch(`/api/tasks/${id}`,{method:'DELETE'});loadTasks();loadStats();}
  catch(e){alert('Failed to stop: '+e.message);}
}

// Live updates via SSE
function startSSE(){
  const es=new EventSource('/api/events');
  es.onmessage=e=>{
    const d=JSON.parse(e.data);
    if(d.type==='stats')updateStatCards(d);
    if(d.type==='tasks_updated'){loadTasks();loadStats();}
  };
  es.onerror=()=>{setTimeout(startSSE,3000);es.close();};
}

function updateStatCards(d){
  if(d.total!==undefined)document.getElementById('stat-total').textContent=d.total;
  if(d.running!==undefined)document.getElementById('stat-running').textContent=d.running;
  if(d.done!==undefined)document.getElementById('stat-done').textContent=d.done;
  if(d.failed!==undefined)document.getElementById('stat-failed').textContent=d.failed;
  if(d.workers_online!==undefined)document.getElementById('stat-workers').textContent=d.workers_online;
}

function tick(){document.getElementById('server-time').textContent=new Date().toLocaleTimeString();}

tick();setInterval(tick,1000);
loadStats();loadTasks();loadWorkers();
setInterval(()=>{loadStats();loadTasks();loadWorkers();},10000);
startSSE();
</script>
</body>
</html>
"""
