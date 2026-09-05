"""
video/dashboard.py — approval dashboard + content dashboard (spec §16, §25).

Deliberately dependency-free (stdlib http.server): a single-page UI over a
small JSON API. Runs as its own process, never inside the worker:

    python -m video.dashboard          # port 8321, or DVE_DASHBOARD_PORT

Auth: when DVE_DASHBOARD_TOKEN is set, every request needs
Authorization: Bearer <token> (the UI prompts once and stores it in
sessionStorage). Set it anywhere the dashboard is reachable beyond localhost.

API:
  GET  /api/summary                    TODAY / PERFORMANCE / TOP / INSIGHTS
  GET  /api/queue?tab=needs_review     review queues (§16 tabs)
  GET  /api/video/{id}                 full review payload
  POST /api/action                     {action, video_id | video_ids, actor, ...}
       actions: approve | reject | regenerate | batch_approve | publish |
                schedule | recommendation_decide
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from video import learning, pipeline, scoring, settings
from video.store import store

TABS = {
    "ideas": ("opportunities", {"status": "open"}),
    "scripts": ("scripts", {"status": "draft"}),
    "videos": ("videos", None),
    "needs_review": ("videos", {"status": "needs_review"}),
    "approved": ("videos", {"status": "approved"}),
    "scheduled": ("videos", {"status": "scheduled"}),
    "published": ("videos", {"status": "published"}),
    "failed": ("videos", {"status": "failed"}),
    "archived": ("videos", {"status": "archived"}),
}


def summary() -> dict:
    s = store()
    videos = s.find("videos")
    by_status: dict[str, int] = {}
    for v in videos:
        by_status[v.get("status", "?")] = by_status.get(v.get("status", "?"), 0) + 1

    published = [v for v in videos if v.get("status") == "published"]
    total_views = 0
    completions = []
    for v in published:
        m = scoring.latest_metrics(v["id"]) or {}
        total_views += m.get("views", 0)
        if m.get("completion_rate"):
            completions.append(m["completion_rate"])

    dims = scoring.dimension_scores()

    def top(dim, k=3):
        return sorted(dims.get(dim, {}).items(),
                      key=lambda kv: kv[1]["avg_score"], reverse=True)[:k]

    recs = s.find("recommendations", {"status": "open"})
    return {
        "today": {
            "ideas_open": len(s.find("opportunities", {"status": "open"})),
            "awaiting_approval": by_status.get("needs_review", 0),
            "approved": by_status.get("approved", 0),
            "scheduled": by_status.get("scheduled", 0),
            "published": by_status.get("published", 0),
            "failed": by_status.get("failed", 0),
        },
        "performance": {
            "videos_published": len(published),
            "total_views": total_views,
            "avg_completion": round(sum(completions) / len(completions), 1) if completions else None,
        },
        "top_content": {
            "videos": sorted(
                ({"id": v["id"], "title": v.get("title"), "score": v.get("video_score")}
                 for v in published if v.get("video_score")),
                key=lambda x: x["score"], reverse=True)[:5],
            "destinations": top("destination"),
            "hooks": top("hook_category"),
            "franchises": top("franchise"),
            "lengths": top("length"),
            "voices": top("voice"),
        },
        "ai_insights": [
            {"id": r["id"], "kind": r["kind"], "finding": r["finding"],
             "action": r.get("suggested_action", "")}
            for r in recs
        ],
    }


def video_detail(video_id: str) -> dict | None:
    s = store()
    v = s.get("videos", video_id)
    if not v:
        return None
    qa_history = s.find("qa_results", {"video_id": video_id})
    return {
        "video": v,
        "concept": s.get("concepts", v.get("concept_id", "")),
        "script": s.get("scripts", v.get("script_id", "")),
        "hook": s.get("hooks", v.get("hook_id", "")),
        "hooks_available": s.find("hooks", {"concept_id": v.get("concept_id", "")}),
        "source": s.get("content_sources", v.get("source_id", "")),
        "assets": [s.get("assets", a) for a in v.get("asset_ids", []) if a],
        "qa": qa_history[-1] if qa_history else None,
        "approvals": s.find("approval_events", {"video_id": video_id}),
    }


def handle_action(body: dict) -> dict:
    action = body.get("action", "")
    actor = body.get("actor") or "dashboard"
    vid = body.get("video_id", "")
    if action == "approve":
        return pipeline.approve(vid, actor)
    if action == "reject":
        return pipeline.reject(vid, actor, body.get("reason", ""))
    if action == "regenerate":
        return pipeline.regenerate(vid, actor, change=body.get("change", ""),
                                   hook_id=body.get("hook_id"),
                                   voice_id=body.get("voice_id"))
    if action == "batch_approve":
        return {"approved": [v["id"] for v in
                             pipeline.batch_approve(body.get("video_ids", []), actor)]}
    if action in ("publish", "schedule"):
        from video import youtube
        return youtube.publish(vid, visibility=body.get("visibility", "public"),
                               playlist=body.get("playlist", ""),
                               schedule_at=body.get("schedule_at", ""))
    if action == "recommendation_decide":
        return learning.decide(body.get("recommendation_id", ""),
                               bool(body.get("accept")), actor)
    if action == "edit":
        s = store()
        v = s.get("videos", vid)
        if not v:
            raise KeyError(vid)
        for field in ("title", "description", "tags", "scheduled_at"):
            if field in body:
                v[field] = body[field]
        return s.put("videos", v)
    raise ValueError(f"unknown action {action!r}")


_HTML = """<!doctype html><meta charset=utf-8><title>Destination Video Engine</title>
<style>
body{font:14px/1.5 -apple-system,system-ui,sans-serif;margin:0;background:#f5f4f1;color:#101418}
header{background:#101418;color:#fff;padding:12px 24px;display:flex;gap:16px;align-items:baseline}
header h1{font-size:16px;margin:0}nav{display:flex;gap:4px;padding:12px 24px;flex-wrap:wrap}
nav button{border:1px solid #ccc;background:#fff;padding:6px 12px;border-radius:16px;cursor:pointer}
nav button.on{background:#0E7C66;color:#fff;border-color:#0E7C66}
main{padding:0 24px 48px;max-width:1100px}
.cards{display:flex;gap:12px;flex-wrap:wrap;margin:12px 0}
.card{background:#fff;border-radius:8px;padding:12px 16px;min-width:110px;box-shadow:0 1px 2px #0002}
.card b{display:block;font-size:22px}
table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden}
td,th{padding:8px 12px;border-bottom:1px solid #eee;text-align:left;vertical-align:top}
.btn{border:0;padding:5px 10px;border-radius:6px;cursor:pointer;margin-right:4px}
.ok{background:#0E7C66;color:#fff}.no{background:#C8402A;color:#fff}.mut{background:#e5e2dc}
.badge{padding:2px 8px;border-radius:10px;font-size:12px}
.pass{background:#DFF3EE}.warning{background:#FBEFC7}.fail{background:#F8D7D0}
pre{white-space:pre-wrap;background:#f8f7f4;padding:8px;border-radius:6px;font-size:12px}
#detail{background:#fff;border-radius:8px;padding:16px;margin-top:16px}
</style>
<header><h1>Destination Video Engine</h1><span id=mode></span></header>
<nav id=tabs></nav><main>
<div id=summary></div><div id=list></div><div id=detail hidden></div>
</main>
<script>
const TABS=["needs_review","ideas","scripts","approved","scheduled","published","failed","archived","videos"];
let token=sessionStorage.getItem("dve_token")||"";
async function api(p,opts={}){
  opts.headers=Object.assign({"Content-Type":"application/json"},opts.headers||{});
  if(token)opts.headers["Authorization"]="Bearer "+token;
  const r=await fetch(p,opts);
  if(r.status===401){token=prompt("Dashboard token:")||"";sessionStorage.setItem("dve_token",token);return api(p,opts)}
  if(!r.ok)throw new Error(await r.text());return r.json()}
function esc(x){const d=document.createElement("div");d.textContent=x==null?"":String(x);return d.innerHTML}
async function loadSummary(){
  const s=await api("/api/summary");
  const t=s.today,p=s.performance;
  let h="<div class=cards>";
  for(const[k,v]of Object.entries(t))h+=`<div class=card><b>${v}</b>${esc(k.replace(/_/g," "))}</div>`;
  h+=`<div class=card><b>${p.total_views}</b>views</div>`;
  if(p.avg_completion!=null)h+=`<div class=card><b>${p.avg_completion}%</b>avg completion</div>`;
  h+="</div>";
  if(s.ai_insights.length){h+="<h3>AI insights</h3><table>";
    for(const r of s.ai_insights)h+=`<tr><td>${esc(r.finding)}<br><i>${esc(r.action)}</i></td>
      <td><button class="btn ok" onclick="decide('${r.id}',true)">Accept</button>
      <button class="btn mut" onclick="decide('${r.id}',false)">Reject</button></td></tr>`;
    h+="</table>"}
  document.getElementById("summary").innerHTML=h}
async function decide(id,accept){await api("/api/action",{method:"POST",body:JSON.stringify({action:"recommendation_decide",recommendation_id:id,accept})});loadSummary()}
let tab="needs_review";
async function loadTab(t){tab=t;
  document.querySelectorAll("nav button").forEach(b=>b.classList.toggle("on",b.dataset.t===t));
  document.getElementById("detail").hidden=true;
  const rows=await api("/api/queue?tab="+t);
  let h=`<h3>${esc(t.replace(/_/g," "))} (${rows.length})</h3>`;
  if(t==="needs_review"&&rows.length)h+=`<button class="btn ok" onclick="batchApprove()">Batch approve all QA-pass</button>`;
  h+="<table>";
  for(const r of rows){
    if(r.title!==undefined||r.franchise!==undefined){
      h+=`<tr><td><a href=# onclick="openVideo('${r.id}');return false"><b>${esc(r.title||r.id)}</b></a><br>
        ${esc(r.franchise||"")} · ${esc(r.destination||"")} · pred ${r.predicted_score??""} · score ${r.video_score??""}</td>
        <td><span class="badge ${esc(r.qa_result||"")}">${esc(r.qa_result||"")}</span></td>
        <td>${esc((r.status||""))}</td></tr>`
    }else{
      h+=`<tr><td><b>${esc(r.score??"")}</b></td><td>${esc(r.title||r.working_title||r.id)}<br>
        <small>${esc((r.reasons||[]).join(" · "))}</small></td></tr>`}}
  h+="</table>";document.getElementById("list").innerHTML=h}
async function batchApprove(){
  const rows=await api("/api/queue?tab=needs_review");
  const ids=rows.filter(r=>r.qa_result==="pass").map(r=>r.id);
  if(!ids.length)return alert("No QA-pass videos to approve");
  await api("/api/action",{method:"POST",body:JSON.stringify({action:"batch_approve",video_ids:ids})});
  loadTab(tab);loadSummary()}
async function openVideo(id){
  const d=await api("/api/video/"+id);const v=d.video,q=d.qa;
  let h=`<h3>${esc(v.title)}</h3>
  <p>${esc(v.franchise)} · ${esc(v.destination)} · ${v.duration_seconds||"?"}s · voice ${esc(v.voice_id)} ·
   QA <span class="badge ${esc(v.qa_result)}">${esc(v.qa_result)}</span> · status ${esc(v.status)}</p>`;
  if(v.file_path)h+=`<p><code>${esc(v.file_path)}</code></p>`;
  h+=`<p>
   <button class="btn ok" onclick="act('approve','${v.id}')">Approve</button>
   <button class="btn no" onclick="act('reject','${v.id}')">Reject</button>
   <button class="btn mut" onclick="act('regenerate','${v.id}')">Regenerate</button>`;
  if(v.status==="approved")h+=`<button class="btn ok" onclick="act('publish','${v.id}')">Publish</button>`;
  h+=`</p>`;
  if(d.hooks_available&&d.hooks_available.length>1){h+="<p>Hooks: ";
    for(const hk of d.hooks_available)h+=`<button class="btn mut" title="${esc(hk.category)}"
      onclick="regenHook('${v.id}','${hk.id}')">${esc(hk.text)}</button> `;h+="</p>"}
  if(q){h+="<h4>QA</h4><table>";
    for(const c of q.checks)h+=`<tr><td>${esc(c.name)}</td>
      <td><span class="badge ${esc(c.verdict)}">${esc(c.verdict)}</span></td><td>${esc(c.detail)}</td></tr>`;
    h+="</table>"}
  if(d.script)h+=`<h4>Script</h4><pre>${esc(d.script.voiceover)}</pre>
    <h4>Shot list</h4><pre>${esc(JSON.stringify(d.script.shot_list,null,1))}</pre>`;
  if(d.source)h+=`<p>Source: <a href="${esc(d.source.source_url)}">${esc(d.source.headline)}</a></p>`;
  const el=document.getElementById("detail");el.innerHTML=h;el.hidden=false;el.scrollIntoView()}
async function act(action,id){
  const body={action,video_id:id};
  if(action==="reject")body.reason=prompt("Reason?")||"";
  await api("/api/action",{method:"POST",body:JSON.stringify(body)});
  loadTab(tab);loadSummary()}
async function regenHook(id,hookId){
  await api("/api/action",{method:"POST",body:JSON.stringify({action:"regenerate",video_id:id,hook_id:hookId,change:"hook"})});
  loadTab(tab)}
const nav=document.getElementById("tabs");
for(const t of TABS){const b=document.createElement("button");b.textContent=t.replace(/_/g," ");
  b.dataset.t=t;b.onclick=()=>loadTab(t);nav.appendChild(b)}
loadSummary();loadTab("needs_review");
</script>"""


class Handler(BaseHTTPRequestHandler):
    def _auth_ok(self) -> bool:
        if not settings.DASHBOARD_TOKEN:
            return True
        return self.headers.get("Authorization", "") == f"Bearer {settings.DASHBOARD_TOKEN}"

    def _send(self, code: int, body: bytes, ctype: str = "application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, data):
        self._send(code, json.dumps(data, default=str).encode())

    def log_message(self, fmt, *args):
        settings.log("dashboard", fmt % args)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            return self._send(200, _HTML.encode(), "text/html; charset=utf-8")
        if not self._auth_ok():
            return self._json(401, {"error": "unauthorized"})
        try:
            if url.path == "/api/summary":
                return self._json(200, summary())
            if url.path == "/api/queue":
                tab = parse_qs(url.query).get("tab", ["needs_review"])[0]
                if tab not in TABS:
                    return self._json(400, {"error": f"unknown tab {tab}"})
                table, where = TABS[tab]
                rows = store().find(table, where)
                rows.sort(key=lambda r: r.get("score", 0) or 0, reverse=True)
                return self._json(200, rows[:200])
            if url.path.startswith("/api/video/"):
                detail = video_detail(url.path.rsplit("/", 1)[1])
                return self._json(200 if detail else 404, detail or {"error": "not found"})
            return self._json(404, {"error": "not found"})
        except Exception as exc:
            return self._json(500, {"error": str(exc)})

    def do_POST(self):
        if not self._auth_ok():
            return self._json(401, {"error": "unauthorized"})
        if urlparse(self.path).path != "/api/action":
            return self._json(404, {"error": "not found"})
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
            return self._json(200, handle_action(body))
        except (KeyError,) as exc:
            return self._json(404, {"error": f"not found: {exc}"})
        except (ValueError, PermissionError) as exc:
            return self._json(400, {"error": str(exc)})
        except Exception as exc:
            return self._json(500, {"error": str(exc)})


def main():
    port = int(os.environ.get("DVE_DASHBOARD_PORT", "8321"))
    if not settings.DASHBOARD_TOKEN:
        settings.log("dashboard", "WARNING: DVE_DASHBOARD_TOKEN unset — "
                                  "only run on localhost like this")
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    settings.log("dashboard", f"listening on :{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
