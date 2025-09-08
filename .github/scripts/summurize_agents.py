# FILE: scripts/summarize_agents.py
# Reads logs/YYYY-MM-DD_{v2|v3}.json and writes logs/summary-YYYY-MM-DD.md
import json, os, datetime


d = datetime.date.today().isoformat()
base = "logs"


v2 = None
v3 = None
try:
v2 = json.load(open(os.path.join(base, f"{d}_v2.json"), encoding="utf-8"))
except Exception:
pass
try:
v3 = json.load(open(os.path.join(base, f"{d}_v3.json"), encoding="utf-8"))
except Exception:
pass


def stats(payload):
if not payload: return {"final":0, "new":0, "avg":0.0}
jobs = payload.get("jobs", [])
avg = round(sum(j.get("score",0) for j in jobs)/max(1,len(jobs)), 2)
new = sum(1 for j in jobs if j.get("is_new"))
return {"final":len(jobs), "new":new, "avg":avg}


s2 = stats(v2)
s3 = stats(v3)


winner = "tie"
if s3["final"] > s2["final"]:
winner = "v3"
elif s2["final"] > s3["final"]:
winner = "v2"


out = [
f"# {d} — job_search_agent summary\n",
f"- v2 → total: {s2['final']}, new: {s2['new']}, avg score: {s2['avg']}\n",
f"- v3 → total: {s3['final']}, new: {s3['new']}, avg score: {s3['avg']}\n",
f"**Winner:** {winner}\n",
]


# Append top 3 per agent if available
for tag, payload in (("v2", v2), ("v3", v3)):
if not payload: continue
jobs = payload.get("jobs", [])[:3]
out.append(f"\n## Top 3 — {tag}\n")
for i, j in enumerate(jobs, 1):
out.append(f"{i}. {j.get('company','?')} — {j.get('title','?')} (score={j.get('score',0)}, new={'yes' if j.get('is_new') else 'no'})\n ↳ {j.get('url','')}\n")


os.makedirs(base, exist_ok=True)
open(os.path.join(base, f"summary-{d}.md"), "w", encoding="utf-8").write("".join(out))
print("".join(out))
