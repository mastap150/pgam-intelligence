import json, os
from PIL import Image

SRC = "./png"
WEB = "./web"; os.makedirs(WEB, exist_ok=True)
THUMB = "./thumb"; os.makedirs(THUMB, exist_ok=True)

results = json.load(open("./results.json"))
MAIN_W, THUMB_W = 1400, 420
total = 0
for r in results:
    if not r.get("file") or not os.path.exists(r["file"]):
        r["web"] = r["thumb"] = None
        continue
    im = Image.open(r["file"]).convert("RGB")
    r["px"] = list(im.size)
    base = os.path.basename(r["file"]).replace(".png", ".jpg")
    # main
    w = min(MAIN_W, im.width)
    m = im.resize((w, max(1, round(im.height * w / im.width))), Image.LANCZOS)
    mp = os.path.join(WEB, base); m.save(mp, "JPEG", quality=82, optimize=True, progressive=True)
    # thumb: crop to first 1.4 screens so the grid stays scannable
    tw = THUMB_W
    t = im.resize((tw, max(1, round(im.height * tw / im.width))), Image.LANCZOS)
    t = t.crop((0, 0, tw, min(t.height, round(tw * 0.78))))
    tp = os.path.join(THUMB, base); t.save(tp, "JPEG", quality=70, optimize=True)
    r["web"], r["thumb"] = mp, tp
    total += os.path.getsize(mp) + os.path.getsize(tp)
    print(f'{base:44} {im.width}x{im.height} -> {os.path.getsize(mp)//1024}KB + {os.path.getsize(tp)//1024}KB')

json.dump(results, open("./results.json", "w"), indent=2)
print(f"\nTOTAL EMBED BUDGET: {total/1024/1024:.2f} MB")
