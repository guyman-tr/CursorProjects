import json

import pandas as pd

from _common import jaccard_tokens, load_settings, normalize_name, out_dir


def main() -> int:
    settings = load_settings()
    strip_prefixes = settings["mapping"].get("stripPrefixes", ["vw_", "v_"])
    threshold = float(settings["mapping"].get("threshold", 0.65))

    out = out_dir()
    syn_path = out / "synapse_objects.csv"
    dbx_path = out / "databricks_objects.csv"
    if not syn_path.exists() or not dbx_path.exists():
        raise SystemExit("Run inventory scripts first")

    syn = pd.read_csv(syn_path)
    dbx = pd.read_csv(dbx_path)

    syn["norm"] = syn["name"].astype(str).map(lambda s: normalize_name(s, strip_prefixes))
    dbx["norm"] = dbx["name"].astype(str).map(lambda s: normalize_name(s, strip_prefixes))

    dbx_by_norm = {}
    for i, r in dbx.iterrows():
        n = str(r.get("norm") or "")
        if n and n not in dbx_by_norm:
            dbx_by_norm[n] = i

    mappings = []
    review_rows = []

    for _, s in syn.iterrows():
        s_norm = str(s["norm"])

        best_idx = None
        best_score = -1.0
        reason = "fuzzy_name"

        exact = dbx_by_norm.get(s_norm)
        if exact is not None:
            best_idx = exact
            best_score = 1.0
            reason = "exact_name"
        else:
            for i, d in dbx.iterrows():
                score = jaccard_tokens(s_norm, str(d["norm"]))
                if score > best_score:
                    best_score = score
                    best_idx = i

        if best_idx is None:
            continue
        best = dbx.loc[best_idx]

        review_rows.append({
            "syn_catalog": s.get("catalog"),
            "syn_schema": s.get("schema"),
            "syn_name": s.get("name"),
            "syn_type": s.get("type"),
            "dbx_catalog": best.get("catalog"),
            "dbx_schema": best.get("schema"),
            "dbx_name": best.get("name"),
            "dbx_type": best.get("type"),
            "confidence": float(min(0.99, max(0.0, best_score))),
            "matchReason": reason,
        })

        if best_score >= threshold:
            mappings.append({
                "synapse": {
                    "catalog": s.get("catalog"),
                    "schema": s.get("schema"),
                    "name": s.get("name"),
                    "type": str(s.get("type", "TABLE")).lower(),
                },
                "databricks": {
                    "catalog": best.get("catalog"),
                    "schema": best.get("schema"),
                    "name": best.get("name"),
                    "type": str(best.get("type", "table")).lower(),
                },
                "confidence": float(min(0.99, max(0.0, best_score))),
                "matchReason": reason,
            })

    review = pd.DataFrame.from_records(review_rows).sort_values("confidence", ascending=False)
    review_path = out / "mapping_review.csv"
    review.to_csv(review_path, index=False)

    out_json = {
        "version": 1,
        "updatedAt": pd.Timestamp.utcnow().isoformat() + "Z",
        "threshold": threshold,
        "mappings": sorted(mappings, key=lambda m: m["confidence"], reverse=True),
    }
    mapping_path = out / "mapping.json"
    mapping_path.write_text(json.dumps(out_json, indent=2), encoding="utf-8")

    print(f"Wrote mapping: {mapping_path} (mappings={len(mappings)}, threshold={threshold})")
    print(f"Wrote review:  {review_path} (rows={len(review)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
