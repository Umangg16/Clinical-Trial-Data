# ctgov_loader_final.py
# Full JSON → PostgreSQL loader for ClinicalTrials.gov (v2 API)
# - Targets schema: ctg (as in your DB)
# - Idempotent per-NCT load (deletes old rows, reloads fresh inside one transaction)
# - Parallel bulk loading with progress bar (tqdm)
# - Auto-fixes missing category titles with "~" to satisfy NOT NULL
# - No commits inside stage loaders; a single transaction per NCT
# - Works for ALL studies (no results filter), or a single NCT, or limited pages for testing

import argparse
import json
import time
from typing import Optional, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg
from psycopg.rows import dict_row
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

PG_DSN = "dbname=ctgov user=postgres password=Snigdha_3009 host=localhost port=5432"
BASE = "https://clinicaltrials.gov/api/v2"


# -------------------- HTTP session --------------------
def make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=6, read=6, connect=6, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=64, pool_maxsize=64))
    sess.headers.update({"Accept": "application/json"})
    return sess


# -------------------- Helpers --------------------
def get(d: dict, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p, default)
        if cur is default:
            break
    return cur

def parse_numeric_or_text(val) -> Tuple[Optional[float], Optional[str]]:
    if val is None:
        return None, None
    try:
        return float(val), None
    except Exception:
        return None, str(val)

def parse_ci_limit(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None

def resolve_group(title: Optional[str], arm_labels: List[str]) -> Optional[str]:
    if not title:
        return title
    t = title.lower().strip()
    for a in arm_labels:
        aa = a.lower().strip()
        if t == aa or t.replace(" ", "") == aa.replace(" ", ""):
            return a
    return title


# -------------------- DB utils --------------------
def connect():
    return psycopg.connect(PG_DSN, row_factory=dict_row)

def ensure_loader_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ctg.load_log (
              nct_id TEXT PRIMARY KEY,
              loaded_at TIMESTAMPTZ DEFAULT now()
            );
        """)
    conn.commit()

def clear_existing(cur, nct_id: str):
    """
    Delete any existing rows for this NCT across all dependent tables
    (order chosen to avoid FK issues even if you add them later).
    """
    # Child-first:
    cur.execute("DELETE FROM ctg.outcome_measurements WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.outcome_denoms       WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.outcome_measures     WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.baseline_measure_values WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.baseline_measures       WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.baseline_denoms         WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.adverse_events     WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.ae_event_groups    WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.participant_flow_milestones   WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.participant_flow_withdrawals  WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.participant_flow_periods      WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.arm_interventions WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.interventions     WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.arms              WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.groups            WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.ipd_sharing       WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.study_status      WHERE nct_id=%s;", (nct_id,))
    cur.execute("DELETE FROM ctg.studies           WHERE nct_id=%s;", (nct_id,))

    cur.execute("DELETE FROM ctg.trials_raw        WHERE nct_id=%s;", (nct_id,))

def mark_loaded(cur, nct_id: str):
    cur.execute("INSERT INTO ctg.load_log (nct_id) VALUES (%s) ON CONFLICT DO NOTHING;", (nct_id,))


# -------------------- Fetchers --------------------
def fetch_study(session: requests.Session, nct_id: str) -> dict:
    r = session.get(f"{BASE}/studies/{nct_id}", timeout=60)
    r.raise_for_status()
    return r.json()

def list_all_nct_ids(session: requests.Session, page_size=100, max_pages: Optional[int]=None) -> List[str]:
    ids: List[str] = []
    next_token = None
    page = 0
    print("📡 Fetching NCT ID pages …")
    while True:
        params = {"pageSize": page_size}
        if next_token:
            params["pageToken"] = next_token
        r = session.get(f"{BASE}/studies", params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        studies = data.get("studies", []) or []
        for st in studies:
            ids.append(st["protocolSection"]["identificationModule"]["nctId"])
        page += 1
        print(f"  ✅ Page {page} | +{len(studies)} | Total IDs: {len(ids)}")
        next_token = data.get("nextPageToken")
        if not next_token:
            break
        if max_pages and page >= max_pages:
            break
        time.sleep(0.2)
    return ids


# -------------------- Stage loaders (no commits here) --------------------
def load_core(cur, study: dict):
    p = study.get("protocolSection", {}) or {}
    ident = p.get("identificationModule", {}) or {}
    status = p.get("statusModule", {}) or {}
    design = p.get("designModule", {}) or {}
    phases = p.get("phasesModule", {}) or {}
    desc = p.get("descriptionModule", {}) or {}

    def d(x):
        return x.get("date") if isinstance(x, dict) else None

    nct = ident["nctId"]

    # raw
    cur.execute("""
        INSERT INTO ctg.trials_raw (nct_id, source_json)
        VALUES (%s,%s::jsonb)
        ON CONFLICT (nct_id) DO UPDATE SET source_json = EXCLUDED.source_json, ingested_at = now();
    """, (nct, json.dumps(study)))

    # studies (rich core; only filling the columns you created)
    cur.execute("""
        INSERT INTO ctg.studies
        (nct_id, org_study_id, org_full_name, org_class, brief_title, official_title,
         study_type, phase, enrollment_count, enrollment_type, start_date,
         primary_completion_date, completion_date, brief_summary, detailed_description)
        VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (nct_id) DO UPDATE SET
          org_study_id=EXCLUDED.org_study_id,
          org_full_name=EXCLUDED.org_full_name,
          org_class=EXCLUDED.org_class,
          brief_title=EXCLUDED.brief_title,
          official_title=EXCLUDED.official_title,
          study_type=EXCLUDED.study_type,
          phase=EXCLUDED.phase,
          enrollment_count=EXCLUDED.enrollment_count,
          enrollment_type=EXCLUDED.enrollment_type,
          start_date=EXCLUDED.start_date,
          primary_completion_date=EXCLUDED.primary_completion_date,
          completion_date=EXCLUDED.completion_date,
          brief_summary=EXCLUDED.brief_summary,
          detailed_description=EXCLUDED.detailed_description;
    """, (
        nct,
        get(ident, "orgStudyIdInfo", "id"),
        get(ident, "organization", "fullName"),
        get(ident, "organization", "class"),
        ident.get("briefTitle"),
        ident.get("officialTitle"),
        design.get("studyType"),
        phases.get("phases"),
        get(design, "enrollmentInfo", "count"),
        get(design, "enrollmentInfo", "type"),
        d(status.get("startDateStruct")),
        d(status.get("primaryCompletionDateStruct")),
        d(status.get("completionDateStruct")),
        get(p, "descriptionModule", "briefSummary"),
        get(p, "descriptionModule", "detailedDescription"),
    ))

    # study_status
    cur.execute("""
        INSERT INTO ctg.study_status
        (nct_id, overall_status, status_verified_month, results_first_posted, last_update_posted)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (nct_id) DO UPDATE SET
          overall_status=EXCLUDED.overall_status,
          status_verified_month=EXCLUDED.status_verified_month,
          results_first_posted=EXCLUDED.results_first_posted,
          last_update_posted=EXCLUDED.last_update_posted;
    """, (
        nct,
        status.get("overallStatus"),
        status.get("statusVerifiedDate"),
        get(status, "resultsFirstPostDateStruct", "date"),
        get(status, "lastUpdatePostDateStruct", "date"),
    ))

def load_arms_and_interventions(cur, study: dict):
    p = study.get("protocolSection", {}) or {}
    aim = p.get("armsInterventionsModule", {}) or {}
    nct = get(p, "identificationModule", "nctId")

    arms = aim.get("arms", []) or []
    interventions = aim.get("interventions", []) or []
    pairs = aim.get("armInterventionPairs", []) or []

    for arm in arms:
        cur.execute("""
            INSERT INTO ctg.arms (nct_id, arm_label, arm_type, arm_description)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (nct_id, arm_label) DO UPDATE SET
              arm_type=EXCLUDED.arm_type,
              arm_description=EXCLUDED.arm_description;
        """, (nct, arm.get("label"), arm.get("type"), arm.get("description")))

    for iv in interventions:
        cur.execute("""
            INSERT INTO ctg.interventions
            (nct_id, intervention_name, intervention_type, description, other_names)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (nct_id, intervention_name) DO UPDATE SET
              intervention_type=EXCLUDED.intervention_type,
              description=EXCLUDED.description,
              other_names=EXCLUDED.other_names;
        """, (nct, iv.get("name"), iv.get("type"), iv.get("description"), iv.get("otherNames")))

    for link in pairs:
        cur.execute("""
            INSERT INTO ctg.arm_interventions (nct_id, arm_label, intervention_name)
            VALUES (%s,%s,%s)
            ON CONFLICT DO NOTHING;
        """, (nct, link.get("armLabel"), link.get("interventionName")))

def load_groups_for_module(cur, nct: str, module_name: str, groups: List[dict], arm_labels: List[str]):
    for g in groups or []:
        mapped = resolve_group(g.get("title"), arm_labels)
        cur.execute("""
            INSERT INTO ctg.groups (nct_id, module, group_id, title, description)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING;
        """, (nct, module_name, g.get("id"), mapped, g.get("description")))

def load_outcomes(cur, study: dict):
    nct = get(study, "protocolSection", "identificationModule", "nctId")
    omm = get(study, "resultsSection", "outcomeMeasuresModule") or {}

    # arm labels for mapping groups
    cur.execute("SELECT arm_label FROM ctg.arms WHERE nct_id=%s;", (nct,))
    arm_labels = [r["arm_label"] for r in cur.fetchall()]

    for o in omm.get("outcomeMeasures", []) or []:
        cur.execute("""
            INSERT INTO ctg.outcome_measures
            (nct_id, outcome_type, title, description, population_desc, reporting_status,
             param_type, dispersion_type, unit_of_measure, timeframe, raw_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING outcome_id;
        """, (
            nct, o.get("type"), o.get("title"), o.get("description"),
            o.get("populationDescription"), o.get("reportingStatus"),
            o.get("paramType"), o.get("dispersionType"),
            o.get("unitOfMeasure"), o.get("timeFrame"),
            json.dumps(o)
        ))
        outcome_id = cur.fetchone()["outcome_id"]

        # groups for this outcome
        load_groups_for_module(cur, nct, 'outcome', o.get("groups", []), arm_labels)

        # denoms
        for d in o.get("denoms", []) or []:
            units = d.get("units")
            for c in d.get("counts", []) or []:
                cur.execute("""
                    INSERT INTO ctg.outcome_denoms (nct_id, outcome_id, group_id, units, count_value)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (nct, outcome_id, c.get("groupId"), units, c.get("value")))

        # measurements
        for cls in o.get("classes", []) or []:
            for cat in cls.get("categories", []) or []:
                cat_title = (cat.get("title") or "").strip() or "~"  # A: placeholder
                for meas in cat.get("measurements", []) or []:
                    vnum, vtext = parse_numeric_or_text(meas.get("value"))
                    lower = parse_ci_limit(meas.get("lowerLimit"))
                    upper = parse_ci_limit(meas.get("upperLimit"))
                    cur.execute("""
                        INSERT INTO ctg.outcome_measurements
                        (nct_id, outcome_id, group_id, category_title, value_numeric, value_text, lower_limit, upper_limit, comment)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        nct, outcome_id, meas.get("groupId"), cat_title,
                        vnum, vtext, lower, upper, meas.get("comment")
                    ))

def load_baseline(cur, study: dict):
    nct = get(study, "protocolSection", "identificationModule", "nctId")
    base = get(study, "resultsSection", "baselineCharacteristicsModule") or {}

    # arm labels
    cur.execute("SELECT arm_label FROM ctg.arms WHERE nct_id=%s;", (nct,))
    arm_labels = [r["arm_label"] for r in cur.fetchall()]

    # groups
    load_groups_for_module(cur, nct, 'baseline', base.get("groups", []), arm_labels)

    # denoms
    for d in base.get("denoms", []) or []:
        units = d.get("units")
        for c in d.get("counts", []) or []:
            cur.execute("""
                INSERT INTO ctg.baseline_denoms (nct_id, units, group_id, value_count)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
            """, (nct, units, c.get("groupId"), c.get("value")))

    # measures
    for m in base.get("measures", []) or []:
        cur.execute("""
            INSERT INTO ctg.baseline_measures
            (nct_id, title, param_type, dispersion_type, unit_of_measure, raw_json)
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING baseline_id;
        """, (
            nct, m.get("title"), m.get("paramType"), m.get("dispersionType"),
            m.get("unitOfMeasure"), json.dumps(m)
        ))
        bid = cur.fetchone()["baseline_id"]

        for cls in m.get("classes", []) or []:
            for cat in cls.get("categories", []) or []:
                cat_title = (cat.get("title") or "").strip() or "~"
                for meas in cat.get("measurements", []) or []:
                    vnum, _ = parse_numeric_or_text(meas.get("value"))
                    lower = parse_ci_limit(meas.get("lowerLimit"))
                    upper = parse_ci_limit(meas.get("upperLimit"))
                    cur.execute("""
                        INSERT INTO ctg.baseline_measure_values
                        (nct_id, baseline_id, group_id, category_title, value_numeric, lower_limit, upper_limit)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING;
                    """, (nct, bid, meas.get("groupId"), cat_title, vnum, lower, upper))

def load_adverse_events(cur, study: dict):
    nct = get(study, "protocolSection", "identificationModule", "nctId")
    ae = get(study, "resultsSection", "adverseEventsModule") or {}

    # arm labels
    cur.execute("SELECT arm_label FROM ctg.arms WHERE nct_id=%s;", (nct,))
    arm_labels = [r["arm_label"] for r in cur.fetchall()]

    # event groups
    load_groups_for_module(cur, nct, 'adverse_event', ae.get("eventGroups", []), arm_labels)
    for g in ae.get("eventGroups", []) or []:
        mapped = resolve_group(g.get("title"), arm_labels)
        cur.execute("""
            INSERT INTO ctg.ae_event_groups
            (nct_id, group_id, title, description,
             deaths_num_affected, deaths_num_at_risk,
             serious_num_affected, serious_num_at_risk,
             other_num_affected, other_num_at_risk)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (nct_id, group_id) DO NOTHING;
        """, (
            nct, g.get("id"), mapped, g.get("description"),
            g.get("deathsNumAffected"), g.get("deathsNumAtRisk"),
            g.get("seriousNumAffected"), g.get("seriousNumAtRisk"),
            g.get("otherNumAffected"), g.get("otherNumAtRisk")
        ))

    def add_events(events, is_serious: bool):
        for t in events or []:
            for s in t.get("stats", []) or []:
                cur.execute("""
                    INSERT INTO ctg.adverse_events
                    (nct_id, term, organ_system, source_vocabulary, assessment_type, is_serious,
                     group_id, num_affected, num_at_risk)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (
                    nct, t.get("term"), t.get("organSystem"),
                    ae.get("sourceVocabulary"), ae.get("assessmentType"),
                    is_serious, s.get("groupId"), s.get("numAffected"), s.get("numAtRisk")
                ))

    add_events(ae.get("seriousEvents"), True)
    add_events(ae.get("otherEvents"), False)

def load_participant_flow(cur, study: dict):
    nct = get(study, "protocolSection", "identificationModule", "nctId")
    pf = get(study, "resultsSection", "participantFlowModule") or {}

    # arm labels
    cur.execute("SELECT arm_label FROM ctg.arms WHERE nct_id=%s;", (nct,))
    arm_labels = [r["arm_label"] for r in cur.fetchall()]

    load_groups_for_module(cur, nct, 'participant_flow', pf.get("groups", []), arm_labels)

    for period in pf.get("periods", []) or []:
        cur.execute("""
            INSERT INTO ctg.participant_flow_periods (nct_id, period_title)
            VALUES (%s,%s)
            ON CONFLICT DO NOTHING;
        """, (nct, period.get("title")))

        for m in period.get("milestones", []) or []:
            for ach in m.get("achievements", []) or []:
                cur.execute("""
                    INSERT INTO ctg.participant_flow_milestones
                    (nct_id, period_title, milestone_type, group_id, num_subjects)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (nct, period.get("title"), m.get("type"), ach.get("groupId"), ach.get("numSubjects")))

        for dw in period.get("dropWithdraws", []) or []:
            for r in dw.get("reasons", []) or []:
                cur.execute("""
                    INSERT INTO ctg.participant_flow_withdrawals
                    (nct_id, period_title, reason_type, group_id, num_subjects)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                """, (nct, period.get("title"), dw.get("type"), r.get("groupId"), r.get("numSubjects")))

def load_ipd_sharing(cur, study: dict):
    nct = get(study, "protocolSection", "identificationModule", "nctId")
    ipd = get(study, "protocolSection", "ipdSharingStatementModule") or {}
    if not ipd:
        return
    cur.execute("""
        INSERT INTO ctg.ipd_sharing
        (nct_id, ipd_sharing, description, info_types, timeframe, access_criteria)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (nct_id) DO UPDATE SET
          ipd_sharing=EXCLUDED.ipd_sharing,
          description=EXCLUDED.description,
          info_types=EXCLUDED.info_types,
          timeframe=EXCLUDED.timeframe,
          access_criteria=EXCLUDED.access_criteria;
    """, (
        nct, ipd.get("ipdSharing"), ipd.get("description"),
        ipd.get("infoTypes"), ipd.get("timeFrame"), ipd.get("accessCriteria")
    ))


# -------------------- Per-NCT transactional loader --------------------
def load_one_nct(nct_id: str) -> bool:
    """
    Opens its own DB connection (thread-safe), wraps the whole load in a transaction,
    deletes old rows for the NCT, inserts fresh rows, and marks load_log in same txn.
    """
    session = make_session()
    try:
        study = fetch_study(session, nct_id)
    except Exception as e:
        print(f"⚠️  {nct_id} fetch failed: {e}")
        return False

    try:
        with connect() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    # Idempotent: clear previous
                    clear_existing(cur, nct_id)

                    # Stages
                    load_core(cur, study)
                    load_arms_and_interventions(cur, study)
                    load_outcomes(cur, study)
                    load_baseline(cur, study)
                    load_adverse_events(cur, study)
                    load_participant_flow(cur, study)
                    load_ipd_sharing(cur, study)

                    # Mark done in same transaction
                    mark_loaded(cur, nct_id)
        return True
    except Exception as e:
        print(f"❌  {nct_id} load failed: {e}")
        return False


# -------------------- Bulk orchestrator --------------------
def bulk_load(nct_ids: List[str], workers: int = 8):
    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(load_one_nct, n): n for n in nct_ids}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Loading trials", unit="trial"):
            try:
                if fut.result():
                    ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
    print(f"\n✅ Loaded: {ok} | ❌ Failed: {fail}")


# -------------------- CLI --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ClinicalTrials.gov → PostgreSQL (ctg schema) full loader")
    parser.add_argument("--nct", help="Load a single NCT ID")
    parser.add_argument("--all", action="store_true", help="Load all studies (no filter)")
    parser.add_argument("--page-size", type=int, default=100, help="Listing page size for --all")
    parser.add_argument("--max-pages", type=int, default=None, help="Limit pages for --all (testing)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers for bulk mode")
    args = parser.parse_args()

    # Ensure load_log exists
    with connect() as c:
        ensure_loader_tables(c)

    if args.nct:
        print(f"→ Loading single trial: {args.nct}")
        ok = load_one_nct(args.nct)
        print("✅ Done." if ok else "❌ Failed.")

    elif args.all:
        print("\n🔍 Fetching ALL NCT IDs from v2 …")
        session = make_session()
        ids = list_all_nct_ids(session, page_size=args.page_size, max_pages=args.max_pages)
        print(f"📦 Found {len(ids)} trials. Starting parallel load with {args.workers} workers …\n")
        bulk_load(ids, workers=args.workers)

    else:
        print("Nothing to do. Use --nct NCTxxxx or --all (optionally --max-pages).")
