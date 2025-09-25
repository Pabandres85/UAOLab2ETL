from __future__ import annotations
from pathlib import Path
import json, re, sqlite3
from datetime import datetime

import pandas as pd
from lxml import etree


# ──────────────────────────── helpers ────────────────────────────
def structured_log(entry: dict, log_path: str | Path):
    """Append de línea JSON a un archivo .log (JSONL)."""
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    record = dict(entry)
    record.setdefault("ts", datetime.utcnow().isoformat())
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _std_id(val):
    if pd.isna(val):
        return None
    return str(val).strip()


# ──────────────────────────── extract ────────────────────────────
def load_alumnos(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    if "id_alumno" not in df.columns:
        raise ValueError("alumnos.csv debe incluir 'id_alumno'")
    df["id_alumno"] = df["id_alumno"].map(_std_id)
    df = df.drop_duplicates(subset=["id_alumno"], keep="first")

    if "fecha_nacimiento" in df.columns:
        df["fecha_nacimiento"] = pd.to_datetime(
            df["fecha_nacimiento"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    for c in ["nombre", "apellido", "correo"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def load_calificaciones(path: str | Path, escala_max=5.0) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "calificaciones" in data:
        data = data["calificaciones"]

    df = pd.DataFrame(data).fillna("")
    df = df.rename(columns={"materia": "asignatura"})

    for c in ["id_alumno", "asignatura", "periodo"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df["id_alumno"] = df["id_alumno"].map(_std_id)

    if "nota" not in df.columns:
        raise ValueError("calificaciones.json debe incluir 'nota'")
    df["nota"] = pd.to_numeric(df["nota"], errors="coerce")

    # Si parecen estar en escala 0-100, normaliza a 0-5
    if (df["nota"] > escala_max).any() and (df["nota"] <= 100).all():
        df["nota"] = (df["nota"] / 20.0).round(2)

    df["nota"] = df["nota"].clip(0, escala_max).round(1)
    return df


def load_matriculas(path: str | Path) -> pd.DataFrame:
    """Lee XML estándar o 'pseudo-XML' línea por línea."""
    p = Path(path)
    raw = p.read_text(encoding="utf-8", errors="ignore")

    # 1) Intento XML real
    try:
        root = etree.fromstring(raw.encode("utf-8"))
        rows = []
        for m in root.findall(".//matricula"):
            rid = m.findtext("id_alumno", "").strip()
            anio = m.findtext("anio", "").strip()
            estado = m.findtext("estado", "").strip()
            jornada = m.findtext("jornada", "").strip()
            rows.append(
                {"id_alumno": _std_id(rid), "anio": anio, "estado": estado, "jornada": jornada}
            )
        df = pd.DataFrame(rows)
        if len(df):
            return df
    except Exception:
        pass

    # 2) Fallback: bloques por líneas (id / anio / estado / jornada)
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    rows = []
    i = 0
    while i + 3 < len(lines):
        rid, anio, estado, jornada = lines[i : i + 4]
        if re.fullmatch(r"[A-Za-z0-9]+", rid) and re.fullmatch(r"\d{4}", anio):
            rows.append(
                {"id_alumno": _std_id(rid), "anio": anio, "estado": estado, "jornada": jornada}
            )
            i += 4
        else:
            i += 1
    return pd.DataFrame(rows)


# ─────────────────────────── transform ───────────────────────────
def build_final(df_a: pd.DataFrame, df_c: pd.DataFrame, df_m: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df_c.groupby("id_alumno")
        .agg(
            promedio_general=("nota", "mean"),
            nota_minima=("nota", "min"),
            nota_maxima=("nota", "max"),
            asignaturas_cursadas=("asignatura", "count"),
            aprobadas=("nota", lambda s: (s >= 3.0).sum()),
            reprobadas=("nota", lambda s: (s < 3.0).sum()),
        )
        .reset_index()
    )
    agg["promedio_general"] = agg["promedio_general"].round(2)

    out = df_a.merge(df_m, on="id_alumno", how="left").merge(agg, on="id_alumno", how="left")

    def clasif(x):
        if pd.isna(x):
            return "Sin Calificaciones"
        if x >= 4.5:
            return "Destacado"
        if x >= 4.0:
            return "Muy Bueno"
        if x >= 3.5:
            return "Bueno"
        if x >= 3.0:
            return "Regular"
        return "En Riesgo"

    out["clasificacion_rendimiento"] = out["promedio_general"].apply(clasif)
    return out


# ─────────────────────────── load / SQLite ──────────────────────
DDL_VISTAS = [
    """
    CREATE VIEW IF NOT EXISTS vista_rendimiento_alumno AS
    SELECT a.id_alumno,
           a.nombre,
           a.apellido,
           a.correo,
           m.estado AS estado_matricula,
           m.jornada,
           ROUND(AVG(c.nota), 2) AS promedio_general,
           MIN(c.nota)           AS nota_minima,
           MAX(c.nota)           AS nota_maxima,
           SUM(CASE WHEN c.nota >= 3.0 THEN 1 ELSE 0 END) AS asignaturas_aprobadas,
           SUM(CASE WHEN c.nota  < 3.0 THEN 1 ELSE 0 END) AS asignaturas_reprobadas
    FROM alumnos a
    LEFT JOIN matriculas m ON a.id_alumno = m.id_alumno
    LEFT JOIN calificaciones c ON a.id_alumno = c.id_alumno
    GROUP BY a.id_alumno;
    """,
    """
    CREATE VIEW IF NOT EXISTS vista_rendimiento_asignatura AS
    SELECT asignatura,
           COUNT(DISTINCT id_alumno) AS total_estudiantes,
           ROUND(AVG(nota), 2)       AS promedio_asignatura,
           MIN(nota)                 AS nota_minima,
           MAX(nota)                 AS nota_maxima,
           SUM(CASE WHEN nota >= 3.0 THEN 1 ELSE 0 END) AS aprobados,
           SUM(CASE WHEN nota  < 3.0 THEN 1 ELSE 0 END) AS reprobados,
           ROUND(100.0 * SUM(CASE WHEN nota >= 3.0 THEN 1 ELSE 0 END) / COUNT(*), 1)
             AS porcentaje_aprobacion
    FROM calificaciones
    GROUP BY asignatura;
    """,
]


def save_sqlite(
    df_a: pd.DataFrame,
    df_c: pd.DataFrame,
    df_m: pd.DataFrame,
    df_final: pd.DataFrame,
    db_path: str,
):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        df_a.to_sql("alumnos", con, if_exists="replace", index=False)
        df_c.to_sql("calificaciones", con, if_exists="replace", index=False)
        df_m.to_sql("matriculas", con, if_exists="replace", index=False)
        df_final.to_sql("estudiantes_final", con, if_exists="replace", index=False)

        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_monitor (
              run_id TEXT PRIMARY KEY,
              start_ts TEXT,
              end_ts TEXT,
              duration_s REAL,
              status TEXT,
              source_csv_rows INTEGER,
              source_json_rows INTEGER,
              source_xml_rows INTEGER,
              clean_rows_alumnos INTEGER,
              clean_rows_califs INTEGER,
              clean_rows_matriculas INTEGER,
              final_rows INTEGER,
              errors_count INTEGER,
              warns_count INTEGER,
              notes_out_of_range INTEGER,
              mongo_docs_prepared INTEGER,
              log_path TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_alumnos_id ON alumnos(id_alumno);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_calif_id ON calificaciones(id_alumno);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_matr_id ON matriculas(id_alumno);")
        for ddl in DDL_VISTAS:
            cur.execute(ddl)
        con.commit()
    finally:
        con.close()


def insert_monitor(metrics: dict, db_path: str):
    """Inserta (o reemplaza) una fila en etl_monitor."""
    start_ts = metrics.get("start_ts")
    end_ts = metrics.get("end_ts", datetime.utcnow().isoformat())
    if start_ts and not metrics.get("duration_s"):
        try:
            t0 = datetime.fromisoformat(start_ts)
            t1 = datetime.fromisoformat(end_ts)
            metrics["duration_s"] = (t1 - t0).total_seconds()
        except Exception:
            metrics["duration_s"] = None

    cols = [
        "run_id",
        "start_ts",
        "end_ts",
        "duration_s",
        "status",
        "source_csv_rows",
        "source_json_rows",
        "source_xml_rows",
        "clean_rows_alumnos",
        "clean_rows_califs",
        "clean_rows_matriculas",
        "final_rows",
        "errors_count",
        "warns_count",
        "notes_out_of_range",
        "mongo_docs_prepared",
        "log_path",
    ]
    vals = [metrics.get(k) for k in cols]

    con = sqlite3.connect(db_path)
    try:
        con.execute(
            "INSERT OR REPLACE INTO etl_monitor ("
            + ",".join(cols)
            + ") VALUES ("
            + ",".join(["?"] * len(cols))
            + ")",
            vals,
        )
        con.commit()
    finally:
        con.close()


# ─────────────────────────── exports ─────────────────────────────
def export_csv(df: pd.DataFrame, path: str | Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")


def make_mongo_json(
    df_final: pd.DataFrame, df_calif: pd.DataFrame, df_matr: pd.DataFrame, path: str | Path
):
    docs = []
    for _, a in df_final.iterrows():
        rid = a.get("id_alumno")
        califs = df_calif[df_calif["id_alumno"] == rid]
        m = df_matr[df_matr["id_alumno"] == rid].head(1)

        doc = {
            "_id": str(rid),
            "perfil": {
                "id_alumno": str(rid),
                "nombre": a.get("nombre"),
                "apellido": a.get("apellido"),
                "correo": a.get("correo"),
                "fecha_nacimiento": a.get("fecha_nacimiento"),
            },
            "matriculas": []
            if m.empty
            else [
                {
                    "anio": m.iloc[0].get("anio"),
                    "estado": m.iloc[0].get("estado"),
                    "jornada": m.iloc[0].get("jornada"),
                }
            ],
            "calificaciones": [
                {"asignatura": r["asignatura"], "periodo": r.get("periodo"), "nota": float(r["nota"])}
                for _, r in califs.iterrows()
            ],
            "resumen_academico": {
                "promedio_general": None
                if pd.isna(a.get("promedio_general"))
                else float(a.get("promedio_general")),
                "nota_minima": None if pd.isna(a.get("nota_minima")) else float(a.get("nota_minima")),
                "nota_maxima": None if pd.isna(a.get("nota_maxima")) else float(a.get("nota_maxima")),
                "asignaturas_cursadas": int(a.get("asignaturas_cursadas") or 0),
                "asignaturas_aprobadas": int(a.get("aprobadas") or 0),
                "asignaturas_reprobadas": int(a.get("reprobadas") or 0),
                "clasificacion": a.get("clasificacion_rendimiento"),
            },
        }
        docs.append(doc)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(docs, f, ensure_ascii=False, indent=2)
