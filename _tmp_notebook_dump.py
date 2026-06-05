
# ==== CELL 0 title='' ====
%pip install vincenty networkx

# ==== CELL 1 title='' ====
from collections import defaultdict
from datetime import datetime
import time

import networkx as nx
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    when,
    trim,
    coalesce,
    desc,
    row_number,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    DecimalType,
)
from pyspark.sql.window import Window

from vincenty import vincenty

# from cargoOperations import global_config, LibDef, Conventions
from peanuts.ADB.orion import Orion
from peanuts.Utils.email import send_email_o365

# ==== CELL 2 title='TEST' ====
# COMMAND ----------

# =============================================================================
# Debug / parity helpers
# =============================================================================

debug_counts = []

def safe_count(df):
    try:
        return df.count()
    except Exception as e:
        print(f"Count failed: {e}")
        return None


def log_df_stage(stage_name, df, brd_col=None, off_col=None, path_col=None):
    """
    Log row count, O-D count, and path count for a Spark DataFrame.
    """
    row_count = safe_count(df)

    od_count = None
    path_count = None

    if brd_col and off_col and brd_col in df.columns and off_col in df.columns:
        od_count = safe_count(df.select(brd_col, off_col).dropDuplicates())

    if path_col and path_col in df.columns:
        path_count = safe_count(df.select(path_col).dropDuplicates())

    debug_counts.append({
        "stage": stage_name,
        "rows": row_count,
        "ods": od_count,
        "distinct_paths": path_count,
    })

    print(f"{stage_name}: rows={row_count}, ods={od_count}, distinct_paths={path_count}")


def all_results_to_df(results, stage_name):
    """
    Convert all_results snapshot to Spark DataFrame safely.
    """
    if not results:
        print(f"{stage_name}: empty all_results")
        return None

    pdf = pd.DataFrame(results)

    for c in ["CP_1", "CP_2", "CP_3", "CP_4"]:
        if c not in pdf.columns:
            pdf[c] = None

    df = spark.createDataFrame(pdf)

    if "PathPP" not in df.columns:
        df = df.withColumn(
            "PathPP",
            F.expr("""
                CASE WHEN Stops = 0 THEN CONCAT(Brd, '-', Off)
                     WHEN Stops = 1 THEN CONCAT(Brd, '-', CP_1, '-', Off)
                     WHEN Stops = 2 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', Off)
                     WHEN Stops = 3 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', Off)
                     ELSE CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', CP_4, '-', Off)
                END
            """)
        )

    df.createOrReplaceTempView(stage_name)
    log_df_stage(stage_name, df, "Brd", "Off", "PathPP")
    return df


def show_debug_counts():
    return spark.createDataFrame(pd.DataFrame(debug_counts))

# ==== CELL 3 title='' ====
external_path = "abfss://group077@aabaoriondlsprod.dfs.core.windows.net/delta-struct"

OUTPATH = "abfss://group077@aabaoriondlsprod.dfs.core.windows.net/prod/sas-cargo/output/"
INPATH = "abfss://group077@aabaoriondlsprod.dfs.core.windows.net/prod/sas-cargo/input/"

PR_SCHED = 1
PR_TRUCK = 9

# Old constants / caps
TOP_K_SMALL_POOL = 10
TOP_K_SMALL_FINAL = 8
TOP_K_LARGE_INTERLEAVE = 16

# Old CombineLogicPP keeps final top 40 candidates and exports top 30.
COMBINED_TOP_N = 40
PP_EXPORT_TOP_N = 30
FINAL_PRIORITY_TOP_N = 7

# For matching the pasted old notebook, keep this False.
# The old notebook reads exportrtg.csv but then empties it using limit(0).
INCLUDE_CURRENT_ROUTES = False

# If True, send output email.
SEND_EMAIL = True

START_TIME = datetime.now()
print(f"START_TIME: {START_TIME}")

# ==== CELL 4 title='' ====
functional_account = "MOSCOGCID10"
orion = Orion(functional_account=functional_account, group="group077", type="both")

# ==== CELL 5 title='' ====
# =============================================================================
# Utility functions
# =============================================================================

def export_csv(df, filepath):
    """
    Export a Spark DataFrame as a single CSV file.
    """
    tmp = "dbfs:/tmp/_csv_export_routing_guide"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)

    csv_files = [f.path for f in dbutils.fs.ls(tmp) if f.path.endswith(".csv")]
    if not csv_files:
        raise RuntimeError(f"No CSV part file was created in {tmp}")

    dbutils.fs.mv(csv_files[0], filepath)
    dbutils.fs.rm(tmp, recurse=True)


def vincenty_dist(lat1, lon1, lat2, lon2):
    """
    Driver-side Vincenty distance in miles.
    """
    try:
        return vincenty((lat1, lon1), (lat2, lon2), miles=True)
    except Exception:
        return None


def safe_strip(value):
    """
    Null-safe string strip.
    """
    if value is None:
        return ""
    return str(value).strip()

# ==== CELL 6 title='' ====
# =============================================================================
# LATLONG
# Old fixed-width logic:
# CITY    data[0:3]
# LATDEGS data[33:35]
# ...
# LONEW   data[47:48]
# Spark substring is 1-based, so this matches old code.
# =============================================================================

LATLONG_df = (
    spark.read.text(f"{INPATH}/stainfo_db.txt")
    .select(
        F.substring("value", 1, 3).alias("CITY"),
        F.substring("value", 34, 2).cast(IntegerType()).alias("LATDEGS"),
        F.substring("value", 36, 2).cast(IntegerType()).alias("LATMINS"),
        F.substring("value", 38, 2).cast(IntegerType()).alias("LATSECS"),
        F.substring("value", 40, 1).alias("LATNS"),
        F.substring("value", 41, 3).cast(IntegerType()).alias("LONDEGS"),
        F.substring("value", 44, 2).cast(IntegerType()).alias("LONMINS"),
        F.substring("value", 46, 2).cast(IntegerType()).alias("LONSECS"),
        F.substring("value", 48, 1).alias("LONEW"),
    )
    .withColumn("LAT", col("LATDEGS") + col("LATMINS") / 60 + col("LATSECS") / 3600)
    .withColumn("LON", col("LONDEGS") + col("LONMINS") / 60 + col("LONSECS") / 3600)
    .withColumn("LAT", when(col("LATNS") == "S", -col("LAT")).otherwise(col("LAT")))
    # Preserve old logic exactly, even though geographic convention would usually invert W.
    .withColumn("LON", when(col("LONEW") == "E", -col("LON")).otherwise(col("LON")))
    .filter("LAT IS NOT NULL AND LON IS NOT NULL AND TRIM(CITY) != ''")
    .select("CITY", "LAT", "LON")
)

coords = {
    safe_strip(row.CITY): (row.LAT, row.LON)
    for row in LATLONG_df.collect()
    if safe_strip(row.CITY)
}

print(f"Loaded coordinates for {len(coords)} stations")

# ==== CELL 7 title='' ====
# =============================================================================
# Cargo stations / inbound / outbound
# =============================================================================

sta_types = (
    spark.read.option("header", True)
    .csv(f"{INPATH}/sta_product.csv")
    .withColumn("Sta", col("STA_IAT_CD"))
    .filter(trim(col("PROD_TYPE")) != "MAIL")
)

inb_sta = {
    safe_strip(row.Sta)
    for row in (
        sta_types.filter(trim(col("PROD_DIRECTION")).isin(["I", "B"]))
        .select("Sta")
        .distinct()
        .collect()
    )
    if safe_strip(row.Sta)
}

outb_sta = {
    safe_strip(row.Sta)
    for row in (
        sta_types.filter(trim(col("PROD_DIRECTION")).isin(["O", "B"]))
        .select("Sta")
        .distinct()
        .collect()
    )
    if safe_strip(row.Sta)
}

cargo_stations = inb_sta | outb_sta

print(f"Inbound stations: {len(inb_sta)}")
print(f"Outbound stations: {len(outb_sta)}")
print(f"Cargo stations: {len(cargo_stations)}")

# ==== CELL 8 title='' ====
# =============================================================================
# Circuity restrictions
# =============================================================================

circ_restr = (
    spark.read.option("header", False)
    .csv(f"{INPATH}/Circuitey.csv")
    .toDF("StartMile", "EndMile", "MaxCirc")
    .select(
        col("StartMile").cast(DoubleType()).alias("StartMile"),
        col("EndMile").cast(DoubleType()).alias("EndMile"),
        col("MaxCirc").cast(DoubleType()).alias("MaxCirc"),
    )
    .collect()
)


def get_max_circuity(distance_miles):
    """
    Given great-circle distance in miles, return max allowed circuity.
    """
    if distance_miles is None:
        return None

    for row in circ_restr:
        if row.StartMile is None or row.EndMile is None:
            continue
        if row.StartMile <= distance_miles < row.EndMile:
            return row.MaxCirc

    return None


def get_max_circuity_or_none(distance_miles):
    max_circ = get_max_circuity(distance_miles)
    if max_circ is None:
        return None
    return max_circ


print(f"Loaded circuity restriction rows: {len(circ_restr)}")


# ==== CELL 9 title='' ====
# =============================================================================
# Station country / customs
# =============================================================================

stations_df = orion.mq("""
    SELECT cargo_agent_airprt_iata_cd AS City,
           cntry_cd AS Country
    FROM PROD_CGO_VW.CARGO_STN_MASTER_DETL
""")

station_country = {
    safe_strip(row.City): safe_strip(row.Country)
    for row in stations_df.collect()
    if safe_strip(row.City)
}

customs_df = (
    spark.read.option("header", True)
    .csv(f"{INPATH}/Sta_Facility_Prod_Customs.csv")
    .filter(trim(col("FAC_TYPE")) == "CUSTOMS")
    .filter(trim(col("CUSTOMS_PORT")) == "Y")
)

us_customs = {
    safe_strip(row.STA_IAT_CD)
    for row in customs_df.select("STA_IAT_CD").collect()
    if safe_strip(row.STA_IAT_CD)
}

GATEWAYS = {
    "JFK", "DFW", "MIA", "ORD", "CLT", "PHL", "PHX",
    "LAX", "SFO", "LAS", "LGA", "BOS", "DCA", "RDU"
}

print(f"Stations with country: {len(station_country)}")
print(f"US customs stations: {len(us_customs)}")

# ==== CELL 10 title='' ====
# =============================================================================
# Extract OAG schedule
# =============================================================================

oag_query = """
    SELECT SRVC_TYPE_CD,
           OPERAT_FLIGHT_NBR,
           OPERAT_AIRLN_IATA_CD,
           DEP_AIRPRT_IATA_CD,
           ARVL_AIRPRT_IATA_CD,
           FLIGHT_SCHD_PUBLSH_DT,
           LOCAL_DEP_DT,
           DEP_MINUTE_PAST_MDNGHT_QTY,
           FLIGHT_FIRST_AVAIL_SLS_DT,
           LOCAL_ARVL_DT,
           ARVL_MINUTE_PAST_MDNGHT_QTY,
           DEP_GMT_TMS,
           ARVL_GMT_TMS,
           SCHD_AIRCFT_CD,
           EQUIP_TTL_SEAT_QTY,
           MKT_AIRLN_IATA_CD,
           MKT_FLIGHT_NBR,
           OPERAT_PAX_FLIGHT_IND,
           FLIGHT_OAG_PUBLSH_CD
    FROM PROD_INDSTR_FLIGHT_SCHD_VW.OAG_CURR
    WHERE LOCAL_DEP_DT BETWEEN date AND date + 120
      AND OPERAT_AIRLN_IATA_CD = 'AA'
      AND MKT_AIRLN_IATA_CD = 'AA'
      AND OPERAT_AIRLN_IATA_CD = MKT_AIRLN_IATA_CD
      AND OPERAT_FLIGHT_NBR = MKT_FLIGHT_NBR
      AND OPERAT_PAX_FLIGHT_IND = 'Y'
      AND FLIGHT_OAG_PUBLSH_CD <> 'X'
"""

oag_raw = orion.mq(oag_query)

if isinstance(oag_raw, pd.DataFrame):
    oag_raw = spark.createDataFrame(oag_raw)

oag_raw.createOrReplaceTempView("oag_raw")

print(f"OAG rows: {oag_raw.count()}")

# ==== CELL 11 title='' ====
# =============================================================================
# Latest schedule selection and Schedule table
# Old GetSched grouped latest schedule by:
# FN, AL, BRD, OFF, LDD, LDT
# Then joined back on max PD.
# =============================================================================

somevars = (
    oag_raw.select(
        col("FLIGHT_SCHD_PUBLSH_DT").alias("PD"),
        col("LOCAL_DEP_DT").alias("LDD"),
        col("DEP_MINUTE_PAST_MDNGHT_QTY").alias("LDT_MIN"),
        col("OPERAT_FLIGHT_NBR").alias("FN"),
        trim(col("OPERAT_AIRLN_IATA_CD")).alias("AL"),
        trim(col("DEP_AIRPRT_IATA_CD")).alias("brd"),
        trim(col("ARVL_AIRPRT_IATA_CD")).alias("off"),
        trim(col("SCHD_AIRCFT_CD")).alias("Eqp"),
    )
    .withColumn(
        "WB_ind",
        when(F.substring(trim(col("Eqp")), 1, 2).isin("33", "76", "77", "78"), 1).otherwise(0),
    )
)

somevars_for_latest = somevars.filter(
    col("brd").isin(list(outb_sta)) & col("off").isin(list(inb_sta))
)

latest_keys = (
    somevars_for_latest.groupBy("FN", "AL", "brd", "off", "LDD", "LDT_MIN")
    .agg(F.max("PD").alias("maxPD"))
)

a = somevars_for_latest.alias("a")
b = latest_keys.alias("b")

latest_sched = (
    a.join(
        b,
        (
            (col("a.FN") == col("b.FN"))
            & (col("a.AL") == col("b.AL"))
            & (col("a.brd") == col("b.brd"))
            & (col("a.off") == col("b.off"))
            & (col("a.LDD") == col("b.LDD"))
            & (col("a.LDT_MIN") == col("b.LDT_MIN"))
            & (col("a.PD") == col("b.maxPD"))
        ),
        "inner",
    )
    .select("a.*")
)

Schedule = (
    latest_sched.groupBy("LDD", "brd", "off")
    .agg(
        F.sum("WB_ind").alias("wb_sum"),
        F.count("*").alias("freq"),
    )
    .groupBy("brd", "off")
    .agg(
        F.avg("freq").alias("AvgFreq"),
        F.avg("wb_sum").alias("WB"),
    )
    .withColumn("WB_available", when(col("WB") > 0.05, 1).otherwise(0))
)

flight_edges = Schedule.collect()

print(f"Flight O-D edges before customs/distance filtering: {len(flight_edges)}")

# ==== CELL 12 title='TEST' ====
# DEBUG
Schedule_debug = Schedule.select(
    col("brd").alias("Orig"),
    col("off").alias("Dest"),
    "AvgFreq",
    "WB",
    "WB_available"
)

Schedule_debug.createOrReplaceTempView("NEW_01_Schedule")
log_df_stage("NEW_01_Schedule", Schedule_debug, "Orig", "Dest")

# ==== CELL 13 title='' ====
# =============================================================================
# Truck schedule / NewMarkets
# =============================================================================

NewMkts = (
    spark.read.option("header", True)
    .csv(f"{INPATH}/TruckSchedule.csv")
    .withColumn("Orig", F.substring(col("OriginDestination"), 1, 3))
    .withColumn("Dest", F.substring(col("OriginDestination"), 4, 3))
    .filter("length(Orig) = 3 AND length(Dest) = 3")
    .select("Orig", "Dest")
    .dropDuplicates(["Orig", "Dest"])
)

truck_edges = NewMkts.collect()

# TESTING---------------------------
# DEBUG
NewMkts.createOrReplaceTempView("NEW_02_NewMarkets")
log_df_stage("NEW_02_NewMarkets", NewMkts, "Orig", "Dest")
# TESTING---------------------------

print(f"Truck edges from TruckSchedule.csv: {len(truck_edges)}")

truck_only_stations = set()

for row in truck_edges:
    orig = safe_strip(row.Orig)
    dest = safe_strip(row.Dest)

    if orig and orig not in cargo_stations:
        truck_only_stations.add(orig)

    if dest and dest not in cargo_stations:
        truck_only_stations.add(dest)

print(f"Truck-only stations excluded as CPs: {len(truck_only_stations)}")

# ==== CELL 14 title='' ====
# =============================================================================
# Build flight graph and separate truck edge store
# =============================================================================

G = nx.DiGraph()

truck_edge_data = {}
truck_in_to = defaultdict(list)
truck_out_from = defaultdict(list)

# Flight graph
for row in flight_edges:
    orig = safe_strip(row.brd)
    dest = safe_strip(row.off)

    if not orig or not dest:
        continue

    orig_country = station_country.get(orig, "")
    dest_country = station_country.get(dest, "")

    # Old customs rule: remove US -> non-US if origin lacks customs.
    if orig_country == "US" and dest_country != "US" and orig not in us_customs:
        continue

    if orig in coords and dest in coords:
        dist = vincenty_dist(*coords[orig], *coords[dest])
    else:
        dist = None

    if dist is None or dist <= 0:
        continue

    G.add_edge(
        orig,
        dest,
        distance=dist,
        freq=row.AvgFreq,
        wb=row.WB,
        wb_available=int(row.WB_available),
        mode="F",
        priority=PR_SCHED,
    )

# Truck edge store
# Do not skip truck edges when a same O-D flight exists.
for row in truck_edges:
    orig = safe_strip(row.Orig)
    dest = safe_strip(row.Dest)

    if not orig or not dest:
        continue

    if orig in coords and dest in coords:
        dist = vincenty_dist(*coords[orig], *coords[dest])
    else:
        dist = None

    if dist is None or dist <= 0:
        continue

    orig_country = station_country.get(orig, "")
    dest_country = station_country.get(dest, "")

    # Old GB-origin truck restriction.
    if orig_country == "GB" and dest_country != "GB":
        continue

    truck_edge_data[(orig, dest)] = {
        "distance": dist,
        "freq": 999,
        "wb": 0,
        "wb_available": 0,
        "mode": "T",
        "priority": PR_TRUCK,
    }

    truck_out_from[orig].append(dest)
    truck_in_to[dest].append(orig)

print(f"Flight graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} flight edges")
print(f"Truck edges stored separately: {len(truck_edge_data)}")

none_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("distance") is None]
print(f"Flight edges with None distance: {len(none_edges)}")


# ==== CELL 15 title='' ====
# =============================================================================
# Path validation and metric helpers
# =============================================================================

def station_sequence_is_valid(path):
    """
    Old RemoveNonUSCp / gateway rules for station sequence.
    Does not require graph edge existence.
    """
    if len(path) < 2:
        return False

    intermediates = path[1:-1]

    for cp in intermediates:
        if cp in truck_only_stations:
            return False

    countries = [station_country.get(s, "") for s in path]
    orig_country = countries[0]
    dest_country = countries[-1]
    cp_countries = countries[1:-1]

    # Any US intermediate must be an approved gateway.
    for cp in intermediates:
        if station_country.get(cp, "") == "US" and cp not in GATEWAYS:
            return False

    # 1-stop rules
    if len(intermediates) == 1:
        if orig_country != "US" and cp_countries[0] != "US" and dest_country != "US":
            return False

        if orig_country == "US" and cp_countries[0] != "US" and dest_country == "US":
            return False

        if orig_country == dest_country and cp_countries[0] == "US" and orig_country != "US":
            return False

    # 2-stop rules
    if len(intermediates) == 2:
        if "US" not in cp_countries:
            return False

        if orig_country == "US" and cp_countries[0] != "US" and cp_countries[1] == "US":
            return False

        if cp_countries[0] == "US" and cp_countries[1] != "US" and dest_country == "US":
            return False

        if orig_country == dest_country and orig_country != "US":
            return False

        if orig_country == cp_countries[1] and orig_country != "US" and cp_countries[0] == "US":
            return False

        if cp_countries[0] == dest_country and cp_countries[0] != "US" and cp_countries[1] == "US":
            return False

    return True


def all_flight_edges_exist(path):
    for i in range(len(path) - 1):
        edge = G.get_edge_data(path[i], path[i + 1])
        if edge is None or edge.get("mode") != "F":
            return False
    return True


def is_valid_flight_path(path):
    if len(path) < 2:
        return False

    if not all_flight_edges_exist(path):
        return False

    return station_sequence_is_valid(path)


def matches_old_truck_pattern_for_modes(modes):
    """
    Old AddTruckRoutes allows truck legs only at beginning and/or end
    of an otherwise flight-only path.
    """
    if not modes:
        return False

    if "T" not in modes:
        return all(m == "F" for m in modes)

    if "F" not in modes:
        return False

    first_f = modes.index("F")
    last_f = len(modes) - 1 - modes[::-1].index("F")

    if first_f > 1:
        return False

    if len(modes) - 1 - last_f > 1:
        return False

    if any(m != "T" for m in modes[:first_f]):
        return False

    if any(m != "F" for m in modes[first_f:last_f + 1]):
        return False

    if any(m != "T" for m in modes[last_f + 1:]):
        return False

    return True


def get_edge_for_mode(orig, dest, mode):
    if mode == "F":
        return G.get_edge_data(orig, dest)
    if mode == "T":
        return truck_edge_data.get((orig, dest))
    return None


def compute_flight_path_metrics(path):
    """
    Compute old strict metrics for flight-only paths, including two-stop sub-leg circuity.
    """
    orig = path[0]
    dest = path[-1]

    if orig not in coords or dest not in coords:
        return None

    gcd = vincenty_dist(*coords[orig], *coords[dest])
    if gcd is None or gcd <= 0:
        return None

    max_circ = get_max_circuity_or_none(gcd)
    if max_circ is None:
        return None

    path_dist = 0
    total_wb = 0
    total_freq = 0
    min_wb_avail = 1

    for i in range(len(path) - 1):
        edge = G.get_edge_data(path[i], path[i + 1])
        if edge is None:
            return None

        leg_dist = edge.get("distance")
        if leg_dist is None or leg_dist <= 0:
            return None

        path_dist += leg_dist
        total_wb += edge.get("wb", 0)
        total_freq += edge.get("freq", 0)
        min_wb_avail = min(min_wb_avail, edge.get("wb_available", 0))

    circuity = path_dist / gcd

    if circuity > max_circ:
        return None

    # Old two-stop sub-leg circuity checks
    if len(path) == 4:
        # Orig -> CP2 through CP1
        if path[0] in coords and path[2] in coords:
            sub_gcd_12 = vincenty_dist(*coords[path[0]], *coords[path[2]])
            if sub_gcd_12 and sub_gcd_12 > 0:
                sub_max_12 = get_max_circuity_or_none(sub_gcd_12)
                if sub_max_12 is None:
                    return None

                sub_dist_12 = G[path[0]][path[1]]["distance"] + G[path[1]][path[2]]["distance"]
                if sub_dist_12 / sub_gcd_12 > sub_max_12:
                    return None

        # CP1 -> Dest through CP2
        if path[1] in coords and path[3] in coords:
            sub_gcd_23 = vincenty_dist(*coords[path[1]], *coords[path[3]])
            if sub_gcd_23 and sub_gcd_23 > 0:
                sub_max_23 = get_max_circuity_or_none(sub_gcd_23)
                if sub_max_23 is None:
                    return None

                sub_dist_23 = G[path[1]][path[2]]["distance"] + G[path[2]][path[3]]["distance"]
                if sub_dist_23 / sub_gcd_23 > sub_max_23:
                    return None

    return {
        "gcd": gcd,
        "path_dist": path_dist,
        "circuity": circuity,
        "total_wb": total_wb,
        "total_freq": total_freq,
        "wb_available": min_wb_avail,
        "has_truck": False,
        "flight_truck": "F",
        "stops": len(path) - 2,
    }


def compute_mixed_path_metrics(path, modes):
    """
    Compute old-style truck-assisted metrics.
    """
    if len(path) < 2:
        return None

    if len(modes) != len(path) - 1:
        return None

    if not matches_old_truck_pattern_for_modes(modes):
        return None

    if not station_sequence_is_valid(path):
        return None

    orig = path[0]
    dest = path[-1]

    if orig not in coords or dest not in coords:
        return None

    gcd = vincenty_dist(*coords[orig], *coords[dest])
    if gcd is None or gcd <= 0:
        return None

    max_circ = get_max_circuity_or_none(gcd)
    if max_circ is None:
        return None

    path_dist = 0
    total_wb = 0
    total_freq = 0
    min_wb_avail = 1

    for i, mode in enumerate(modes):
        edge = get_edge_for_mode(path[i], path[i + 1], mode)
        if edge is None:
            return None

        leg_dist = edge.get("distance")
        if leg_dist is None or leg_dist <= 0:
            return None

        path_dist += leg_dist
        total_wb += edge.get("wb", 0)
        total_freq += edge.get("freq", 0)
        min_wb_avail = min(min_wb_avail, edge.get("wb_available", 0))

    circuity = path_dist / gcd

    if circuity > max_circ:
        return None

    has_truck = "T" in modes

    # Old truck rule:
    # WB_availT = 1 if TotWB > 0.05 else 0.
    if has_truck:
        wb_available = 1 if total_wb > 0.05 else 0
    else:
        wb_available = min_wb_avail

    return {
        "gcd": gcd,
        "path_dist": path_dist,
        "circuity": circuity,
        "total_wb": total_wb,
        "total_freq": total_freq,
        "wb_available": wb_available,
        "has_truck": has_truck,
        "flight_truck": "T" if has_truck else "F",
        "stops": len(path) - 2,
    }


def make_result_from_path(path, metrics, grade, priority):
    return {
        "Brd": path[0],
        "Off": path[-1],
        "CP_1": path[1] if len(path) > 2 else None,
        "CP_2": path[2] if len(path) > 3 else None,
        "CP_3": path[3] if len(path) > 4 else None,
        "CP_4": path[4] if len(path) > 5 else None,
        "Stops": len(path) - 2,
        "FlightTruck": metrics["flight_truck"],
        "TotFreq": metrics["total_freq"],
        "TotWB": metrics["total_wb"],
        "WB_available": metrics["wb_available"],
        "Circuity": metrics["circuity"],
        "GCD": metrics["gcd"],
        "PathDistance": metrics["path_dist"],
        "Grade": grade,
        "Priority": priority,
    }


def path_from_result_row(r):
    path = [r["Brd"]]

    for cp_col in ["CP_1", "CP_2", "CP_3", "CP_4"]:
        cp = r.get(cp_col)
        if cp is not None and safe_strip(cp):
            path.append(safe_strip(cp))

    path.append(r["Off"])
    return path


# ==== CELL 16 title='' ====
# =============================================================================
# Enumerate strict flight-only 0/1/2-stop paths
# =============================================================================

all_origins = set(G.nodes()) & outb_sta
all_dests = set(G.nodes()) & inb_sta

od_pairs = [(o, d) for o in all_origins for d in all_dests if o != d]

print(f"Potential O-D pairs: {len(od_pairs)}")

all_results = []

t0 = time.time()
total = len(od_pairs)

print("Starting strict flight path enumeration...")
print("-" * 80)

for i, (orig, dest) in enumerate(od_pairs):
    if orig not in coords or dest not in coords:
        continue

    gcd = vincenty_dist(*coords[orig], *coords[dest])
    if gcd is None or gcd <= 0:
        continue

    max_circ = get_max_circuity_or_none(gcd)
    if max_circ is None:
        continue

    candidates = []

    # 0-stop
    if G.has_edge(orig, dest):
        d = G[orig][dest]["distance"]
        if d / gcd <= max_circ:
            candidates.append(([orig, dest], d))

    # 1-stop
    for cp1 in G.successors(orig):
        if cp1 in {orig, dest}:
            continue

        if not G.has_edge(cp1, dest):
            continue

        d = G[orig][cp1]["distance"] + G[cp1][dest]["distance"]

        if d / gcd <= max_circ:
            candidates.append(([orig, cp1, dest], d))

    # 2-stop
    for cp1 in G.successors(orig):
        if cp1 in {orig, dest}:
            continue

        d1 = G[orig][cp1]["distance"]

        if d1 > gcd * max_circ:
            continue

        for cp2 in G.successors(cp1):
            if cp2 in {orig, cp1, dest}:
                continue

            if not G.has_edge(cp2, dest):
                continue

            d = d1 + G[cp1][cp2]["distance"] + G[cp2][dest]["distance"]

            if d / gcd <= max_circ:
                candidates.append(([orig, cp1, cp2, dest], d))

    def strict_flight_sort_key(candidate):
        path, path_dist = candidate
        stops = len(path) - 2
        total_freq = sum(G[path[j]][path[j + 1]].get("freq", 0) for j in range(len(path) - 1))
        total_wb = sum(G[path[j]][path[j + 1]].get("wb", 0) for j in range(len(path) - 1))
        circuity = path_dist / gcd if gcd > 0 else 1.0
        return (stops, -total_freq, circuity, -total_wb, "-".join(path))

    candidates.sort(key=strict_flight_sort_key)

    rank = 0

    for path, _ in candidates:
        if not is_valid_flight_path(path):
            continue

        metrics = compute_flight_path_metrics(path)
        if metrics is None:
            continue

        rank += 1
        all_results.append(make_result_from_path(path, metrics, rank, PR_SCHED))

    if (i + 1) % 500 == 0:
        elapsed = time.time() - t0
        rate = (i + 1) / elapsed if elapsed > 0 else 0
        remaining = (total - i - 1) / rate if rate > 0 else 0

        print(
            f"[{i + 1:>6}/{total}] "
            f"{(i + 1) / total * 100:.1f}% | "
            f"{len(all_results)} paths | "
            f"{rate:.0f} pairs/sec | "
            f"ETA {remaining / 60:.1f} min"
        )

elapsed = time.time() - t0
print("-" * 80)
print(f"Strict flight enumeration complete: {len(all_results)} paths in {elapsed / 60:.1f} min")

# ==== CELL 17 title='TESTING' ====
# DEBUG
strict_flight_results_snapshot = list(all_results)
NEW_03_StrictFlightPaths = all_results_to_df(
    strict_flight_results_snapshot,
    "NEW_03_StrictFlightPaths"
)

# ==== CELL 18 title='' ====
# =============================================================================
# WB relaxed circuity recovery
# =============================================================================

print("\nStarting WB relaxed circuity recovery...")

od_with_wb = {
    (r["Brd"], r["Off"])
    for r in all_results
    if r["WB_available"] == 1
}

od_missing_wb = [
    (o, d)
    for o, d in od_pairs
    if (o, d) not in od_with_wb
]

print(f"O-D pairs missing WB path: {len(od_missing_wb)}")

wb_relaxed_results = []
t1 = time.time()


def all_edges_have_wb(path):
    for j in range(len(path) - 1):
        edge = G.get_edge_data(path[j], path[j + 1])
        if edge is None or edge.get("wb_available", 0) != 1:
            return False
    return True


def relaxed_two_stop_sublegs(path, total_circuity):
    """
    Old relaxed recovery logic:
    - strict recovery: total circuity < 3.0 and sub-leg <= 1.5
    - lenient recovery: total circuity < 1.7 and sub-leg <= 2.0
    """
    if len(path) != 4:
        return True

    orig, cp1, cp2, dest = path

    sub12 = None
    sub23 = None

    if orig in coords and cp2 in coords:
        sub_gcd_12 = vincenty_dist(*coords[orig], *coords[cp2])
        if sub_gcd_12 and sub_gcd_12 > 0:
            sub_dist_12 = G[orig][cp1]["distance"] + G[cp1][cp2]["distance"]
            sub12 = sub_dist_12 / sub_gcd_12

    if cp1 in coords and dest in coords:
        sub_gcd_23 = vincenty_dist(*coords[cp1], *coords[dest])
        if sub_gcd_23 and sub_gcd_23 > 0:
            sub_dist_23 = G[cp1][cp2]["distance"] + G[cp2][dest]["distance"]
            sub23 = sub_dist_23 / sub_gcd_23

    strict_ok = (
        total_circuity < 3.0
        and (sub12 is None or sub12 <= 1.5)
        and (sub23 is None or sub23 <= 1.5)
    )

    lenient_ok = (
        total_circuity < 1.7
        and (sub12 is None or sub12 <= 2.0)
        and (sub23 is None or sub23 <= 2.0)
    )

    return strict_ok or lenient_ok


for orig, dest in od_missing_wb:
    if orig not in coords or dest not in coords:
        continue

    gcd = vincenty_dist(*coords[orig], *coords[dest])
    if gcd is None or gcd <= 0:
        continue

    relaxed_max_circ = 3.0
    max_path_distance = 7000

    candidates = []

    # 0-stop
    if G.has_edge(orig, dest):
        path = [orig, dest]
        edge = G[orig][dest]
        if edge.get("wb_available", 0) == 1:
            d = edge["distance"]
            if d / gcd <= relaxed_max_circ and d <= max_path_distance:
                candidates.append((path, d))

    # 1-stop
    for cp1 in G.successors(orig):
        if cp1 in {orig, dest}:
            continue

        if not G.has_edge(cp1, dest):
            continue

        path = [orig, cp1, dest]

        if not all_edges_have_wb(path):
            continue

        d = G[orig][cp1]["distance"] + G[cp1][dest]["distance"]

        if d / gcd <= relaxed_max_circ and d <= max_path_distance:
            candidates.append((path, d))

    # 2-stop
    for cp1 in G.successors(orig):
        if cp1 in {orig, dest}:
            continue

        d1 = G[orig][cp1]["distance"]

        if d1 > gcd * relaxed_max_circ:
            continue

        for cp2 in G.successors(cp1):
            if cp2 in {orig, cp1, dest}:
                continue

            if not G.has_edge(cp2, dest):
                continue

            path = [orig, cp1, cp2, dest]

            if not all_edges_have_wb(path):
                continue

            d = d1 + G[cp1][cp2]["distance"] + G[cp2][dest]["distance"]
            circuity = d / gcd

            if circuity > relaxed_max_circ or d > max_path_distance:
                continue

            if not relaxed_two_stop_sublegs(path, circuity):
                continue

            candidates.append((path, d))

    if not candidates:
        continue

    # Old fallback keeps one best relaxed path per O-D by circuity.
    candidates.sort(key=lambda x: (x[1] / gcd, "-".join(x[0])))

    path, path_dist = candidates[0]

    if not station_sequence_is_valid(path):
        continue

    circuity = path_dist / gcd

    total_wb = 0
    total_freq = 0
    min_wb_avail = 1

    for j in range(len(path) - 1):
        edge = G[path[j]][path[j + 1]]
        total_wb += edge.get("wb", 0)
        total_freq += edge.get("freq", 0)
        min_wb_avail = min(min_wb_avail, edge.get("wb_available", 0))

    wb_relaxed_results.append({
        "Brd": orig,
        "Off": dest,
        "CP_1": path[1] if len(path) > 2 else None,
        "CP_2": path[2] if len(path) > 3 else None,
        "CP_3": None,
        "CP_4": None,
        "Stops": len(path) - 2,
        "FlightTruck": "F",
        "TotFreq": total_freq,
        "TotWB": total_wb,
        "WB_available": min_wb_avail,
        "Circuity": circuity,
        "GCD": gcd,
        "PathDistance": path_dist,
        "Grade": 99,
        "Priority": PR_SCHED,
    })

all_results.extend(wb_relaxed_results)

print(f"WB relaxed paths added: {len(wb_relaxed_results)} in {time.time() - t1:.1f}s")
print(f"Total paths after WB recovery: {len(all_results)}")

# ==== CELL 19 title='TESTING' ====
# DEBUG
after_wb_relaxed_snapshot = list(all_results)
NEW_04_AfterWBRelaxed = all_results_to_df(
    after_wb_relaxed_snapshot,
    "NEW_04_AfterWBRelaxed"
)

if NEW_03_StrictFlightPaths is not None and NEW_04_AfterWBRelaxed is not None:
    NEW_04_WBRelaxedOnly = NEW_04_AfterWBRelaxed.join(
        NEW_03_StrictFlightPaths.select("Brd", "Off", "PathPP").dropDuplicates(),
        ["Brd", "Off", "PathPP"],
        "left_anti"
    )
    NEW_04_WBRelaxedOnly.createOrReplaceTempView("NEW_04_WBRelaxedOnly")
    log_df_stage("NEW_04_WBRelaxedOnly", NEW_04_WBRelaxedOnly, "Brd", "Off", "PathPP")

# ==== CELL 20 title='' ====
# =============================================================================
# Manual AddCPs.txt
# Old AddCPS appends manual one/two CPs with Priority = 2.
# Do not require graph legs.
# =============================================================================

print("\nChecking AddCPs.txt manual paths...")

manual_cp_results = []

try:
    addcp_lines = spark.read.text(f"{INPATH}/AddCPs.txt").collect()
except Exception as e:
    addcp_lines = []
    print(f"Could not read AddCPs.txt or file is missing: {e}")

existing_manual_keys = {
    (
        r["Brd"],
        r["Off"],
        r.get("CP_1"),
        r.get("CP_2"),
        r.get("CP_3"),
        r.get("CP_4"),
    )
    for r in all_results
}

for row in addcp_lines:
    line = row["value"]
    if line is None:
        continue

    line = line.rstrip("\n")

    if len(line.strip()) < 9:
        continue

    orig = line[0:3].strip()
    dest = line[3:6].strip()
    cp1 = line[6:9].strip()
    cp2 = line[9:12].strip() if len(line) >= 12 else ""

    if len(orig) != 3 or len(dest) != 3 or len(cp1) != 3:
        continue

    path = [orig, cp1, dest] if len(cp2) != 3 else [orig, cp1, cp2, dest]

    if any(p not in coords for p in path):
        continue

    gcd = vincenty_dist(*coords[orig], *coords[dest])
    if gcd is None or gcd <= 0:
        continue

    max_circ = get_max_circuity_or_none(gcd)
    if max_circ is None:
        continue

    path_dist = 0
    valid_dist = True

    for j in range(len(path) - 1):
        d = vincenty_dist(*coords[path[j]], *coords[path[j + 1]])
        if d is None or d <= 0:
            valid_dist = False
            break
        path_dist += d

    if not valid_dist:
        continue

    circuity = path_dist / gcd

    # The graph notebook does not have the old final universal circuity recompute,
    # so enforce total circuity here.
    if circuity > max_circ:
        continue

    key = (
        orig,
        dest,
        path[1] if len(path) > 2 else None,
        path[2] if len(path) > 3 else None,
        None,
        None,
    )

    if key in existing_manual_keys:
        continue

    manual_cp_results.append({
        "Brd": orig,
        "Off": dest,
        "CP_1": path[1] if len(path) > 2 else None,
        "CP_2": path[2] if len(path) > 3 else None,
        "CP_3": None,
        "CP_4": None,
        "Stops": len(path) - 2,
        "FlightTruck": "F",
        "TotFreq": 0,
        "TotWB": 0,
        "WB_available": 0,
        "Circuity": circuity,
        "GCD": gcd,
        "PathDistance": path_dist,
        "Grade": 999,
        "Priority": 2,
    })

    existing_manual_keys.add(key)

all_results.extend(manual_cp_results)

print(f"Manual AddCPs paths added: {len(manual_cp_results)}")
print(f"Total paths after AddCPs: {len(all_results)}")

# ==== CELL 21 title='TESTING' ====
# DEBUG
after_addcps_snapshot = list(all_results)
NEW_05_AfterAddCPs = all_results_to_df(
    after_addcps_snapshot,
    "NEW_05_AfterAddCPs"
)

if NEW_04_AfterWBRelaxed is not None and NEW_05_AfterAddCPs is not None:
    NEW_05_AddCPsOnly = NEW_05_AfterAddCPs.join(
        NEW_04_AfterWBRelaxed.select("Brd", "Off", "PathPP").dropDuplicates(),
        ["Brd", "Off", "PathPP"],
        "left_anti"
    )
    NEW_05_AddCPsOnly.createOrReplaceTempView("NEW_05_AddCPsOnly")
    log_df_stage("NEW_05_AddCPsOnly", NEW_05_AddCPsOnly, "Brd", "Off", "PathPP")

# ==== CELL 22 title='' ====
# =============================================================================
# Old-style AddTruckRoutes — Spark-safe version
# - No nonstop_lookup
# - No Python UDFs
# - No toPandas()
# - Includes NULL-safe US gateway CP filtering
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from pyspark.storagelevel import StorageLevel

DEBUG_TRUCK_AUGMENTATION = True

print("\nStarting old-style truck augmentation...")

# -----------------------------------------------------------------------------
# 1. Build flight-only pool from all_results
# -----------------------------------------------------------------------------

flight_only_rows = [
    r for r in all_results
    if r.get("FlightTruck") == "F" and r.get("Stops", 99) <= 2
]

if not flight_only_rows:
    print("No flight-only rows found. Skipping truck augmentation.")
    truck_augmented_results = []

else:
    flight_pool_df = spark.createDataFrame(pd.DataFrame(flight_only_rows))

    for c_name in ["CP_1", "CP_2", "CP_3", "CP_4"]:
        if c_name not in flight_pool_df.columns:
            flight_pool_df = flight_pool_df.withColumn(c_name, lit(None).cast("string"))

    flight_pool_df = flight_pool_df.withColumn(
        "Path",
        F.expr("""
            CASE WHEN Stops = 0 THEN CONCAT('"', Brd, ',', Off, '"')
                 WHEN Stops = 1 THEN CONCAT('"', Brd, ',', CP_1, ',', Off, '"')
                 ELSE CONCAT('"', Brd, ',', CP_1, ',', CP_2, ',', Off, '"')
            END
        """)
    )

    # -------------------------------------------------------------------------
    # 2. Build old small-shipment truck base:
    #    rankings = top 10 flight paths per O-D
    #    WB_del   = paths beyond top 10 with TotWB > 0.02
    #    plus direct schedule/nonstop rows
    # -------------------------------------------------------------------------

    w_old_small_pretruck = Window.partitionBy("Brd", "Off").orderBy(
        "Stops",
        "Priority",
        desc("TotFreq"),
        "Circuity",
        desc("WB_available"),
        desc("TotWB"),
        "Path",
    )

    ranked_small = flight_pool_df.withColumn(
        "NumPath",
        row_number().over(w_old_small_pretruck)
    )

    old_small_rankings = ranked_small.filter(col("NumPath") <= 10)
    old_small_wb_del = ranked_small.filter(
        (col("NumPath") > 10) & (col("TotWB") > 0.02)
    )

    flights_for_trucks_df = old_small_rankings.unionByName(
        old_small_wb_del,
        allowMissingColumns=True,
    )

    flights_for_trucks_filtered = flights_for_trucks_df.filter(
        col("Stops").isin(1, 2)
    )

    direct_schedule_df = flight_pool_df.filter(
        (col("Stops") == 0) & (col("Priority") == PR_SCHED)
    )

    base_flight_df = direct_schedule_df.unionByName(
        flights_for_trucks_filtered,
        allowMissingColumns=True,
    ).persist(StorageLevel.MEMORY_AND_DISK)

    print("Truck base debug:")
    print("Rows:", base_flight_df.count())
    print("ODs:", base_flight_df.select("Brd", "Off").dropDuplicates().count())

    if DEBUG_TRUCK_AUGMENTATION:
        display(
            base_flight_df.groupBy("Stops", "WB_available")
            .count()
            .orderBy("Stops", "WB_available")
        )

    # -------------------------------------------------------------------------
    # 3. Build truck-in / truck-out lookup DataFrames
    # -------------------------------------------------------------------------

    truck_in_rows = [
        (truck_orig, flight_orig)
        for flight_orig, truck_origs in truck_in_to.items()
        for truck_orig in truck_origs
    ]

    truck_out_rows = [
        (flight_dest, truck_dest)
        for flight_dest, truck_dests in truck_out_from.items()
        for truck_dest in truck_dests
    ]

    truck_in_schema = StructType([
        StructField("TruckOrig", StringType(), False),
        StructField("FlightOrig", StringType(), False),
    ])

    truck_out_schema = StructType([
        StructField("FlightDest", StringType(), False),
        StructField("TruckDest", StringType(), False),
    ])

    truck_in_df = spark.createDataFrame(truck_in_rows, schema=truck_in_schema)
    truck_out_df = spark.createDataFrame(truck_out_rows, schema=truck_out_schema)

    # Truck lookup tables are small.
    truck_in_df = F.broadcast(truck_in_df)
    truck_out_df = F.broadcast(truck_out_df)

    # -------------------------------------------------------------------------
    # 4. Generate old AddTruckRoutes-style structural candidates
    #    Allowed:
    #      truck + flight
    #      flight + truck
    #      truck + flight + truck
    # -------------------------------------------------------------------------

    b = base_flight_df.alias("b")
    ti = truck_in_df.alias("ti")
    to = truck_out_df.alias("to")

    # Truck + flight
    aug_front = (
        b.join(ti, col("b.Brd") == col("ti.FlightOrig"), "inner")
        .select(
            col("ti.TruckOrig").alias("Brd"),
            col("b.Off").alias("Off"),
            col("b.Brd").alias("CP_1"),
            when(col("b.Stops") >= 1, col("b.CP_1")).cast("string").alias("CP_2"),
            when(col("b.Stops") >= 2, col("b.CP_2")).cast("string").alias("CP_3"),
            lit(None).cast("string").alias("CP_4"),
            (col("b.Stops") + lit(1)).alias("Stops"),
            lit("front").alias("TruckPosition"),
        )
    )

    # Flight + truck
    aug_end = (
        b.join(to, col("b.Off") == col("to.FlightDest"), "inner")
        .select(
            col("b.Brd").alias("Brd"),
            col("to.TruckDest").alias("Off"),

            # old path + truck:
            # stops 0: Brd-Off-TruckDest => CP_1 = old Off
            # stops 1: Brd-CP1-Off-TruckDest => CP_1 = old CP1, CP_2 = old Off
            # stops 2: Brd-CP1-CP2-Off-TruckDest => CP_1 = old CP1, CP_2 = old CP2, CP_3 = old Off
            when(col("b.Stops") == 0, col("b.Off"))
            .otherwise(col("b.CP_1")).cast("string").alias("CP_1"),

            when(col("b.Stops") == 1, col("b.Off"))
            .when(col("b.Stops") >= 2, col("b.CP_2"))
            .otherwise(lit(None)).cast("string").alias("CP_2"),

            when(col("b.Stops") >= 2, col("b.Off"))
            .otherwise(lit(None)).cast("string").alias("CP_3"),

            lit(None).cast("string").alias("CP_4"),
            (col("b.Stops") + lit(1)).alias("Stops"),
            lit("end").alias("TruckPosition"),
        )
    )

    # Truck + flight + truck
    aug_both = (
        b.join(ti, col("b.Brd") == col("ti.FlightOrig"), "inner")
        .join(to, col("b.Off") == col("to.FlightDest"), "inner")
        .select(
            col("ti.TruckOrig").alias("Brd"),
            col("to.TruckDest").alias("Off"),

            col("b.Brd").alias("CP_1"),

            when(col("b.Stops") == 0, col("b.Off"))
            .otherwise(col("b.CP_1")).cast("string").alias("CP_2"),

            when(col("b.Stops") == 1, col("b.Off"))
            .when(col("b.Stops") >= 2, col("b.CP_2"))
            .otherwise(lit(None)).cast("string").alias("CP_3"),

            when(col("b.Stops") >= 2, col("b.Off"))
            .otherwise(lit(None)).cast("string").alias("CP_4"),

            (col("b.Stops") + lit(2)).alias("Stops"),
            lit("both").alias("TruckPosition"),
        )
    )

    all_truck_candidates = (
        aug_front
        .unionByName(aug_end, allowMissingColumns=True)
        .unionByName(aug_both, allowMissingColumns=True)
        .filter(col("Stops") <= 4)
        .dropDuplicates(["Brd", "Off", "CP_1", "CP_2", "CP_3", "CP_4"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    print("Raw structural truck candidates:")
    print("Rows:", all_truck_candidates.count())
    print("ODs:", all_truck_candidates.select("Brd", "Off").dropDuplicates().count())

    # -------------------------------------------------------------------------
    # 5. Build edge lookup DataFrame from flight graph + truck edge store
    # -------------------------------------------------------------------------

    edge_rows = []

    for u, v, d in G.edges(data=True):
        if d.get("mode") == "F":
            edge_rows.append((
                safe_strip(u),
                safe_strip(v),
                "F",
                float(d.get("distance") or 0),
                float(d.get("freq") or 0),
                float(d.get("wb") or 0),
                int(d.get("wb_available") or 0),
            ))

    for (u, v), d in truck_edge_data.items():
        edge_rows.append((
            safe_strip(u),
            safe_strip(v),
            "T",
            float(d.get("distance") or 0),
            float(d.get("freq") or 0),
            float(d.get("wb") or 0),
            int(d.get("wb_available") or 0),
        ))

    edge_schema = StructType([
        StructField("EdgeOrig", StringType(), False),
        StructField("EdgeDest", StringType(), False),
        StructField("EdgeMode", StringType(), False),
        StructField("EdgeDistance", DoubleType(), False),
        StructField("EdgeFreq", DoubleType(), False),
        StructField("EdgeWB", DoubleType(), False),
        StructField("EdgeWBAvailable", IntegerType(), False),
    ])

    edge_df = (
        spark.createDataFrame(edge_rows, schema=edge_schema)
        .dropDuplicates(["EdgeOrig", "EdgeDest", "EdgeMode"])
    )

    edge_df = F.broadcast(edge_df)

    # -------------------------------------------------------------------------
    # 6. Add leg origin/dest/mode columns
    # -------------------------------------------------------------------------

    c = all_truck_candidates

    c = (
        c
        .withColumn("L1Orig", col("Brd"))
        .withColumn("L1Dest", col("CP_1"))

        .withColumn("L2Orig", col("CP_1"))
        .withColumn(
            "L2Dest",
            when(col("Stops") == 1, col("Off")).otherwise(col("CP_2"))
        )

        .withColumn("L3Orig", col("CP_2"))
        .withColumn(
            "L3Dest",
            when(col("Stops") == 2, col("Off")).otherwise(col("CP_3"))
        )

        .withColumn("L4Orig", col("CP_3"))
        .withColumn(
            "L4Dest",
            when(col("Stops") == 3, col("Off")).otherwise(col("CP_4"))
        )

        .withColumn("L5Orig", col("CP_4"))
        .withColumn("L5Dest", col("Off"))
    )

    for leg_no in range(1, 6):
        is_first_truck = (
            col("TruckPosition").isin("front", "both") &
            (lit(leg_no) == lit(1))
        )

        is_last_truck = (
            col("TruckPosition").isin("end", "both") &
            (lit(leg_no) == (col("Stops") + lit(1)))
        )

        c = c.withColumn(
            f"L{leg_no}Mode",
            when(is_first_truck | is_last_truck, lit("T")).otherwise(lit("F"))
        )

    # -------------------------------------------------------------------------
    # 7. Join leg metrics from edge_df
    # -------------------------------------------------------------------------

    def join_leg_metrics(df, leg_no):
        e = edge_df.alias(f"e{leg_no}")
        existing_cols = [col(x) for x in df.columns]

        joined = df.join(
            e,
            (
                (col(f"L{leg_no}Orig") == col(f"e{leg_no}.EdgeOrig")) &
                (col(f"L{leg_no}Dest") == col(f"e{leg_no}.EdgeDest")) &
                (col(f"L{leg_no}Mode") == col(f"e{leg_no}.EdgeMode"))
            ),
            "left",
        )

        return joined.select(
            *existing_cols,
            col(f"e{leg_no}.EdgeDistance").alias(f"L{leg_no}Distance"),
            col(f"e{leg_no}.EdgeFreq").alias(f"L{leg_no}Freq"),
            col(f"e{leg_no}.EdgeWB").alias(f"L{leg_no}WB"),
            col(f"e{leg_no}.EdgeWBAvailable").alias(f"L{leg_no}WBAvailable"),
        )

    for leg_no in range(1, 6):
        c = join_leg_metrics(c, leg_no)

    # Required legs = Stops + 1
    c = c.filter(
        col("L1Distance").isNotNull() &
        col("L2Distance").isNotNull() &
        when(col("Stops") >= 2, col("L3Distance").isNotNull()).otherwise(lit(True)) &
        when(col("Stops") >= 3, col("L4Distance").isNotNull()).otherwise(lit(True)) &
        when(col("Stops") >= 4, col("L5Distance").isNotNull()).otherwise(lit(True))
    )

    # -------------------------------------------------------------------------
    # 8. Compute O-D GCD for candidate O-Ds on driver, then join back
    # -------------------------------------------------------------------------
    # This is only per distinct truck candidate O-D, not per path.

    od_rows = []

    for row in c.select("Brd", "Off").dropDuplicates().toLocalIterator():
        brd = safe_strip(row.Brd)
        off = safe_strip(row.Off)

        if brd in coords and off in coords:
            gcd_value = vincenty_dist(*coords[brd], *coords[off])
        else:
            gcd_value = None

        if gcd_value is not None and gcd_value > 0:
            od_rows.append((brd, off, float(gcd_value)))

    gcd_schema = StructType([
        StructField("Brd", StringType(), False),
        StructField("Off", StringType(), False),
        StructField("GCD", DoubleType(), False),
    ])

    gcd_df = F.broadcast(spark.createDataFrame(od_rows, schema=gcd_schema))

    circ_rows = [
        (
            float(r.StartMile),
            float(r.EndMile),
            float(r.MaxCirc),
        )
        for r in circ_restr
        if r.StartMile is not None and r.EndMile is not None and r.MaxCirc is not None
    ]

    circ_schema = StructType([
        StructField("StartMile", DoubleType(), False),
        StructField("EndMile", DoubleType(), False),
        StructField("MaxCirc", DoubleType(), False),
    ])

    circ_df = F.broadcast(spark.createDataFrame(circ_rows, schema=circ_schema))

    c = c.join(gcd_df, ["Brd", "Off"], "inner")

    c = c.join(
        circ_df,
        (col("GCD") >= col("StartMile")) & (col("GCD") < col("EndMile")),
        "inner",
    )

    # -------------------------------------------------------------------------
    # 9. Compute metrics and apply total circuity restriction
    # -------------------------------------------------------------------------

    path_distance_expr = (
        F.coalesce(col("L1Distance"), lit(0.0)) +
        F.coalesce(col("L2Distance"), lit(0.0)) +
        when(col("Stops") >= 2, F.coalesce(col("L3Distance"), lit(0.0))).otherwise(lit(0.0)) +
        when(col("Stops") >= 3, F.coalesce(col("L4Distance"), lit(0.0))).otherwise(lit(0.0)) +
        when(col("Stops") >= 4, F.coalesce(col("L5Distance"), lit(0.0))).otherwise(lit(0.0))
    )

    total_freq_expr = (
        F.coalesce(col("L1Freq"), lit(0.0)) +
        F.coalesce(col("L2Freq"), lit(0.0)) +
        when(col("Stops") >= 2, F.coalesce(col("L3Freq"), lit(0.0))).otherwise(lit(0.0)) +
        when(col("Stops") >= 3, F.coalesce(col("L4Freq"), lit(0.0))).otherwise(lit(0.0)) +
        when(col("Stops") >= 4, F.coalesce(col("L5Freq"), lit(0.0))).otherwise(lit(0.0))
    )

    total_wb_expr = (
        F.coalesce(col("L1WB"), lit(0.0)) +
        F.coalesce(col("L2WB"), lit(0.0)) +
        when(col("Stops") >= 2, F.coalesce(col("L3WB"), lit(0.0))).otherwise(lit(0.0)) +
        when(col("Stops") >= 3, F.coalesce(col("L4WB"), lit(0.0))).otherwise(lit(0.0)) +
        when(col("Stops") >= 4, F.coalesce(col("L5WB"), lit(0.0))).otherwise(lit(0.0))
    )

    c = (
        c
        .withColumn("PathDistance", path_distance_expr)
        .withColumn("TotFreq", total_freq_expr)
        .withColumn("TotWB", total_wb_expr)
        .withColumn("Circuity", col("PathDistance") / col("GCD"))
        .withColumn("WB_available", when(col("TotWB") > 0.05, lit(1)).otherwise(lit(0)))
        .withColumn("FlightTruck", lit("T"))
        .withColumn("Priority", lit(PR_TRUCK))
        .withColumn("Grade", lit(999))
        .filter(col("Circuity") <= col("MaxCirc"))
    )

    # -------------------------------------------------------------------------
    # 10. Apply old PlusTrucks US-gateway CP filters, NULL-SAFE
    # -------------------------------------------------------------------------
    # IMPORTANT:
    # Do NOT write this as:
    #   ~(((CP_1_Country == 'US') & ...) | ...)
    # without null handling. For Stops 1/2/3, CP_2/CP_3/CP_4 can be NULL,
    # and Spark's three-valued boolean logic will drop those rows.
    # This was why your attempted gateway filter left only Stops = 4.

    station_country_rows = [
        (safe_strip(sta), safe_strip(country))
        for sta, country in station_country.items()
        if safe_strip(sta)
    ]

    station_country_df = (
        spark.createDataFrame(station_country_rows, ["Station", "Country"])
        .dropDuplicates(["Station"])
    )

    station_country_df = F.broadcast(station_country_df)

    for cp_col in ["CP_1", "CP_2", "CP_3", "CP_4"]:
        sc = station_country_df.alias(f"sc_{cp_col}")

        c = (
            c.join(
                sc,
                F.trim(col(cp_col)) == col(f"sc_{cp_col}.Station"),
                "left"
            )
            .withColumnRenamed("Country", f"{cp_col}_Country")
            .drop("Station")
        )

    gateway_list = list(GATEWAYS)

    def is_bad_us_cp(cp_col, country_col):
        cp_value = F.coalesce(F.trim(col(cp_col)), lit(""))
        country_value = F.coalesce(F.trim(col(country_col)), lit(""))

        return (
            (cp_value != lit("")) &
            (country_value == lit("US")) &
            (~cp_value.isin(gateway_list))
        )

    bad_gateway_cp = (
        is_bad_us_cp("CP_1", "CP_1_Country") |
        is_bad_us_cp("CP_2", "CP_2_Country") |
        is_bad_us_cp("CP_3", "CP_3_Country") |
        is_bad_us_cp("CP_4", "CP_4_Country")
    )

    c = (
        c.filter(~bad_gateway_cp)
        .drop(
            "CP_1_Country",
            "CP_2_Country",
            "CP_3_Country",
            "CP_4_Country",
        )
    )

    # Optional debug checkpoint after gateway filtering.
    if DEBUG_TRUCK_AUGMENTATION:
        print("After NULL-safe US gateway CP filter:")
        display(
            c.groupBy("Stops")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct(F.concat_ws("-", col("Brd"), col("Off"))).alias("ods")
            )
            .orderBy("Stops")
        )

    # -------------------------------------------------------------------------
    # 11. Build PathS / Mkt and dedupe against existing non-truck paths
    # -------------------------------------------------------------------------

    c = (
        c
        .withColumn(
            "PathS",
            F.concat(
                F.coalesce(F.trim(col("Brd")), lit("")),
                F.coalesce(F.trim(col("CP_1")), lit("")),
                F.coalesce(F.trim(col("CP_2")), lit("")),
                F.coalesce(F.trim(col("CP_3")), lit("")),
                F.coalesce(F.trim(col("CP_4")), lit("")),
                F.coalesce(F.trim(col("Off")), lit("")),
            )
        )
        .withColumn("Mkt", F.concat(col("Brd"), col("Off")))
    )

    try:
        existing_paths_df = spark.table("NEW_05_AfterAddCPs")
    except Exception:
        existing_paths_df = spark.createDataFrame(pd.DataFrame(all_results))

    for cp_col in ["CP_1", "CP_2", "CP_3", "CP_4"]:
        if cp_col not in existing_paths_df.columns:
            existing_paths_df = existing_paths_df.withColumn(cp_col, lit(None).cast("string"))

    existing_paths_df = (
        existing_paths_df
        .withColumn(
            "PathS",
            F.concat(
                F.coalesce(F.trim(col("Brd")), lit("")),
                F.coalesce(F.trim(col("CP_1")), lit("")),
                F.coalesce(F.trim(col("CP_2")), lit("")),
                F.coalesce(F.trim(col("CP_3")), lit("")),
                F.coalesce(F.trim(col("CP_4")), lit("")),
                F.coalesce(F.trim(col("Off")), lit("")),
            )
        )
        .withColumn("Mkt", F.concat(col("Brd"), col("Off")))
    )

    flight_ref_df = F.broadcast(
        existing_paths_df.select("Mkt", "PathS").dropDuplicates()
    )

    flight_mkt_df = F.broadcast(
        existing_paths_df.select("Mkt").dropDuplicates()
    )

    truck_path_unique = c.join(flight_ref_df, ["Mkt", "PathS"], "left_anti")

    truck_od_dups = truck_path_unique.join(flight_mkt_df, ["Mkt"], "inner")
    truck_od_unique = truck_path_unique.join(flight_mkt_df, ["Mkt"], "left_anti")

    # -------------------------------------------------------------------------
    # 12. Old TruckTop10 / TruckDupTop2 ranking
    # -------------------------------------------------------------------------

    w_truck_old = Window.partitionBy("Brd", "Off").orderBy(
        desc("WB_available"),
        "Stops",
        "Circuity",
        desc("TotWB"),
        desc("TotFreq"),
        "PathS",
    )

    truck_top10 = (
        truck_od_unique
        .withColumn("NumPath", row_number().over(w_truck_old))
        .filter(col("NumPath") <= 10)
    )

    truck_dup_top3 = (
        truck_od_dups
        .withColumn("NumPath", row_number().over(w_truck_old))
        .filter(col("NumPath") <= 3)
    )

    truck_top10.createOrReplaceTempView("NEW_TruckTop10_equivalent")
    truck_dup_top3.createOrReplaceTempView("NEW_TruckDupTop3_equivalent")

    if DEBUG_TRUCK_AUGMENTATION:
        print("NEW_TruckTop10_equivalent")
        display(
            truck_top10.groupBy("FlightTruck", "Stops")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct(F.concat_ws("-", col("Brd"), col("Off"))).alias("ods")
            )
            .orderBy("FlightTruck", "Stops")
        )

        print("NEW_TruckDupTop3_equivalent")
        display(
            truck_dup_top3.groupBy("FlightTruck", "Stops")
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct(F.concat_ws("-", col("Brd"), col("Off"))).alias("ods")
            )
            .orderBy("FlightTruck", "Stops")
        )

    truck_final_df = truck_top10.unionByName(
        truck_dup_top3,
        allowMissingColumns=True
    )

    truck_final_df = truck_final_df.select(
        "Brd",
        "Off",
        "CP_1",
        "CP_2",
        "CP_3",
        "CP_4",
        "Stops",
        "FlightTruck",
        "TotFreq",
        "TotWB",
        "WB_available",
        "Circuity",
        "GCD",
        "PathDistance",
        "Grade",
        "Priority",
    )

    truck_final_df = truck_final_df.persist(StorageLevel.MEMORY_AND_DISK)

    print("Final truck DataFrame:")
    print("Rows:", truck_final_df.count())
    print("ODs:", truck_final_df.select("Brd", "Off").dropDuplicates().count())

    if DEBUG_TRUCK_AUGMENTATION:
        display(
            truck_final_df.groupBy("WB_available", "Stops")
            .count()
            .orderBy("WB_available", "Stops")
        )

    # Collect final truck results only, not raw candidates.
    # This is still a driver-side list because the downstream notebook currently
    # builds results_df from all_results. Keep only the final ranked truck rows.
    truck_augmented_results = [
        row.asDict()
        for row in truck_final_df.toLocalIterator()
    ]

    # Cleanup persisted intermediate DataFrames.
    base_flight_df.unpersist()
    all_truck_candidates.unpersist()
    truck_final_df.unpersist()

all_results.extend(truck_augmented_results)

print(f"Final truck paths added: {len(truck_augmented_results)}")
print(f"Total paths after truck augmentation: {len(all_results)}")

# ==== CELL 23 title='TESTING' ====
# DEBUG
after_truck_snapshot = list(all_results)
NEW_06_AfterTruck = all_results_to_df(
    after_truck_snapshot,
    "NEW_06_AfterTruck"
)

if NEW_05_AfterAddCPs is not None and NEW_06_AfterTruck is not None:
    NEW_06_TruckOnly = NEW_06_AfterTruck.join(
        NEW_05_AfterAddCPs.select("Brd", "Off", "PathPP").dropDuplicates(),
        ["Brd", "Off", "PathPP"],
        "left_anti"
    )
    NEW_06_TruckOnly.createOrReplaceTempView("NEW_06_TruckOnly")
    log_df_stage("NEW_06_TruckOnly", NEW_06_TruckOnly, "Brd", "Off", "PathPP")

# ==== CELL 24 title='' ====
# =============================================================================
# Build results_df
# =============================================================================

if not all_results:
    raise ValueError("No paths generated. Cannot continue.")

# Before results_df creation
for r in all_results:
    if r.get("Priority") == 2:
        r["FlightTruck"] = "F"

results_pdf = pd.DataFrame(all_results)

for c in ["CP_1", "CP_2", "CP_3", "CP_4"]:
    if c not in results_pdf.columns:
        results_pdf[c] = None

results_df = spark.createDataFrame(results_pdf)

results_df = results_df.withColumn(
    "FlightTruck",
    when(col("Priority") == 2, lit("F")).otherwise(col("FlightTruck"))
)

if "Priority" not in results_df.columns:
    results_df = results_df.withColumn("Priority", lit(PR_SCHED))
else:
    results_df = results_df.withColumn("Priority", coalesce(col("Priority"), lit(PR_SCHED)))

results_df = (
    results_df
    .withColumn("TD", F.round(col("PathDistance"), 0).cast(DecimalType(6, 0)))
    .withColumn("TotalDistanceKM", F.round(col("PathDistance") / 0.62137119).cast(DecimalType(8, 0)))
    .withColumn(
        "Path",
        F.expr("""
            CASE WHEN Stops = 0 THEN CONCAT('"', Brd, ',', Off, '"')
                 WHEN Stops = 1 THEN CONCAT('"', Brd, ',', CP_1, ',', Off, '"')
                 WHEN Stops = 2 THEN CONCAT('"', Brd, ',', CP_1, ',', CP_2, ',', Off, '"')
                 WHEN Stops = 3 THEN CONCAT('"', Brd, ',', CP_1, ',', CP_2, ',', CP_3, ',', Off, '"')
                 ELSE CONCAT('"', Brd, ',', CP_1, ',', CP_2, ',', CP_3, ',', CP_4, ',', Off, '"')
            END
        """),
    )
    .withColumn(
        "PathPP",
        F.expr("""
            CASE WHEN Stops = 0 THEN CONCAT(Brd, '-', Off)
                 WHEN Stops = 1 THEN CONCAT(Brd, '-', CP_1, '-', Off)
                 WHEN Stops = 2 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', Off)
                 WHEN Stops = 3 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', Off)
                 ELSE CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', CP_4, '-', Off)
            END
        """),
    )
)

results_df.cache()
results_df.createOrReplaceTempView("AllPaths")

print(f"Total valid paths: {results_df.count()}")


# ==== CELL 25 title='TESTING' ====
# DEBUG
results_df.createOrReplaceTempView("NEW_07_AllPaths")
log_df_stage("NEW_07_AllPaths", results_df, "Brd", "Off", "PathPP")

NEW_07_AllPaths_ByType = results_df.groupBy("FlightTruck", "Stops").count().orderBy("FlightTruck", "Stops")
NEW_07_AllPaths_ByType.createOrReplaceTempView("NEW_07_AllPaths_ByType")
display(NEW_07_AllPaths_ByType)

# ==== CELL 26 title='' ====
display(
    results_df.groupBy("Priority", "FlightTruck", "Stops")
    .count()
    .orderBy("Priority", "FlightTruck", "Stops")
)

# ==== CELL 27 title='' ====
# =============================================================================
# Small and Large shipment path pools
# =============================================================================

# w_small = Window.partitionBy("Brd", "Off").orderBy(
#     "Stops",
#     "Priority",
#     desc("TotFreq"),
#     "Circuity",
#     desc("WB_available"),
#     desc("TotWB"),
#     "Path"
# )

# SmallShip = results_df \
#     .withColumn("_rn", row_number().over(w_small)) \
#     .filter(f"_rn <= {TOP_K_SMALL_FINAL}") \
#     .withColumn("Grade", col("_rn")) \
#     .drop("_rn")

# Fix: Match old's final SmallShipment ranking
w_small = Window.partitionBy("Brd", "Off").orderBy(
    "Stops",
    "FlightTruck",       # NOT "Priority" - old sorts F before T
    desc("TotFreq"),
    "Circuity",
    desc("TotWB"),
    "TD",                # Old uses CircT as tiebreaker (= TD/GCD ≈ Circuity for same GCD)
    "Path",              # Additional deterministic tiebreaker
)

SmallShip = results_df \
    .withColumn("_rn", row_number().over(w_small)) \
    .filter(f"_rn <= {TOP_K_SMALL_FINAL}") \
    .withColumn("Grade", col("_rn")) \
    .drop("_rn")

SmallShip.cache()
SmallShip.createOrReplaceTempView("SmallShipmentPathsFullDetails")

print(f"Small shipment paths: {SmallShip.count()}")

# Large shipment includes WB paths, including truck-assisted WB paths.
# Do not cap here. CombineLogicPP later applies RankNum <= 16.
# w_large = Window.partitionBy("Brd", "Off").orderBy(
#     desc("WB_available"),
#     "Stops",
#     "FlightTruck",
#     "Circuity",
#     desc("TotWB"),
#     desc("TotFreq"),
#     "Path",
# )

# LargeShip = (
#     results_df.filter("WB_available = 1")
#     .withColumn("_rn", row_number().over(w_large))
#     .withColumn("Grade", col("_rn"))
#     .drop("_rn")
# )

# Fix: Match old's Large ranking - WB_available is NOT primary sort
w_large = Window.partitionBy("Brd", "Off").orderBy(
    desc("WB_available"),
    "Stops",
    "FlightTruck",
    "Circuity",
    desc("TotWB"),
    desc("TotFreq"),
    "Path",
)

LargeShip = (
    results_df.filter("WB_available = 1")
    .withColumn("_rn", row_number().over(w_large))
    .withColumn("Grade", col("_rn"))
    .drop("_rn")
)

LargeShip.cache()
LargeShip.createOrReplaceTempView("LargeShipmentPathsFullDetails")

print(f"Small shipment paths: {SmallShip.count()}")
print(f"Large shipment paths: {LargeShip.count()}")

# ==== CELL 28 title='' ====
display(SmallShip.groupBy("FlightTruck", "Priority", "Stops").count().orderBy("FlightTruck", "Priority", "Stops"))
print(SmallShip.select("Brd", "Off").dropDuplicates().count())

# ==== CELL 29 title='TESTING' ====
# DEBUG
SmallShip.createOrReplaceTempView("NEW_08_SmallShipmentPathsFullDetails")
LargeShip.createOrReplaceTempView("NEW_09_LargeShipmentPathsFullDetails")

log_df_stage("NEW_08_SmallShipmentPathsFullDetails", SmallShip, "Brd", "Off", "PathPP")
log_df_stage("NEW_09_LargeShipmentPathsFullDetails", LargeShip, "Brd", "Off", "PathPP")

display(SmallShip.groupBy("FlightTruck", "Stops").count().orderBy("FlightTruck", "Stops"))
display(LargeShip.groupBy("FlightTruck", "Stops").count().orderBy("FlightTruck", "Stops"))

# ==== CELL 30 title='' ====
# =============================================================================
# CombineLogicPP old-style final path interleaving
# =============================================================================

SmS = SmallShip.filter("Stops > 0 AND Stops <= 4").withColumn("Source", lit("SmS"))
NS_S = SmallShip.filter("Stops = 0").withColumn("Source", lit("SmS"))

LgS = LargeShip.filter("Stops > 0 AND Stops <= 4").withColumn("Source", lit("LgS"))
NS_L = LargeShip.filter("Stops = 0").withColumn("Source", lit("LgS"))

# Deterministic nonstop dedupe.
nonstops_raw = NS_S.unionByName(NS_L, allowMissingColumns=True).withColumn("MergePriority", lit(1))

w_nonstop = Window.partitionBy("Brd", "Off").orderBy(
    "Source",
    "Stops",
    "Grade",
    "TD",
    "Path",
)

nonstops = (
    nonstops_raw.withColumn("_rn", row_number().over(w_nonstop))
    .filter("_rn = 1")
    .drop("_rn")
)

CombinedPaths = SmS.unionByName(LgS, allowMissingColumns=True)

w_pos = Window.partitionBy("Brd", "Off").orderBy(
    "Stops",
    "FlightTruck",
    desc("TotFreq"),
    "Circuity",        
    "Source",
    "Path",
)

CombinedPaths = CombinedPaths.withColumn("PosRank", row_number().over(w_pos))

Position2 = CombinedPaths.filter("PosRank = 1").withColumn("MergePriority", lit(2))
Position4 = CombinedPaths.filter("PosRank = 2").withColumn("MergePriority", lit(4))
Position7 = CombinedPaths.filter("PosRank = 3").withColumn("MergePriority", lit(7))
Position8 = CombinedPaths.filter("PosRank = 4").withColumn("MergePriority", lit(8))

w_lg = Window.partitionBy("Brd", "Off").orderBy(
    "Stops",
    "FlightTruck",
    desc("WB_available"),
    "Circuity",
    desc("TotWB"),
    desc("TotFreq"),
    "Path",
)

LgS_ranking = (
    LgS.withColumn("RankNum", row_number().over(w_lg))
    .filter(f"RankNum <= {TOP_K_LARGE_INTERLEAVE}")
    .withColumn(
        "MergePriority",
        F.expr("""
            CASE WHEN RankNum = 1 THEN 3
                 WHEN RankNum = 2 THEN 5
                 WHEN RankNum = 3 THEN 6
                 WHEN RankNum = 4 THEN 9
                 WHEN RankNum = 5 THEN 10
                 WHEN RankNum = 6 THEN 11
                 ELSE 19
            END
        """),
    )
)

combinedfiles_raw = (
    nonstops
    .unionByName(Position2, allowMissingColumns=True)
    .unionByName(Position4, allowMissingColumns=True)
    .unionByName(Position7, allowMissingColumns=True)
    .unionByName(Position8, allowMissingColumns=True)
    .unionByName(LgS_ranking, allowMissingColumns=True)
)

# Deterministic dedupe by path.
w_dedupe = Window.partitionBy("Brd", "Off", "Path").orderBy(
    "MergePriority",
    "Stops",
    "Grade",
    "TD",
    "Source",
    "Path",
)

combinedfiles = (
    combinedfiles_raw.withColumn("_dedupe_rn", row_number().over(w_dedupe))
    .filter("_dedupe_rn = 1")
    .drop("_dedupe_rn")
)

w_combined = Window.partitionBy("Brd", "Off").orderBy(
    "MergePriority",
    "Stops",
    "Grade",
    "TD",
    "Path",
)

CombinedTop10 = (
    combinedfiles.withColumn("CombinedNumPath", row_number().over(w_combined))
    .filter(f"CombinedNumPath <= {COMBINED_TOP_N}")
)

CombinedTop10.cache()
CombinedTop10.createOrReplaceTempView("CombinedTop10")

print(f"Combined candidate paths: {combinedfiles.count()}")
print(f"CombinedTop10/top {COMBINED_TOP_N} paths: {CombinedTop10.count()}")

# ==== CELL 31 title='TESTING' ====
# DEBUG
CombinedTop10.createOrReplaceTempView("NEW_10_CombinedTop10")
log_df_stage("NEW_10_CombinedTop10", CombinedTop10, "Brd", "Off", "PathPP")

display(
    CombinedTop10.groupBy("MergePriority", "Source", "FlightTruck", "Stops")
    .count()
    .orderBy("MergePriority", "Source", "FlightTruck", "Stops")
)

# ==== CELL 32 title='' ====
# =============================================================================
# Project Payload export pool
# =============================================================================

w_pp = Window.partitionBy("Brd", "Off").orderBy(
    "MergePriority",
    "Stops",
    "Grade",
    "TD",
    "Path",
)

ForExportPP = (
    CombinedTop10.drop("Priority")
    .withColumn("Priority", row_number().over(w_pp))
    .filter(f"Priority <= {PP_EXPORT_TOP_N}")
    .select("Brd", "Off", "PathPP", "Priority", "TotalDistanceKM")
)

ForExportPP.cache()
ForExportPP.createOrReplaceTempView("ForExportPP")

print(f"ForExportPP paths: {ForExportPP.count()}")

# ==== CELL 33 title='TESTING' ====
# DEBUG
ForExportPP.createOrReplaceTempView("NEW_11_ForExportPP")
log_df_stage("NEW_11_ForExportPP", ForExportPP, "Brd", "Off", "PathPP")

# ==== CELL 34 title='' ====
# =============================================================================
# Current route overlay
# For exact match to pasted old notebook, INCLUDE_CURRENT_ROUTES = False.
# =============================================================================

CurrRoutes = (
    spark.read.option("header", False)
    .csv(f"{INPATH}/exportrtg.csv")
    .toDF("Brd", "Off", "PriorityCurr", "RTGSTA", "NUMCON", "TotalDistanceKMCurr", "PathPP1")
    .withColumn("PathPP", trim(col("PathPP1")))
    .select("Brd", "Off", "PriorityCurr", "TotalDistanceKMCurr", "PathPP")
    .dropDuplicates(["Brd", "Off", "PathPP", "PriorityCurr", "TotalDistanceKMCurr"])
)

if not INCLUDE_CURRENT_ROUTES:
    CurrRoutes = CurrRoutes.limit(0)

Both = ForExportPP.join(CurrRoutes, ["Brd", "Off", "PathPP"], "inner")
NewOnly = ForExportPP.join(CurrRoutes, ["Brd", "Off", "PathPP"], "left_anti")
CurrOnly = CurrRoutes.join(ForExportPP, ["Brd", "Off", "PathPP"], "left_anti")

active_brd = ForExportPP.select("Brd").distinct()
active_off = ForExportPP.select("Off").distinct()

CurrOnly_valid = (
    CurrOnly.join(active_brd, "Brd", "inner")
    .join(active_off, "Off", "inner")
)

combined = (
    Both.unionByName(NewOnly, allowMissingColumns=True)
    .unionByName(CurrOnly_valid, allowMissingColumns=True)
    .withColumn("Priority", when(col("Priority").isNull(), lit(99)).otherwise(col("Priority")))
)

w_final = Window.partitionBy("Brd", "Off").orderBy(
    "Priority",
    "TotalDistanceKM",
    "PriorityCurr",
    "TotalDistanceKMCurr",
    "PathPP",
)

ForExportPP_final = (
    combined.withColumn("PriorNew", row_number().over(w_final))
    .filter(f"PriorNew <= {FINAL_PRIORITY_TOP_N}")
    .withColumn("Priority", col("PriorNew"))
    .withColumn("TotalDistanceKM", coalesce(col("TotalDistanceKM"), col("TotalDistanceKMCurr")))
    .select("Brd", "Off", "PathPP", "Priority", "TotalDistanceKM")
    .dropDuplicates(["Brd", "Off", "PathPP", "Priority", "TotalDistanceKM"])
)

ForExportPP_final.cache()
ForExportPP_final.createOrReplaceTempView("ForExportPP_final")

final_count = ForExportPP_final.count()
print(f"Final routing guide paths: {final_count}")

# ==== CELL 35 title='' ====
test_markets = [
    ("BOS", "DFW"), ("BOS", "LHR"), ("CLT", "DFW"), ("CLT", "LHR"),
    ("DFW", "LHR"), ("JFK", "LHR"), ("LHR", "DFW"), ("ORD", "DFW"), ("ORD", "LHR")
]

for brd, off in test_markets:
    new_paths = ForExportPP_final.filter(
        (trim(col("Brd")) == brd) & (trim(col("Off")) == off)
    ).orderBy("Priority").collect()
    
    print(f"\n{'='*60}")
    print(f"  {brd} → {off}: {len(new_paths)} paths")
    print(f"{'='*60}")
    for r in new_paths:
        print(f"  Priority {r.Priority}: {r.PathPP} ({r.TotalDistanceKM} km)")

# ==== CELL 36 title='TESTING' ====
from cargoOperations import global_config, LibDef, Conventions

# =============================================================================
# Old-vs-new stage count comparison
# =============================================================================

PDS = global_config.outlib

old_stage_queries = {
    "OLD_01_Schedule": f"""
        SELECT Orig, Dest
        FROM group077_cargo.Schedule
    """,

    "OLD_03_CP_F": f"""
    SELECT Brd,
           Off,
           CASE WHEN Stops = 0 THEN CONCAT(Brd, '-', Off)
                WHEN Stops = 1 THEN CONCAT(Brd, '-', CP_1, '-', Off)
                WHEN Stops = 2 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', Off)
                ELSE PathS
           END AS PathPP
    FROM group077_cargo.CP_F
    """,

    "OLD_08_SmallShipmentPathsFullDetails": f"""
        SELECT Brd, Off,
               CASE WHEN Stops = 0 THEN CONCAT(Brd, '-', Off)
                    WHEN Stops = 1 THEN CONCAT(Brd, '-', CP_1, '-', Off)
                    WHEN Stops = 2 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', Off)
                    WHEN Stops = 3 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', Off)
                    ELSE CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', CP_4, '-', Off)
               END AS PathPP
        FROM group077_cargo.SmallShipmentPathsFullDetails
    """,

    "OLD_09_LargeShipmentPathsFullDetails": f"""
        SELECT Brd, Off,
               CASE WHEN Stops = 0 THEN CONCAT(Brd, '-', Off)
                    WHEN Stops = 1 THEN CONCAT(Brd, '-', CP_1, '-', Off)
                    WHEN Stops = 2 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', Off)
                    WHEN Stops = 3 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', Off)
                    ELSE CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', CP_4, '-', Off)
               END AS PathPP
        FROM group077_cargo.LargeShipmentPathsFullDetails
    """,

    "OLD_10_CombinedTop10": f"""
        SELECT Brd, Off,
               CASE WHEN Stops = 0 THEN CONCAT(Brd, '-', Off)
                    WHEN Stops = 1 THEN CONCAT(Brd, '-', CP_1, '-', Off)
                    WHEN Stops = 2 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', Off)
                    WHEN Stops = 3 THEN CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', Off)
                    ELSE CONCAT(Brd, '-', CP_1, '-', CP_2, '-', CP_3, '-', CP_4, '-', Off)
               END AS PathPP
        FROM group077_cargo.CombinedTop10
    """,

    "OLD_11_ForExportPP": f"""
        SELECT Brd, Off, PathPP
        FROM group077_cargo.AA_Metal_PP
    """,
}

old_count_rows = []

for stage_name, query in old_stage_queries.items():
    try:
        df = spark.sql(query)
        df.createOrReplaceTempView(stage_name)

        row_count = df.count()
        od_count = df.select("Brd", "Off").dropDuplicates().count() if "Brd" in df.columns else df.select("Orig", "Dest").dropDuplicates().count()
        path_count = df.select("PathPP").dropDuplicates().count() if "PathPP" in df.columns else None

        old_count_rows.append({
            "stage": stage_name,
            "rows": row_count,
            "ods": od_count,
            "distinct_paths": path_count,
        })

        print(f"{stage_name}: rows={row_count}, ods={od_count}, distinct_paths={path_count}")

    except Exception as e:
        print(f"Could not load {stage_name}: {e}")

old_counts_df = spark.createDataFrame(pd.DataFrame(old_count_rows))
old_counts_df.createOrReplaceTempView("OLD_debug_counts")
display(old_counts_df)

# ==== CELL 37 title='TESTING' ====
OLD_FINAL_PATH = f"{OUTPATH}/AllRoutes_PPayload_PlusCurrent.csv"

OLD_FINAL = (
    spark.read.option("header", True)
    .csv(OLD_FINAL_PATH)
    .select(
        trim(col("Brd")).alias("Brd"),
        trim(col("Off")).alias("Off"),
        trim(col("PathPP")).alias("PathPP"),
        col("Priority").cast("int").alias("Priority"),
        col("TotalDistanceKM").cast("decimal(8,0)").alias("TotalDistanceKM"),
    )
)

OLD_FINAL.createOrReplaceTempView("OLD_FINAL")
log_df_stage("OLD_FINAL", OLD_FINAL, "Brd", "Off", "PathPP")

# ==== CELL 38 title='TESTING' ====
display(
    spark.sql(f"""
        SELECT 'TruckTop10' AS table_name,
               FlightTruck,
               Stops,
               COUNT(*) AS rows,
               COUNT(DISTINCT CONCAT(Brd, '-', Off)) AS ods
        FROM group077_cargo.TruckTop10
        GROUP BY FlightTruck, Stops

        UNION ALL

        SELECT 'TruckDupTop2' AS table_name,
               FlightTruck,
               Stops,
               COUNT(*) AS rows,
               COUNT(DISTINCT CONCAT(Brd, '-', Off)) AS ods
        FROM group077_cargo.TruckDupTop2
        GROUP BY FlightTruck, Stops

        ORDER BY table_name, FlightTruck, Stops
    """)
)

# ==== CELL 39 title='TESTING' ====
display(
    spark.sql(f"""
        SELECT FlightTruck,
               Stops,
               COUNT(*) AS rows,
               COUNT(DISTINCT CONCAT(Brd, '-', Off)) AS ods
        FROM group077_cargo.CP
        GROUP BY FlightTruck, Stops
        ORDER BY FlightTruck, Stops
    """)
)

# ==== CELL 40 title='' ====
import gc

# Stop holding large Python-side objects after Spark DataFrames are already built.
for obj_name in [
    "strict_flight_results_snapshot",
    "after_wb_relaxed_snapshot",
    "after_addcps_snapshot",
    "after_truck_snapshot",
    "truck_augmented_results",
    "manual_cp_results",
    "wb_relaxed_results",
    "results_pdf",
]:
    if obj_name in globals():
        del globals()[obj_name]

# If you no longer need all_results after results_df / ForExportPP are built, delete it too.
# Only do this after all downstream Spark DataFrames are already created.
if "all_results" in globals():
    del all_results

gc.collect()

# Clear Spark cached data that is no longer needed for diagnostics.
spark.catalog.clearCache()

print("Driver memory cleanup complete.")

# ==== CELL 41 title='TESTING' ====
display(
    results_df.groupBy("FlightTruck", "Stops")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.concat_ws("-", col("Brd"), col("Off"))).alias("ods")
    )
    .orderBy("FlightTruck", "Stops")
)

# ==== CELL 42 title='TESTING' ====
display(
    spark.sql(f"""
        SELECT FlightTruck,
               Priority,
               Stops,
               COUNT(*) AS rows,
               COUNT(DISTINCT CONCAT(Brd, '-', Off)) AS ods
        FROM group077_cargo.SmallShipmentPathsFullDetails
        GROUP BY FlightTruck, Priority, Stops
        ORDER BY FlightTruck, Priority, Stops
    """)
)

# ==== CELL 43 title='TESTING' ====
OLD_FOR_EXPORT_PP_PATH = f"{external_path}/debug_old_for_export_pp_slim"

OLD_FOR_EXPORT_PP_SOURCE = spark.sql(f"""
    SELECT TRIM(Brd) AS Brd,
           TRIM(Off) AS Off,
           TRIM(PathPP) AS PathPP
    FROM group077_cargo.AA_Metal_PP
""").dropDuplicates(["Brd", "Off", "PathPP"])

(
    OLD_FOR_EXPORT_PP_SOURCE
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(OLD_FOR_EXPORT_PP_PATH)
)

OLD_FOR_EXPORT_PP = spark.read.format("delta").load(OLD_FOR_EXPORT_PP_PATH)
OLD_FOR_EXPORT_PP.createOrReplaceTempView("OLD_FOR_EXPORT_PP")

print("Materialized OLD_FOR_EXPORT_PP:", OLD_FOR_EXPORT_PP.count())

# ==== CELL 44 title='TESTING' ====
NEW_FOR_EXPORT_PP_PATH = f"{external_path}/debug_new_for_export_pp_slim"

(
    ForExportPP
    .select(
        trim(col("Brd")).alias("Brd"),
        trim(col("Off")).alias("Off"),
        trim(col("PathPP")).alias("PathPP")
    )
    .dropDuplicates(["Brd", "Off", "PathPP"])
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(NEW_FOR_EXPORT_PP_PATH)
)

NEW_FOR_EXPORT_PP = spark.read.format("delta").load(NEW_FOR_EXPORT_PP_PATH)
NEW_FOR_EXPORT_PP.createOrReplaceTempView("NEW_FOR_EXPORT_PP")

print("Materialized NEW_FOR_EXPORT_PP:", NEW_FOR_EXPORT_PP.count())

# ==== CELL 45 title='' ====
old_ods = OLD_FOR_EXPORT_PP.select("Brd", "Off").dropDuplicates()
new_ods = NEW_FOR_EXPORT_PP.select("Brd", "Off").dropDuplicates()

missing_ods = old_ods.join(new_ods, ["Brd", "Off"], "left_anti")
extra_ods = new_ods.join(old_ods, ["Brd", "Off"], "left_anti")

print("Missing ODs from new:", missing_ods.count())
print("Extra ODs in new:", extra_ods.count())

display(missing_ods.orderBy("Brd", "Off").limit(200))
display(extra_ods.orderBy("Brd", "Off").limit(200))

# ==== CELL 46 title='' ====
# # =============================================================================
# # Write outputs
# # =============================================================================

# ForExportPP_final.write.format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .save(f"{external_path}/routing_guide")

# export_csv(
#     ForExportPP_final.orderBy("Brd", "Off", "Priority"),
#     f"{OUTPATH}/AllRoutes_PPayload_PlusCurrent.csv",
# )

# print("Delta and CSV outputs written.")

# ==== CELL 47 title='' ====
# =============================================================================
# Optional email
# =============================================================================

if SEND_EMAIL:
    send_email_o365(
        from_email="Z2148603.Cargo_RPA_EMail_Account@aa.com",
        to_emails=["mariam.qureshi@aa.com"],
        subject="All options - routing guide",
        body=f"Routing guide output complete. Total records: {final_count}",
        body_type="plain",
        group="group077",
        dataframes_to_csv={
            "New_routing_guide": ForExportPP_final.orderBy("Brd", "Off", "PathPP").toPandas()
        },
    )

# ==== CELL 48 title='' ====
END_TIME = datetime.now()

print(f"START: {START_TIME}")
print(f"END: {END_TIME}")
print(f"DURATION: {END_TIME - START_TIME}")
print(f"FINAL_COUNT: {final_count}")
