# cargo-routing-guide

Databricks/PySpark routing-guide workflow for cargo path generation.

## Main production notebook script

- `notebooks/routing_guide_new.py`

## Run

1. Import `notebooks/routing_guide_new.py` into a Databricks notebook (or run as a Databricks Python notebook script).
2. Ensure required inputs exist in the configured `abfss://` input paths.
3. Install dependencies in the notebook cluster if needed (`vincenty`, `networkx`).
4. Execute all cells in order.

## Outputs

The script writes:

- Delta output: `${external_path}/routing_guide`
- CSV output: `${OUTPATH}/AllRoutes_PPayload_PlusCurrent.csv`

Final CSV columns are:

- `Brd`
- `Off`
- `PathPP`
- `Priority`
- `TotalDistanceKM`
