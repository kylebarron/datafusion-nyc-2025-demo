# datafusion-nyc-2025-demo

Demo for DataFusion NYC Meetup 2025

### Install dependencies

```bash
uv sync
```

### Download NYC Taxi Data

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet
```

If that link is dead, refer to the [NYC Taxi & Limousine Commission (TLC) Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page for updated links, and download data for January 2010.

### Prepare Data

This step is required for now so that we can assign GeoArrow metadata onto the Parquet file. This is mostly a simplification; we could alternative create the geometry columns on the fly in SQL.

```bash
uv run prepare_trip_data.py
```
