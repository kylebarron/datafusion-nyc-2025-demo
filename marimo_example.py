import marimo

__generated_with = "0.15.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import numpy as np
    import pyarrow as pa
    from arro3.core import Table
    from datafusion import SessionContext
    from geodatafusion import register_all
    from lonboard import Map, ScatterplotLayer, viz
    from lonboard.colormap import apply_continuous_cmap
    from palettable.colorbrewer.diverging import BrBG_10
    return (
        BrBG_10,
        Map,
        ScatterplotLayer,
        SessionContext,
        apply_continuous_cmap,
        mo,
        pa,
        register_all,
    )


@app.cell
def _(SessionContext, register_all):
    def create_context():
        ctx = SessionContext()
        register_all(ctx)
        ctx.register_parquet(
            "trips",
            "yellow_tripdata_2010-01_geo.parquet",
            skip_metadata=False,
        )
        return ctx

    ctx = create_context()
    return (ctx,)


@app.cell
def _():
    return


@app.cell
def _(mo):
    get_bbox, set_bbox = mo.state([-74.047108,40.684547,-73.898007,40.872499])
    return get_bbox, set_bbox


@app.cell
def _(get_bbox):
    bbox = get_bbox()
    sql2 = """
        SELECT pickup, total_amount, trip_distance
        FROM trips
        WHERE ST_Intersects(pickup, ST_MakeBox2D(
                ST_Point({minx}, {miny}),
                ST_Point({maxx}, {maxy})
            ))
        LIMIT 100000
        """.format(minx=bbox[0], miny=bbox[1], maxx=bbox[2], maxy=bbox[3])
    return (sql2,)


@app.cell
def _(ctx, sql2):
    df = ctx.sql(sql2)
    df
    return (df,)


@app.cell
def _(BrBG_10, apply_continuous_cmap, df, pa):
    table = pa.table(df)
    min_bound = 5
    max_bound = 50
    total_amount = table["total_amount"].to_numpy()
    normalized_total_amount = (total_amount - min_bound) / (max_bound - min_bound)
    fill_color = apply_continuous_cmap(
        normalized_total_amount,
        BrBG_10,
        alpha=0.7,
    )
    return fill_color, normalized_total_amount, table


@app.cell
def _(ScatterplotLayer, fill_color, normalized_total_amount, table):
    layer = ScatterplotLayer(
        table=table,
        get_fill_color=fill_color,
        get_radius=normalized_total_amount * 90,
        radius_units = "meters",
        radius_min_pixels=0.1
    )
    return (layer,)


@app.cell
def _(Map, layer, set_bbox):
    m = Map(layer)

    def on_bbox_change(change):
        set_bbox(change["new"])

    m.observe(on_bbox_change, names="selected_bounds")

    m
    return


@app.cell
def _(get_bbox):
    print(get_bbox())
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
