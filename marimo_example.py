import marimo

__generated_with = "0.15.4"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    ## `GeoDataFusion` Example

    This is a basic example to use the [DataFusion Python API](https://datafusion.apache.org/python/) with the [GeoDataFusion extension](https://github.com/datafusion-contrib/datafusion-geo) to work with and visualize GeoParquet data.

    This example uses data from the [NYC Taxi & Limousine Commission (TLC) Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import pyarrow as pa
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
def _(mo):
    mo.md(r"""Now we'll create the DataFusion `SessionContext`, its primary API endpoint, and register our spatial extension onto it.""")
    return


@app.cell
def _(SessionContext, register_all):
    ctx = SessionContext()
    register_all(ctx)
    ctx.register_parquet(
        "trips",
        "yellow_tripdata_2010-01_geo.parquet",
        skip_metadata=False,
    )
    return (ctx,)


@app.cell
def _(mo):
    mo.md(
        r"""
    Next we'll initialize a bounding box to be used in a spatial intersection query in DataFusion. 

    This bounding box will be overridden by drawing on the map instance.
    """
    )
    return


@app.cell
def _(mo):
    get_bbox, set_bbox = mo.state([-74.047108,40.684547,-73.898007,40.872499])
    return get_bbox, set_bbox


@app.cell
def _(mo):
    mo.md(r"""Next we define our SQL query, interpolating the bounding box (sorry for the SQL injection!)""")
    return


@app.cell
def _(get_bbox):
    bbox = get_bbox()
    sql = """
        SELECT *
        FROM trips
        WHERE ST_Intersects(pickup, ST_MakeBox2D(
                ST_Point({minx}, {miny}),
                ST_Point({maxx}, {maxy})
            ))
        LIMIT 100000
        """.format(minx=bbox[0], miny=bbox[1], maxx=bbox[2], maxy=bbox[3])
    return (sql,)


@app.cell
def _(ctx, sql):
    df = ctx.sql(sql)
    df
    return (df,)


@app.cell
def _(BrBG_10, apply_continuous_cmap, df, pa):
    table = pa.table(df)
    min_bound = 5
    max_bound = 50
    normalized_total_amount = (table["total_amount"].to_numpy() - min_bound) / (max_bound - min_bound)
    fill_color = apply_continuous_cmap(
        normalized_total_amount,
        BrBG_10,
        alpha=0.7,
    )
    return fill_color, normalized_total_amount, table


@app.cell
def _(ScatterplotLayer, fill_color, normalized_total_amount, table):
    layer = ScatterplotLayer(
        table=table.select(["pickup", "total_amount", "trip_distance"]),
        get_fill_color=fill_color,
        get_radius=normalized_total_amount * 90,
        radius_units="meters",
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


if __name__ == "__main__":
    app.run()
