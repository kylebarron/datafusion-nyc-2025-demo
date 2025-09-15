import pyarrow as pa
import pyarrow.parquet as pq
from arro3.core import ChunkedArray, Table
from geoarrow.rust.core import points

table = Table.from_arrow(pq.read_table("yellow_tripdata_2010-01.parquet"))

pickup_chunks = []
dropoff_chunks = []
for chunk_idx in range(len(table["pickup_longitude"].chunks)):
    longitude_chunk = table["pickup_longitude"].chunks[chunk_idx].to_numpy()
    latitude_chunk = table["pickup_latitude"].chunks[chunk_idx].to_numpy()
    pickup = points([longitude_chunk, latitude_chunk], crs="EPSG:4326")
    pickup_chunks.append(pickup)

    longitude_chunk = table["dropoff_longitude"].chunks[chunk_idx].to_numpy()
    latitude_chunk = table["dropoff_latitude"].chunks[chunk_idx].to_numpy()
    dropoff = points([longitude_chunk, latitude_chunk], crs="EPSG:4326")
    dropoff_chunks.append(dropoff)


pickup_col = ChunkedArray(pickup_chunks, type=pickup_chunks[0].type)
dropoff_col = ChunkedArray(dropoff_chunks, type=dropoff_chunks[0].type)

table_with_geometries = table.append_column(
    "pickup",
    pickup_col,
).append_column(
    "dropoff",
    dropoff_col,
)
pq.write_table(pa.table(table_with_geometries), "yellow_tripdata_2010-01_geo.parquet")


assert (
    pq.read_schema("yellow_tripdata_2010-01_geo.parquet")
    .field("pickup")
    .metadata[b"ARROW:extension:name"]
    == b"geoarrow.point"
), "Should have geoarrow.point type"
