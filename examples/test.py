import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow import fs

endpoint = "https://s3.waw3-1.cloudferro.com"

s3 = fs.S3FileSystem(
    endpoint_override=endpoint,
    anonymous=True
)

path = "emodnet/emodnet_biology/12639/marine_biodiversity_observations_2026-02-26.parquet"

pf = pq.ParquetFile(path, filesystem=s3)

rows_needed = 50
batches = []
rows_collected = 0

for batch in pf.iter_batches(batch_size=50):
    batches.append(batch)
    rows_collected += batch.num_rows

    if rows_collected >= rows_needed:
        break

table = pa.Table.from_batches(batches)
table = table.slice(0, rows_needed)

df = table.to_pandas()

print(df)
