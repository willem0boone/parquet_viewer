from parquet_viewer import ParquetViewService


DATASET_URL = ("https://s3.waw3-1.cloudferro.com/emodnet/emodnet_biology/12639"
               "/marine_biodiversity_observations_2026-02-26.parquet")

service = ParquetViewService(parquet=DATASET_URL)


service.get_view(
    # columns=["datasetid"],
    filters={"datasetid": 4687},
    max_rows=50
)

