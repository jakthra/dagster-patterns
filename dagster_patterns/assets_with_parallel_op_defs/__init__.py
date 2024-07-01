from datetime import datetime
from time import sleep
from typing import List
from dagster import (
    AssetSelection,
    Config,
    Definitions,
    DynamicOut,
    DynamicOutput,
    asset,
    define_asset_job,
    graph_asset,
    op,
)


@op
def process_file(file_path: str):
    sleep(5)
    print(f"Parallel processing {file_path} {datetime.now()}")


class FilesConfig(Config):
    files: List[str]


@asset()
def get_files(config: FilesConfig):
    return config.files


@op(out=DynamicOut())
def load_files(files: List[str]):
    for idx, file in enumerate(files):
        yield DynamicOutput(file, mapping_key=str(idx))


@graph_asset()
def process_files_asset(get_files: List[str]):
    files = load_files(get_files)
    results = files.map(process_file)
    results.collect()
    return results


defs = Definitions(
    assets=[get_files, process_files_asset],
    jobs=[
        define_asset_job(name="parallel_process_files", selection=AssetSelection.all())
    ],
)
