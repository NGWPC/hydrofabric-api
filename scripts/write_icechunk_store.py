"""Writing a file to an icechunk store. See here: https://icechunk.io/en/latest/icechunk-python/xarray/#write-xarray-data-to-icechunk for docs"""
from pathlib import Path

import rioxarray as rxr
import icechunk
from icechunk.xarray import to_icechunk

def write_tif_to_store(
    file_path: Path,
    bucket: str,
    prefix: str,
    commit: str,
    from_env: bool = True,
):
    if file_path.exists() is False:
        raise FileNotFoundError(f"Cannot find: {file_path}")
    ds = rxr.open_rasterio(file_path)
    
    storage_config = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region="us-east-1",
        from_env=True
    )
    repo = icechunk.Repository.create(storage_config)
    session = repo.writable_session("main")
    to_icechunk(ds, session)
    first_snapshot = session.commit(commit)
    print(f"Data is uploaded. Commit: {first_snapshot}")

if __name__ == "__main__":
    file_path = Path("/Users/taddbindas/data/ngwpc/tbdem_conus_atlantic_gulf_30m.tif")
    write_tif_to_store(
        file_path=file_path,
        bucket="hydrofabric-data",
        prefix="surface/nws-topobathy/tbdem_conus_atlantic_gulf_30m",
        commit="initial topobathy commit"
    )
    