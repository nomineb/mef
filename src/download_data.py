from typing import Optional
import datetime
import gzip
import os
import requests
import shutil
import tarfile
import zipfile

from filepaths import downloads_folder, data_folder
from logging_util import get_logger

logger = get_logger(__name__)


def download_helper(
    input_url: str,
    download_path: str,
    output_path: Optional[str] = None,
    requires_unzip: bool = False,
    requires_untar: bool = False,
    requires_gzip: bool = False,
    should_clean: bool = False,
    chunk_size: int = 1024,
) -> bool:
    """
    Downloads a file or archive and optionally unzips/untars/copies it to a destination.

    Inputs:
        `input_url`: Where to download data from.
        `download_path`: An absolute filepath to download to.
        `output_path`: The final destination where the downloaded data should end up.
        `requires_unzip`: Should we unzip the file after downloading?
        `requires_untar`: Should we untar the file after downloading?
        `requires_gzip`: Should we un-gzip the file after downloading?
        `should_clean`: Should we delete the temporary downloaded file when finished?
        `chunk_size`: The chunk size for downloading.

    Returns:
        (bool) Whether the file was downloaded (it might be skipped if found).
    """
    # If the file already exists, do not re-download it.
    final_destination = output_path if output_path is not None else download_path
    if os.path.exists(final_destination):
        logger.info(f"{final_destination.split('/')[-1]} already downloaded, skipping.")
        return False

    # Otherwise, download to the file in chunks.
    logger.info(f"Downloading {final_destination.split('/')[-1]}")
    r = requests.get(input_url, stream=True)
    with open(download_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
    # Optionally unzip the downloaded file.
    if requires_unzip:
        if output_path is None:
            raise ValueError("Unzipping requires an output_path destination.")
        with zipfile.ZipFile(download_path, "r") as zip_to_unzip:
            zip_to_unzip.extractall(output_path)
    # Optionally un-tar the downloaded file.
    elif requires_untar:
        if output_path is None:
            raise ValueError("Extracting a tar requires an output_path destination.")
        with tarfile.open(download_path) as tar:
            tar.extractall(output_path)
    elif requires_gzip:
        if output_path is None:
            raise ValueError("Extracting a gzip requires an output_path destination.")
        with gzip.open(download_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    # If the user didn't ask for unzip/untar, but specified a different output_path,
    # copy the downloaded file to there.
    elif output_path is not None and output_path != download_path:
        shutil.copy(download_path, output_path)
    # Finally, optionally clean up the downloaded temporary file.
    if should_clean and output_path != download_path:
        os.remove(download_path)
    return True


def download_pudl_data(source: str = "aws"):
    """
    Downloads the pudl database. OGE currently supports two sources: zenodo and aws
    (i.e. nightly builds). For more information about data sources see:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#data-access

    Zenodo provides stable, versioned data based on the output of the `main` branch of
    pudl but is updated less freqently.
    The most recent version can be found at:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#zenodo-archives

    As of 12/2/2023, the most recent zenodo data was PUDL Data Release v2022.11.30.

    The `aws` source downloads data from the Catalyst's AWS Open Data Registry. This
    data is updated nightly based on the most recent `dev` branch of pudl so is less
    stable.

    Inputs:
        `source`: where to download pudl from, either "aws" or "zenodo"
    """
    os.makedirs(downloads_folder(""), exist_ok=True)

    if source == "aws":
        # define the urls
        pudl_db_url = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/pudl.sqlite.zip"
        epacems_parquet_url = "https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/v2023.12.01/hourly_emissions_epacems.parquet"

        # download the pudl sqlite database
        if not os.path.exists(downloads_folder("pudl.sqlite")):
            output_filepath = downloads_folder("pudl.sqlite")
            download_helper(
                pudl_db_url,
                download_path=output_filepath + ".zip",
                output_path=output_filepath,
                requires_unzip=True,
                should_clean=True,
            )

            # add a version file
            with open(downloads_folder("pudl_sqlite_version.txt"), "w+") as v:
                v.write(f"{datetime.date.today()}")
        else:
            with open(downloads_folder("pudl_sqlite_version.txt"), "r") as f:
                existing_version = f.readlines()[0].replace("\n", "")
            logger.info(
                "Using nightly build version of PUDL sqlite database downloaded"
            )

        if not os.path.exists(
            downloads_folder("hourly_emissions_epacems.parquet")
        ):
            # download the epacems parquet
            logger.info("Downloading parquet file")
            output_filepath = downloads_folder("hourly_emissions_epacems.parquet")
            download_helper(
                epacems_parquet_url,
                download_path=output_filepath,
            )

            # add a version file
            with open(downloads_folder("epacems_parquet_version.txt"), "w+") as v:
                v.write(f"{datetime.date.today()}")

        else:
            with open(downloads_folder("epacems_parquet_version.txt"), "r") as f:
                existing_version = f.readlines()[0].replace("\n", "")
            logger.info(
                "Using nightly build version of PUDL epacems parquet file downloaded"
            )
    elif source == "zenodo":
        # NOTE: This is the most recent available version as of 12/2/2023
        zenodo_url = "https://zenodo.org/record/7472137/files/pudl-v2022.11.30.tgz"

        # get the version number
        pudl_version = zenodo_url.split("/")[-1].replace(".tgz", "")

        # if the pudl data already exists, do not re-download
        if os.path.exists(downloads_folder("pudl_zenodo")):
            pudl_version_file = downloads_folder("pudl_zenodo/pudl_version.txt")
            with open(pudl_version_file, "r") as f:
                existing_version = f.readlines()[0].replace("\n", "")
            if pudl_version == existing_version:
                logger.info("Most recent PUDL Zenodo archive already downloaded.")
                return
            else:
                logger.info("Downloading new version of pudl")
                shutil.rmtree(downloads_folder("pudl_zenodo"))

        download_pudl_from_zenodo(zenodo_url, pudl_version)
    else:
        raise ValueError(
            f"{source} is an invalid option for `source`. Must be 'aws' \
                         or 'zenodo'."
        )


def download_pudl_from_zenodo(zenodo_url, pudl_version):
    r = requests.get(zenodo_url, params={"download": "1"}, stream=True)
    # specify parameters for progress bar
    total_size_in_bytes = int(r.headers.get("content-length", 0))
    block_size = 1024 * 1024 * 10  # 10 MB
    downloaded = 0
    logger.info("Downloading PUDL data...")
    with open(downloads_folder("pudl.tgz"), "wb") as fd:
        for chunk in r.iter_content(chunk_size=block_size):
            print(
                f"Progress: {(round(downloaded/total_size_in_bytes*100,2))}%   \r",
                end="",
            )
            fd.write(chunk)
            downloaded += block_size
        print("Progress: 100.0%")

    # extract the tgz file
    logger.info("Extracting PUDL data...")
    with tarfile.open(downloads_folder("pudl.tgz")) as tar:
        tar.extractall(data_folder())

    # rename the extracted directory to pudl_zenodo
    os.rename(data_folder(pudl_version), downloads_folder("pudl_zenodo"))

    # add a version file
    with open(downloads_folder("pudl_zenodo/pudl_version.txt"), "w+") as v:
        v.write(pudl_version)

    # delete the downloaded tgz file
    os.remove(downloads_folder("pudl.tgz"))


def download_egrid_files():
    """
    Downloads the egrid excel files from 2018-2022.
    """
    os.makedirs(downloads_folder("egrid"), exist_ok=True)

    # the 2018 and 2019 data are on a different directory than the newer files.
    egrid_url = "https://www.epa.gov/system/files/documents/2024-01/egrid2022_data.xlsx"
    filepath = downloads_folder("egrid/egrid2022_data.xlsx")
    download_helper(egrid_url, filepath)
