# The PyIceberg and DuckDB client image of the Iceberg and Metadata Search system tests.
#
# esti/iceberg_utils.go runs every client step in docker (icebergEnv.runClient) and, when the image
# does not already provide the clients, installs icebergClientPackages at the start of the step.
# On a bare python:3.12-slim that install is pulled over the network on every step; this image bakes
# the same packages in, once, so a run is hermetic and a step does not spend its wall time on pip.
# TestMetadataSearchEndToEnd hit its 10m bound in exactly that install.
#
# The image is built and used locally only - never pushed to any registry:
#
#   docker build -t esti-iceberg-client esti/ops/iceberg-client
#   export ESTI_ICEBERG_CLIENT_IMAGE=esti-iceberg-client
#
# The package set below is icebergClientPackages of esti/iceberg_utils.go, character for character,
# and must stay in step with it: a step still installs that set when the import probe fails, and the
# versions are the ones the suite is verified against (PyIceberg 0.12 turned inspect.manifests()
# into a PyArrow Table, so the shape of what a step reports depends on the pin).
FROM python:3.12-slim

# --no-cache-dir keeps the image small; the profile extras are the ones the steps need:
# sql-sqlite is the SqlCatalog the fixture writer uses (no catalog server is involved in writing
# the fixture files), s3fs is the FileIO of an s3:// warehouse, adlfs is the FileIO of the
# abfs[s]:// locations of the Azure Metadata Search acceptance test (mds_azure_test.go), and
# pyarrow is what both the writer and the PyIceberg reader use for data.
RUN pip install --quiet --disable-pip-version-check --no-cache-dir \
        "pyiceberg[sql-sqlite,s3fs,pyarrow]==0.12.0" \
        "pyarrow==26.0.1" \
        "duckdb==2.0.0" \
        "adlfs==2025.8.0"

# The same probe icebergClientCommand runs before it decides to install: fail the build, not the
# test run, when the image does not provide the clients.
RUN python -c 'import pyiceberg, pyarrow, duckdb, adlfs'
