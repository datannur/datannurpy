# Remote storage

## Remote storage


Scan files on SFTP servers or cloud storage (S3, Azure, GCS):

```yaml
env_file: .env               # SFTP_PASSWORD, AWS_KEY, AWS_SECRET, etc.

add:
  # SFTP (paramiko included by default)
  - folder: sftp://user@host/path/to/data
    storage_options:
      password: ${SFTP_PASSWORD}   # or key_filename: /path/to/key

  # Amazon S3 (requires: pip install datannurpy[s3])
  - folder: s3://my-bucket/data
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}

  # Azure Blob (requires: pip install datannurpy[azure])
  - folder: az://container/data
    storage_options:
      account_name: ${AZURE_ACCOUNT}
      account_key: ${AZURE_KEY}

  # Google Cloud Storage (requires: pip install datannurpy[gcs])
  - folder: gs://my-bucket/data
    storage_options:
      token: /path/to/credentials.json

  # Single remote file
  - dataset: s3://my-bucket/data/sales.parquet
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}

  # Remote SQLite / GeoPackage database
  - database: sftp://host/path/to/db.sqlite
    storage_options:
      key_filename: /path/to/key
  - database: s3://bucket/geodata.gpkg
    storage_options:
      key: ${AWS_KEY}
      secret: ${AWS_SECRET}
```

The `storage_options` dict is passed directly to [fsspec](https://filesystem-spec.readthedocs.io/). See provider documentation for available options:

- [SFTP](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.implementations.sftp.SFTPFileSystem)
- [S3](https://s3fs.readthedocs.io/en/latest/)
- [Azure](https://github.com/fsspec/adlfs)
- [GCS](https://gcsfs.readthedocs.io/en/latest/)
