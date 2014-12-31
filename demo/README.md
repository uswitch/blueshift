# Blueshift demo

This demo program will periodically upload new `tsv`-files to a S3
bucket for Blueshift to upload. The program will ensure that a
`manifest.edn` file is present and that a `demo` table is present in
Redshift.

The `demo` table contains three columns:

- `uuid` A unique identifer for the row
- `key` The name of the `tsv`-file corresponsing to the row
- `timestamp` A timestamp for when the `tsv`-file was created.

There's only one line in each `tsv`-file even though Blueshift support
multiple lines when loading files into tables.

The demo will periodically monitor the S3 bucket to listen for file
changes, and it will monitor the `demo` table in Redshift to look for
new timestamps.

## Usage

The demo is configured using the same configuration file as
Blueshift. The demo also needs to know the JDBC-URL for Redshift. The
URL should be stored in a `txt`-file.

There is a `Procfile` that assumes `../etc/config.edn` exists, and that
there is a file named `./jdbc-url.txt` with content on the form
`jdbc:postgresql://...`. The Procfile will start the demo and a
Blueshift process when run using:

    foreman start
