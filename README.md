# Blueshift

Service to watch Amazon S3 and automate the load into Amazon Redshift.

![Gravitational Blueshift](http://upload.wikimedia.org/wikipedia/commons/5/5c/Gravitional_well.jpg) ([Image used under CC Attribution Share-Alike License](http://en.wikipedia.org/wiki/File:Gravitional_well.jpg)).

## Rationale

[Amazon Redshift](https://aws.amazon.com/redshift/) is a "a fast, fully managed, petabyte-scale data warehouse service" but importing data into it can be a bit tricky: e.g. if you want upsert behaviour you have to [implement it yourself with temporary tables](http://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html), and we've had problems [importing across machines into the same tables](https://forums.aws.amazon.com/message.jspa?messageID=443795). Redshift also performs best when bulk importing lots of large files from S3.

[Blueshift](https://github.com/uswitch/blueshift) is a little service(tm) that makes it easy to automate the loading of data from Amazon S3 and into Amazon Redshift. It will periodically check for data files within a designated bucket and, when new files are found, import them. It provides upsert behaviour by default. 

Importing to Redshift now requires just the ability to write files to S3.

## Using

### Configuring

Blueshift requires minimal configuration. It will only monitor a single S3 bucket currently, so the configuration file (ordinarily stored in `./etc/config.edn`) looks like this:

```clojure
{:s3 {:credentials   {:access-key ""
                      :secret-key ""}
      :bucket        "blueshift-data"
      :poll-interval {:seconds 30}}}
```

The S3 credentials are shared by Blueshift for watching for new files and for [Redshift's `COPY` command](http://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html).

### Building & Running

The application is written in Clojure, to build the project you'll need to use [Leiningen](https://github.com/technomancy/leiningen).

If you want to run the application on your computer you can run it directly with Leiningen (providing the path to your configuration file)

    $ lein run -- --config ./etc/config.edn

Alternatively, you can build an Uberjar that you can run:

    $ lein uberjar
    $ java -Dlogback.configurationFile=./etc/logback.xml -jar target/blueshift-0.1.0-standalone.jar --config ./etc/config.edn

The uberjar includes [Logback](http://logback.qos.ch/) for logging. `./etc/logback.xml.example` provides a simple starter configuration file with a console appender. 

## Using

Once the service is running you can create any number of directories in the S3 bucket. These will be periodically checked for files and, if found, an import triggered. If you wish the contents of the directory to be imported it's necessary for it to contain a file called `manifest.edn` which is used by Blueshift to know which Redshift cluster to import to and how to interpret the data files.

Your S3 structure could look like this:

      bucket
      ├── directory-a
      │   └── foo
      │       └── manifest.edn
      │       └── 0001.tsv
      │       └── 0002.tsv
      └── directory-b

and the `manifest.edn` could look like this:

    {:table        "testing"
     :pk-columns   ["foo"]
     :columns      ["foo" "bar"]
     :jdbc-url     "jdbc:postgresql://foo.eu-west-1.redshift.amazonaws.com:5439/db?tcpKeepAlive=true&user=user&password=pwd"
     :options      ["DELIMITER '\\t'" "IGNOREHEADER 1" "ESCAPE" "TRIMBLANKS"]
     :data-pattern ".*tsv$"}

When a manifest and data files are found an import is triggered. Once the import has been successfully committed Blueshift will **delete** any data files that were imported; the manifest remains ready for new data files to be  imported.

It's important that `:columns` lists all the columns (and only the columns) included within the data file and that they are in the same order. `:pk-columns` must contain a uniquely identifying primary key to ensure the correct upsert behaviour. `:options` can be used to override the Redshift copy options used during the load.

Blueshift creates a temporary Amazon Redshift Copy manifest that lists all the data files found as mandatory for importing, this also makes it very efficient when loading lots of files into a highly distributed cluster.

## Authors

* [Paul Ingles](https://github.com/pingles)
* [Thomas Kristensen](https://github.com/tgk)

## License

Copyright © 2014 [uSwitch.com](http://www.uswitch.com) Limited.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

