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

### Building

The application is written in Clojure, to build the project you'll need to use [Leiningen](https://github.com/technomancy/leiningen).

If you want to run the application on your computer you can run it directly with Leiningen (providing the path to your configuration file)

    $ lein run -- --config ./etc/config.edn

Alternatively, you can build an Uberjar that you can run:

    $ lein uberjar
    $ java -Dlogback.configurationFile=./etc/logback.xml -jar target/blueshift-0.1.0-standalone.jar --config ./etc/config.edn

The uberjar includes [Logback](http://logback.qos.ch/) for logging. `./etc/logback.xml.example` provides a simple starter configuration file with a console appender. 

## License

Copyright © 2014 uSwitch Limited.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

## Authors

* [Paul Ingles](https://github.com/pingles)
* [Thomas Kristensen](https://github.com/tgk)
