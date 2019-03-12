# Json Explorer: Explore schemas for the messiest of JSON datasets
## How to run
- Can be built with sbt by calling:
    1. sbt compile
    2. sbt run

- run our jar
    - java -jar JsonExplorer.jar [path-to-json-file] [-master local[*]] [-name JsonExplorer] [-spark.driver.maxResultSize 4g] [-spark.driver.memory 4g] [-spark.executor.memory 4g]
    
## Command line arguments: flag | default | description

-name JsonExplorer [name passed to Spark and used for output file names]

-master local[*] [pass master url to Spark]

-spark.*** none [optional parameters to pass to Spark Session: https://spark.apache.org/docs/latest/configuration.html]

-memory None [spark persist strategy for shreded rows: blank=none, disk, inmemory]

-log false [append logged statistics to log.json file]

-testmode false [puts into test mode, runs naive implementations and tests]

-test 100.0 [% of rows to test on, double]

-val 0 [set aside this many rows for validation, overrides test percentage if too large]

-k 3 [set k value for naive implementation]

-viz false [output visualization like box plot]

-dot false [output dot files]