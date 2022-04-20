# Json Schema Generator
## Overview
JSON files can contain multiple schemas, in practice this is often limited due to schema versions, prototyping, etc.
However, it is not unheard of to have API dumps, log outputs, and data lakes with many hundred unique schemas, which in reality have a much smaller schema mapping domain with optional fields.
This project aims to produce an optimal set of schemas which best describe a JSON input such that the number of total schemas is sufficently minimal while maximizing specificity.
## Code
src/main/scala/SparkMain.scala contains our main entry point as well as optional field computations and schema output logic. \
src/main/scala/BiMax/OurBiMax.scala contains our bimax-merge clustering algorithm. \
src/main/scala/Optimizer/RewriteAttributes.scala performs the key-space entropy AST detection and rewrite.
## Parameters
Our program expects the first mandatory argument to be the file to generate a schema for. The follow are options \
&nbsp;kse: double: key space entropy threshold, a path with a keyspace greater than this threshold will be marked as a collection. \
&nbsp;entroy: true|false: whether use the above mentioned entropy heuristic. \ 
&nbsp;schema: true|false: whether to write the json-schema for the output to the log file. \ 
&nbsp;log: string: name of file to output schema and log information to. \ 
&nbsp;train: double: percentage of data to be used in training. \ 
&nbsp;val: integer: number of rows to be reserved for validation, overrides train if greater than remaining. \
example parameters: /data/my_data.json kse 1.0 train 30.0 val 1000 log ~/Desktop/my_data.log