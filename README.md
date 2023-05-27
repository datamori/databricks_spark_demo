# Databricks/Spark JSON parsing demo

## Background
Someone I know (outside of my work), asked me for a technical advise for Spark processing.

The question was,

For a public dataset extracted from a public website which is GUI rendered data in JSON with nested multi level hierarchy structure (simply no-sql dataset in JSON), is it possible to flatten the JSON document and ingest it to DB table to run traditional SQL queries?

Essentially, the dataset represents a NoSQL dataset in JSON format, which appears to have been transformed from a tabular dataset for web page visualization. The question at hand was whether it is possible to ingest this JSON document into a database table to enable the execution of traditional SQL queries. 
In response, I affirmed that it is indeed possible to achieve this transformation using Spark, as Spark is capable of performing any data transformation. However, the key determinant of a successful solution lies in selecting an appropriate method for iterating, parsing, and flattening the multi-level nested structure of the JSON document within the Spark framework.

This Repo shares a quick but very optimal & effective my way of Spark implementation does the job.
