# Complete Arrow Spark
> Note: this work is more like a Proof of Concept, and is still a work in progress.

Complete Arrow Spark extends Apache Spark with operations that use Apache Arrow format internally, without any conversions to row-wise format. 

This project consists of two submodules: `arrowspark-core` and `data-generation`

To build an individual project run:
```bash
mvn --projects [module] [command]
```
where [module] is either `arrowspark`, or `data-generation`,
and [command] is the command you would usually use, e.g. `clean package`.

## ArrowSpark - Core
ArrowSpark-Core contains the core functionalities of our extension. 
Its Main class can be used for evaluations.

> Note: currently, we are still in the evaluation-stage, meaning that the main class is still under development
> Note: tests in the test-directory pass, unless we run them through mvn package...

### Dependencies
ArrowSpark needs `libarrow_dataset_jni.so` to successfully read in Parquet files to the in-memory Arrow-format. 
This library can be retrieved after building Apache Arrow. For this, you require: 
`gcc, CMake, make or ninja, at least 1GB RAM`

Then, clone the Arrow repository using:
```bash
git clone https://github.com/apache/arrow.git
```
And navigate to `arrow/cpp`


### Usage
Build the jar with dependencies with
```bash
mvn --projects arrowspark clean compile assembly:single
```
or without dependencies with
```bash
mvn --projects arrowspark package
```
and use the jar as you will. You can find the jar in `target`.

To create both the dependency-jar and the sources-jar, run:
```bash
mvn --projects arrowspark clean compile verify assembly:single
```

For both dependency-jar and normal-jar with skipping tests: 
```bash
mvn -Dmaven.test.skip=true --projects arrowspark clean compile package assembly:single
```
>Note: dependencies jar should be >100 MB
>Note: you can use both the default-jar and dependency-jar to use this project without dependency conflicts, e.g.:
> `java -cp arrowspark-core-1.0-SNAPSHOT.jar:arrowspark-core-1.0-SNAPSHOT-jar-with-dependencies.jar nl.liacs.mijpelaar.Main`


### Export repository to remote
To export the project to a remote machine, you can use the following command, from the directory where the project is cloned
```bash
rsync -az complete-arrow-spark [remote-address]:[path-to-directory-to-place-project-directory]/ --filter=':- .gitignore' --exclude='complete-arrow-spark/.git'
```

### Making a local repository
If you do not want to build a dependency-jar and do not want to deal with dependencies yourself,
you can create a local repository as follows:
```bash 
mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file -Dfile=target/complete-arrow-spark-1.0-SNAPSHOT.jar -DgroupId=nl.liacs.mijpelaar -DartifactId=arrowspark-core -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DlocalRepositoryPath=.
```
Then, you can add it as a local repository
```xml
<repository>
  <id>local-patches</id>
  <url>file://${path.to.complete.arrow.spark}</url>
</repository>
```

```gradle.build
maven {
        url file:// + path_to_complete_arrow_spark
}
```

## DataGeneration
DataGeneration is an util-module to generate the data which we use to evaluate Complete-Arrow-Spark. 

### Usage
Arguments:
1. `-a` or `--amount`: range of integers to generate
2. `-l` or `--local`: whether to run Spark local or not
3. `-p` or `--path`: file-names structure
4. `--spark-local-dir`: a directory to which spark can log temporary files, default: `/tmp/`

>Note: each generated file will be placed in the parent of `path` and have the name `path-basename_X.parquet`,
> where X is the Xth partition. E.g. if path is `nice/directory/file_a` and the amount is 1, then a single
> file is generated in the directory `nice/directory` with the name `file_a_0.parquet`

Build the jar with dependencies with
```bash
mvn --projects data-generation clean compile assembly:single
```
or without dependencies with
```bash
mvn --projects data-generation package
```