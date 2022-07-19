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
Arrow started to provide these files in their jar from a certain version onwards. 
The version in the jar `arrow-dataset-6.0.1.jar` is compatible with Ubuntu 21.10. 
If your system is compatible as well, then this should go well automatically. 
If not, you need to retrieve the right `libarrow_dataset_jni.so` (named specifically that)
If your version is not supported, please check:
 - [Arrow-Install](https://arrow.apache.org/install/) to install Arrow manually
 - [Arrow-Build](https://arrow.apache.org/docs/developers/cpp/building.html) to build Arrow manually
 - [Arrow-Repo](https://github.com/apache/arrow.git) for the github repository
 - [Apache-JFrog-Arrow](https://apache.jfrog.io/ui/native/arrow/) to extract the right file (e.g. for centos8: https://apache.jfrog.io/artifactory/arrow/centos/8/x86_64/Packages/arrow-dataset-libs-6.0.1-1.el8.x86_64.rpm)
If you got the right file, you can ship it with the jar by building with `-Darrow.dataset.lib.folder`.
> NOTE: `-Darrow.dataset.lib.folder` should be relative to the subproject directory

You can check that all library-dependencies are resolved using:
```bash
ldd libarrow_dataset_jni.so
```
You can set `LD_LIBRARY_PATH` if your libraries reside at a custom path.

#### Example -- DAS6
> Note: description is not complete

For the DAS6 system, I had to build Arrow myself to generate the right shared library. Steps: 
1. Download arrow from: https://github.com/apache/arrow/tree/release-6.0.1
```bash
git clone https://github.com/apache/arrow.git -b release-6.0.1
```
2. Build
```bash
  cd arrow/cpp
  cmake . -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_JNI=ON
  sudo make install -j8
```
> Note: if you run `make` without `sudo` then make fails at the end. However, it was still able to 
> generate the correct file, so don't worry :)

This will generate `libarrow_dataset_jni.so.600` in `arrow/cpp/build/latest`
Note that we require `libarrow_dataset_jni.so`, so you should rename it

I added the folder (`arrow/cpp/build/latest`) to the jar with the `-Darrow.dataset.lib.folder` option. 
Then, I had to ensure the right libraries were available. For das-6, this was required for: `libparquet.so.600`, `libarrow.so.600` and `libre2.so.0`, `libsnappy.so.1`
For this, I created a local_libs_dir in the home directory, which is available to all nodes. 
In there, I downloaded the required rpms: 
 1. [libparquet.so.600](https://apache.jfrog.io/artifactory/arrow/centos/8/x86_64/Packages/parquet-libs-6.0.1-1.el8.x86_64.rpm)
 2. [libarrow.so.600](https://apache.jfrog.io/artifactory/arrow/centos/8/x86_64/Packages/arrow-libs-6.0.1-1.el8.x86_64.rpm)
 3. [libre2.so.0](https://download-ib01.fedoraproject.org/pub/epel/8/Everything/x86_64/Packages/r/re2-20190801-1.el8.x86_64.rpm)
 4. [libsnappy.so.1](https://dl.rockylinux.org/pub/rocky/8/BaseOS/x86_64/os/Packages/s/snappy-1.1.8-3.el8.x86_64.rpm)
And 'unpacked' them through `rpm2cpio rpms/library.rpm | cpio -idv`.
Finally, I prepended this local_lib_dir to `LD_LIBRARY_PATH`.

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