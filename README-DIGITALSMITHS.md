# Digitalsmiths Specific Information #

## Build Instructions ##

Create a ~/.gradle/gradle.properties file containing:

```
mavenRepo=http://maven-repo-url
mavenUrl=file:/tmp/gradleRepo/releases
mavenSnapshotUrl=file:/tmp/gradleRepo/snapshots
mavenUsername=dummyName
mavenPassword=dummyPassword
```

These variables must be set before any gradle tasks can be run.  These variables are only actually used when running the
uploadArchives task which is used to upload all the aritfcats to a "remote" repository (this is done typically by 
Jenkins). The uploadArchives task can be tested locally by setting the URL variables to a valid location.  For local 
testing the user name and password are not important.

To build a tar file:
```
./gradlew clean tar
```

To install all the artifacts to the local Maven repository:
```
./gradlew clean install
```

To install all the artifacts to the "remote" Maven repository:
```
./gradlew clean uploadArchives
```

## Release Build ##

There is currently no equivalent of the maven-release-plugin built into the gradle script.  To create a release
build remove the -SNAPSHOT suffix from the curr.release variable in the local gradle.properties file.
```
./gradle.properties

curr.release=1.9.17-digitalsmiths-4-SNAPSHOT
```

Once the build completes increment the Digitalsmiths build number and add back the -SNAPSHOT suffix.

## Run Voldemort ##

To start a single Voldemort node using the default RocksDB settings:

```
bin/voldemort-server.sh config/ffs_single_node_cluster
```
## Run The RocksDB Unit Tests ##

```
./gradlew -Dtest.single=voldemort/store/rocksdb/RocksdbStorageEngineTest cleanTest test
```
