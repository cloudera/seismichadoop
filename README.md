# Seismic Hadoop

## Introduction

Seismic Hadoop combines [Seismic Unix](http://www.cwp.mines.edu/cwpcodes/) with [Cloudera's Distribution including Apache Hadoop](http://www.cloudera.com/hadoop/)
to make it easy to execute common seismic data processing tasks on a Hadoop cluster.

## Build and Installation

You will need to install Seismic Unix on both your client machine and the servers in your Hadoop cluster.

In order to create the jar file that coordinates job execution, simply run `mvn package`.

This will create a `seismic-0.1.0-job.jar` file in the `target/` directory, which includes all of the necessary
dependencies for running a Seismic Unix job on a Hadoop cluster.

## Running Seismic Hadoop

The `suhdp` script in the `bin/` directory may be used as a shortcut for running the following commands. It requires that
the `HADOOP_HOME` environment variable is set on the client machine.

### Writing SEG-Y or SU data files to the Hadoop Cluster

The `load` command to suhdp will take SEG-Y or SU formatted files on the local machine, format them for use with Hadoop,
and copy them to the Hadoop cluster.

	suhdp load -input <local SEG-Y/SU files> -output <HDFS target> [-cwproot <path>]

The `cwproot` argument only needs to be specified if the CWPROOT environment variable is not set on the client machine.
Seismic Hadoop will use the `segyread` command to parse a local file unless it ends with ".su".

### Reading SU data files from the Hadoop Cluster

The `unload` command will read Hadoop-formatted data files from the Hadoop cluster and write them to the local machine.

	suhdp unload -input <SU file/directory of files on HDFS> -output <local file to write>

### Running SU Commands on data in the Hadoop cluster

The `run` command will execute a series of Seismic Unix commands on data stored in HDFS by converting the commands
to a series of MapReduce jobs.

	suhdp run -command "seismic | unix | commands" -input <HDFS input path> -output <HDFS output path> \
	    -cwproot <path to SU on the cluster machines>

For example, we might run:

	suhdp run -command "sufilter f=10,20,30,40 | suchw key1=gx,cdp key2=offset,gx key3=sx,sx b=1,1 c=1,1 d=1,2 | susort cdp gx" \
	    -input aniso.su -output sorted.su -cwproot /usr/local/su

In this case, Seismic Hadoop will run a MapReduce job that applies the `sufilter` and `suchw` commands to each trace during the Map
phase, and then sorts the data by the CDP field in the trace header during the Shuffle phase, and then performs a secondary sort
on the receiver locations for each CDP gather in the Reduce phase. There are a few things to note about running SU commands on the
cluster:

1. Most SU commands that are specified are run as-is by the system. The most notable exception is `susort`, which is performed by the
framework, but is designed to be compatible with the standard `susort` command.
2. If the last SU command specified in the `command` argument is an X Windows command (e.g., `suximage`, `suxwigb`), then the system
will stream the results of running the pipeline to the client machine, where the X Windows command will be executed locally. Make sure
that the `CWPROOT` environment variable is specified on the client machine in order to support this option.
3. Certain commands that are not trace parallel (e.g., `suop2`) will not work correctly on Seismic Hadoop. Also, commands that take
additional input files will not work properly because the system will not copy those input files to the jobs running on the cluster.
We plan to fix this limitation soon.

