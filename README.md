
# Deduplifier

A client/server system for identifying duplicates in a collection of files spanning multiple hosts, disks, filesystems.

File scanning client tested on *Windows 10* and *Ubuntu Linux (20.04, 21.04)* systems, running *python 3.8+*. The server code was developed using standard [*Docker*](https://www.docker.com/) images for running *Linux*-based database and web servers.

**Note**: *This is not production quality code, but rather a personal project which is still very much a work in progress. I started working on it to exercise some of my skills and I'm sharing it now primarily as a demonstration of those skills.*


## General Architecture

The system currently consists of a multi-process filesystem scanner written in [*python*](https://www.python.org/) which reports to a [*postgres*](https://www.postgresql.org/) database backend.

The database includes a number of materialized views and stored procedures for organizing and presenting the data.

A [Flask](https://flask.palletsprojects.com/en/2.1.x/)-based web server is provided for reporting on the data. 

The various components of the system are described in more detail later in this document.

## Build Infrastructure

One of the goals for this project was to create a general system for developing, testing, and deploying scalable software services. 

To that end, this solution uses *Docker* to define and manage the various server instances. These are built using the [scaffold](https://github.com/codecat555/scaffold) framework I developed for this purpose. That system uses hierarchical [*Makefiles*](https://en.wikipedia.org/wiki/Make_(software)#Makefile) and a simple templating mechanism to support generation of a variety of configurations.

The [scaffold](https://github.com/codecat555/scaffold) system is provided as a separate repository, and this deduplifier repository can be managed as a git submodule of a [scaffold](https://github.com/codecat555/scaffold) repo instance.

For development, this deduplifier module uses the [scaffold](https://github.com/codecat555/scaffold) build system to spin out a [*Canonical Multipass*](https://Multipass.run/) instance (running *Ubuntu Linux*) to serve as the *Docker* host.

The folder containing this *deduplifier* repo is mounted on the *Docker* instances using *sshfs*, so code changes are immediately visible whenever a file is saved.

The build system also handles plumbing the networking through the *iptables* firewall so requests can pass from the local system to the *Multipass*-based *Docker* host and thence to the target *Docker* instances (and back again).

>*Note*: After upgrading my development system from Ubuntu 20.04 to 21.04 (and upgrading *Multipass* at the same time), there were some networking differences w.r.t. iptables. The earlier version required a custom service ([host_vm_network_service](https://github.com/codecat555/scaffold/tree/main/host_vm_network_service)) to tweak the legacy iptables configuration. That service is no longer required and has been removed from the build dependency chain (but not from the repo).

The entire server infrastructure can be casually torn down and rebuilt in a few minutes, without affecting any of the code or configuration. This can be done locally or, with minor changes, in some remote datacenter.

The one caveat is that the database files reside within the local file system of the postgres docker instance. Database performance over *sshfs* was poor, so the system allows the database to run locally on the *Docker* database instance while still supporting the easy teardown/rebuild use case.

The [scaffold](https://github.com/codecat555/scaffold) build system provides for (optionally) copying the database files out when the database instance is destroyed, and can then restore those files when the instance is recreated. This process can be controlled via environment variables passed into the build, so different data sets can easily be loaded for testing or whatever. At this point, it would be interesting to see if the *sshfs* performance has improved at all with recent upgrades.


## Compatibility

Make, Git and Multipass are all multiplatform tools. Everything else is encapsulated and so, apart from the networking issue mentioned below, this system should theoretically run on any system that is supported by those tools.  

The code for plumbing the network connections through *iptables* is one dependency that ties the server-side to running on (*iptables*-based) Linux. This could be generalized, if there were ever a need to run on other platforms.

## Testing

Testing so far has been sparse, and ad hoc. There is some automation, but it's rudimentary and not included in this repo.

## Usage

1. Install git, make, and [*Multipass*](https://1.run/) on your system.
    1. Tested with *Ubuntu 20.04 and 21.04*.
    1. Theoretically, should run on any system running *iptables* and with the above tools installed. 
1. Checkout the [scaffold](https://github.com/codecat555/scaffold) code.
1. In the [scaffold](https://github.com/codecat555/scaffold) folder, add the deduplifier code as a submodule and build it:
    1. Run `git submodule add <deduplifier-code-url>`
    1. Run `make -e deduplifier`
        1. This should create the docker host and docker instances.
1. Make sure everything is up and running:
    1. Use the `multipass list` command to see the docker host (named *deduplifier-host*).
    1. Use `multipass exec deduplifier-host -- docker ps` to see the docker instances.
1. Connect to the web service at `http://<your-ip-address>:5005` .
    1. Replace *your-ip-address* with the hostname or ip address of your system.
    1. You should see a page telling you the database is empty, with instructions on how to run the file scanner program, `scan.py`.
    1. You can also see the scanner usage by running `scan.py` with no arguments.
1.  Run the file scanner on the system(s) you want to scan.
    1. You will need python3 and some auxiliary modules.
    1. Not packaged up, so you will need to discover the required modules by inspection or trial-and-error.
1. Create the database materialized views.
    1. Currently a manual step:
        > `psql --host localhost --port 6681 --username postgres -d deduplifier -f deduplifier/code/sql/create_views.sql`
    1. After creating the views, they should be refreshed any time the data is changed.
        > `psql --host localhost --port 6681 --username postgres -d deduplifier -f deduplifier/code/sql/refresh_views.sql`
1. Connect to the web service at `http://<your-ip-address>:5005` .
    1. See the (*updated*) database summary page, showing statistics about your data.
    1. Try `http://<your-ip-address>:5005/files_with_dups` .

## About the Scanner

The file scanner spends a lot of time waiting for the cpu (checksum generation), the filesystem and the network/database. So, in order keep things moving, the scanner can spin out multiple processes which run in parallel. Processes are used rather than threads in order to avoid any limitations imposed by the [GIL](https://realpython.com/python-gil/).

The scanning processes are coordinated using a shared queue, provided by the [*multiprocessing*](https://docs.python.org/3/library/multiprocessing.html) module. The queue is seeded with the paths provided by the user via the command line. Each process pulls the next item from the queue and processes it. If the item is a file, it is examined and uploaded to the database. If the item is a directory, the files and subdirectories it contains are pushed onto the queue. When there's no more work to do, i.e. the queue is empty, the processes quit and then the main process exits.

There are options for skipping files that have been scanned previously, and for skipping those that are covered by an exclusion list provided by the user.

Currently, the scanner is hard-coded to use SHA-256 for generation of file identity hashes. This is surely overkill, but I wanted to use larger numbers during development to ensure the system is capable of handling them properly.

### Image Files

Image files are given special attention, since they often include embedded metadata such as GPS coordinates. When a scanner instance recognizes an image file, it tries to extract any [EXIF](https://en.wikipedia.org/wiki/Exif) data it contains. This could be generalized to support other types of metadata and other file types.

Image files also present an opportunity for fuzzy matching, to identify duplicates which are just variations of the same base image. At last check, there were a number of python modules that support this sort of comparison.

### Logs

Each scanner process logs its activity to a separate file within a common log directory. The path is printed on stdout when the scanner starts. 

## About the Database

The basic structure of the database tracks information about the discovered files, including the *source host name, volume name, path, timestamps, mine type and identity hash (checksum)*. For image files, any available metadata tags are recorded.

The discovery time and scanner process id are also captured with the file data and can be correlated with the log files generated by the scanner processes.

Each filesystem path is recorded as a sequence of strings, rather than as a single string. This is done to aid reporting on totals at any level of the file system hierarchy. The same result could be obtained by using string operations on the full file system paths, but encoding the information in the table structure enables use of hierarchical queries and other powerful facilities of the database.

### Views

A collection of views are layered on top of the basic table structure. These provide more convenient access to some aspects of the data, such as the reassembled paths and their relations.

Most of these views are *materialized*, meaning that they are calculated once and stored. These views must be refreshed whenever the data changes. Currently, there are no automatic mechanisms included for keeping these materialized views up to date.

This layer needs work. The views that are defined need refinement, and it probably makes sense to define some that don't exist yet.

### Stored Procedures

The next layer in the data base is a set of PL/pgSQL stored procedures. This is the API layer for the database, and is used by the filesystem scanner and the web server code.

This is another area where more work is needed. The upsert functions are probably pretty solid, but the reporting functions are not complete and - possibly - not even correct.

### Performance

There are various indexes defined, but the database has not really been tuned at all. There are probably gains to be realized, with even just a little work.


## About the Web Service

The current configuration uses Flask, Jinja, HTML and CSS for generating web pages. There is no client-side code at this time. In the long term, it would make sense to adopt some front-end framework for defining the user interface.

 There are a set of generic Jinja templates for displaying result sets from the database, with basic support for pagination and jumping back and forth within the data. It's really just a basic interface to be used during development, but is suitable for displaying results for any number of different queries.

There is some back-end support for sorting the data by column, but there is no user-interface support for this at present.

## Current Status
 * Code not complete.
 * Needs code for generating seperate test, staging and production instances.
* Needs functional tests and bug fixes.
 * Needs security review.
 * Needs performance review.