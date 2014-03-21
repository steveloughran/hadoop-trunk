<!---
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
-->

# Apache Hadoop FileSystem contract


## Introduction

This is a list of expected behaviors of a Hadoop-compatible filesystem,
including many that aren't (or can't be) explicitly tested -yet.

Most of the Hadoop operations are tested against HDFS in the Hadoop test
suites, because `MiniDFSCluster` is used so ubiquitously. HDFS's actions have
been modeled closely on the Posix Filesystem behavior -using the actions and
return codes of Unix filesystem actions as a reference.

What is not so rigorously tested is how well other filesystems accessible from
Hadoop behave. the bundled S3 filesystem makes Amazon's S3 blobstore accessible
through the FileSystem API. The Swift filesystem driver provides similar
functionality for the OpenStack Swift blobstore. The Azure object storage
filesystem in branch-1-win talks to Microsoft's Azure equivalent. All of these
bind to blobstores, which do have different behaviours, especially regarding
consistency guarantees, and atomicity of operations.

The Local filesystem provides access to the underlying filesystem of the
platform -its behaviour is defined by the operating system -and again, can
behave differently from HDFS.

Finally, there are filesystems implemented by third parties, that assert
compatibility with Apache Hadoop. There is no formal compatibility suite, and
hence no way for anyone to declare compatibility except in the form of their
own compatibility tests.

This document does not attempt to formally define compatibility; passing the
associated test suites does not guarantee correct behavior in MapReduce jobs,
or HBase operations.

What the test suites do define is the expected set of actions -failing these
tests will highlight potential issues.



### Naming

This document follows RFC2119 rules regarding the use of MUST, MUST NOT, MAY,
and SHALL -and MUST NOT be treated as normative.

The term "iff" is shorthand for "if and only if".


## Assumptions contained in the FileSystem uses

The original `FileSystem` class and its usages are based on a set of
assumptions "so obvious that nobody wrote them down" -primarily that HDFS is
the underlying filesystem, and that it offers a subset of the behavior of a
Posix filesystem

* It's a hierarchical directory structure with files and directories.

* Files contain data -possibly 0 bytes worth.

* You cannot put files or directories under a file

* Directories contain 0 or more files

* A directory entry has no data itself

* You can write arbitrary binary data to a file -and when that file's contents
 are read in, from anywhere in or out the cluster -that data is returned.

* You can store many GB of data in a single file.

* The root directory, `"/"`, always exists, and cannot be renamed. It is always a
  directory, and cannot be overwritten by a file write operation. An attempt to
  recursively delete the root directory will delete it's contents (assuming
  permissions allow this), but will retain the root path itself.

* You cannot rename/move a directory under itself.

* You cannot rename/move a directory atop any existing file other than the
  source file itself.

* Security: If you don't have the permissions for an operation, it will fail
  with some kind of error.

### Path Names

Similarly, path names have a set of rules some of which are considered fundamental ("they all begin with '/"), others implementatin specific.

  * A path is comprised of path components separated by '/'.
  
  * Paths are compared based on unicode code-points. 
  
  * Case-insensitive and locale-specific comparisons MUST NOT not be used.
  
  * A path component is unicode string of 1 or more characters.
  
  * Path components MUST NOT include the characters `":"` or `"/"`.
  
  * Path components MUST NOT include characters of ASCII/UTF-8 value 0-31 .
  
  * Path components MUST NOT be `"."`  or `".."` 
  
  * Azure blob store says that paths SHOULD NOT use a trailing "." (as their
   .NET URI class strips it).

### Security Assumptions

Except in the special section on security, this document assumes the client has
full access to the filesystem. Accordingly, the majority of items in the list
do not add the qualification *assuming the user has the rights to perform the
operation with the supplied pa rameters and paths"*

The failure modes when a user lacks security permissions are not specified.

### Networking Assumptions

This document assumes this all network operations succeed -all statements
can be assumed to be qualified as *"assuming the operation does not fail to
to an undefined availability problem"*

* The final state of a filesystem after a network failure is undefined.

* The immediate consistency state of a filesystem after a network failure is undefined.

* If a network failure can be reported to the client, the failure MUST be an
  instance of `IOException`

* The exception details SHOULD include diagnostics suitable for an experienced
  Java developer _or_ operations team to begin diagnostics. For example: source
  and destination hostnames and ports on a `ConnectionRefused` exception.

* The exception details MAY include diagnostics suitable for inexperienced
  developers to begin diagnostics. For example Hadoop tries to include a
  reference to http://wiki.apache.org/hadoop/ConnectionRefused when a TCP
  connection request is refused.

## Core Requirements of a FileSystem


### Atomicity

* Rename of a file MUST be atomic.

* Rename of a directory MUST be atomic.
* Delete of a file MUST be atomic.

* Delete of an empty directory MUST be atomic.

* Recursive directory deletion MAY be atomic. Although HDFS offers atomic
recursive directory deletion, none of the other FileSystems that Hadoop supports
offers such a guarantee -including the local filesystems.

* `mkdir()` SHOULD be atomic.

* `mkdirs()` MAY be atomic. [It is *currently* atomic on HDFS, but this is not
the case for most other filesystems -and cannot be guaranteed for future
versions of HDFS]

* If `append()` is implemented, each individual `append()` operation SHOULD be atomic.

* `FileSystem.listStatus()` does contain any claims of atomicity -caching algorithms mean that HDFS listings can be inconsistent.

### Consistency

The consistency model of a Hadoop filesystem is *one-copy-update-semantics*;
effectively that of a traditional Posix filesystem. 

Every machine in the cluster expects to see the same view of data as others.

Furthermore the consistency semantics out of cluster MUST be the same as that in-cluster:

That said, there are some corner-cases in HDFS to be aware of, specifically
on when changes becomes visible to applications that have an open/cached copy
of a file or directory's data.

* there's no guarantee that changes to a file (e.g. appends, deletes) will be visible
to clients that already have the file open
* changes to a directory may not be visible to a client that is part-way
through enumerating the contents of a directory.



### Concurrency

* The data added to a file during a write or append MAY be visible while the
  write operation is in progress.

* If a client opens a file for a `read()` operation while another `read()`
  operation is in progress, the second operation MUST succeed. Both clients
  MUST have a consistent view of the same data.

* If a file is deleted while a `read()` operation is in progress, the
  `delete()` operation SHOULD complete successfully. Implementations MAY cause
  `delete()` to fail with an `IOException` instead.
 
* If a file is deleted while a `read()` operation is in progress, the `read()`
  operation MAY complete successfully. Implementations MAY cause `read()`
  operations to fail with an `IOException` instead.

* Multiple writers MAY open a file for writing. If this occurs, the outcome
is undefined

* Undefined: action of `delete()` while a write or append operation is in
  progress

### Undefined limits

Here are some limits to filesystem capacity that have never been explicitly
defined.

 1. The maximum # of files in a directory.

 1. Max # of directories in a directory

 1. Maximum total number of entries (files and directories) in a filesystem.

 1. Max length of a filename (HDFS: 8000)

 1. `MAX_PATH` - the total length of the entire directory tree referencing a
  file. Blobstores tend to stop at ~1024 characters

 1. max depth of a path (HDFS: 1000 directories)

 1. The maximum size of a single file

### Undefined timeouts

Timeouts for operations are not defined at all, including:

* The maximum completion time of blocking FS operations.
MAPREDUCE-972 shows how distcp broke on slow s3 renames.

* The timeout for idle read streams before they are closed.

* The timeout for idle write streams before they are closed.

The blocking-operation timeout is in fact variable in HDFS, as sites and
clients may tune the retry parameters so as to convert filesystem failures and
failovers into pauses in operation. Instead there is a general assumption that
FS operations are "fast but not as fast as local FS operations", and that data
reads and writes take a time that scales with the volume of data. This
assumption shows a more fundamental one: the filesystem is "close" to the
client applications as far as network latency and bandwidth is concerned.

There are also some implicit assumptions about the overhead of some operations

1. `seek()` operations are fast and incur little or no network delays. [This 
does not hold on blob stores]

1. Directory list operations are fast for directories with few entries.

1. Directory list operations are fast for directories with few entries, but may
incur a cost that is O(no. of entries). Hadoop 2.x adds iterative listing to
handle the challenge of listing directories with millions of entries without
buffering -at the cost of consistency.

1. A `close()` of an `OutputStream` is fast, irrespective of whether or not
the file operation has succeeded or not. Again, object stores break these assumptions.


### Block Size data

Block-based filesystems have a block size, which is used by higher layers
in the Hadoop stack -specifically the MapReduce layer-. For that reason,
even if a filesystem does not have a notion of blocks, it MUST provide
a blocksize to callers -ideally one large enough to ensure that Map tasks
are given a large amount of data to work through. 


* FileSystem implementations MUST provide a block size of size >1.

* FileSystem implementations SHOULD provide a block size large enough
for the layers above to perform useful work. 

* Block-based filesystems SHOULD provide the actual size of blocks of files.

* FileSystem implementations MUST return a block size >=1 in response to calls
of `getDefaultBlockSize()` and `getDefaultBlockSize(Path)`
block size of size >1.

* FileSystem implementations MUST return a block size >=1 in response to calls
of `getBlockSize(path)` when `path` references a file. 

* `getBlockSize(path)` SHOULD throw a `FileNotFoundException` if the 
path does not exist. (ISSUE: SHOULD vs MUST vs MAY?)

* For all `FileStatus fs = getFileStatus(path)` where `path` exists and is a file,
`fs.getBlockSize()` must return a block size >=1

* The value of `FileSystem.getBlockSize(path)` MUST match the value of
`getFileStatus(path).getBlockSize()`


## Network Failures


* Any operation with a filesystem MAY signal an error by throwing an
  `IOException` or subclass thereof.

* The specific cause of the failure SHOULD be explicitly defined in the
  `IOException` subclass type. This is aid diagnostics.

* The cause or details of the failure SHOULD be described in the exception's
`toString()` value. This to aid diagnostics.

* Filesystem operations MUST NOT throw `RuntimeException` exceptions on the
failure of a remote operations, authentication or other operational problems.

* Stream read operations MAY fail if the read channel has been idle for a
Filesystem-specific period of time.

* Stream write operations MAY fail if the write channel has been idle for
a Filesystem-specific period of time.

* Network failures MAY be raised in the Stream `close()` operation

