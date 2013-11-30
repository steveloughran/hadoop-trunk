<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
  
# title


## Introduction

This document defines the required behaviors of a Hadoop-compatible filesystem
for implementors and maintainers of the Hadoop filesystem, and for users of
the Hadoop FileSystem APIs

Most of the Hadoop operations are tested against HDFS in the Hadoop test
suites, initially through `MiniDFSCluster`, before release by vendor-specific
'production' tests, and implicitly by the Hadoop stack above it.
HDFS's actions have
been modeled closely on the POSIX Filesystem behavior -using the actions and
return codes of Unix filesystem actions as a reference.

What is not so rigorously tested is how well other filesystems accessible from
Hadoop behave. the bundled S3 filesystem makes Amazon's S3 blobstore accessible
through the FileSystem API. The Swift filesystem driver provides similar
functionality for the OpenStack Swift blobstore. The Azure object storage
filesystem in branch-1-win talks to Microsoft's Azure equivalent. All of these
bind to blobstores, which do have different behaviors, especially regarding
consistency guarantees, and atomicity of operations.

The "Local" filesystem provides access to the underlying filesystem of the
platform -its behavior is defined by the operating system -and again, can
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
  
  
## Implicit assumptions of the Hadoop FileSystem APIs
 
The original `FileSystem` class and its usages are based on a set of
assumptions *so obvious that nobody wrote them down* -primarily that HDFS is
the underlying filesystem, and that it offers a subset of the behavior of a
Posix filesystem -or at least the the implementation of the Posix filesystem
APIs and model provided by Linux filesystems.

Irrespective of the API, the model of a filesystem implemented in Unix
is the one that all Hadoop-compatible filesystems are expected to
present.

* It's a hierarchical directory structure with files and directories.

* Files contain data -possibly 0 bytes worth.

* You cannot put files or directories under a file

* Directories contain 0 or more files

* A directory entry has no data itself

* You can write arbitrary binary data to a file -and when that file's contents
 are read in, from anywhere in or out the cluster -that data is returned.

* You can store many gigabytes of data in a single file.

* The root directory, `"/"`, always exists, and cannot be renamed. 

* The root directory, `"/"`, It is always a  directory, and cannot be overwritten by a file write operation.

* Any attempt to  recursively delete the root directory will delete its contents (assuming
  permissions allow this), but will retain the root path itself.

* You cannot rename/move a directory under itself.

* You cannot rename/move a directory atop any existing file other than the
  source file itself.
  
* Directory listings return all the data files in the directory (i.e.
there may be hidden checksum files, but all the data files are listed).

* The attributes of a file in a directory listing (e.g. owner, length) match
 the actual attributes of a file, and are consistent with the view from an
 opened file reference. 

* Security: If the caller lacks the permissions for an operation, it will fail,
raising an error.

### Path Names
  
* A Path is comprised of Path elements separated by `"/"`.

* A path element is a unicode string of 1 or more characters.

* Path element MUST NOT include the characters `":"` or `"/"`.

* Path element SHOULD NOT include characters of ASCII/UTF-8 value 0-31 .

* Path element MUST NOT be `"."`  or `".."` 

* Note also that the Azure blob store documents say that paths SHOULD NOT use
 a trailing `"."` (as their .NET URI class strips it).
 
 * Paths are compared based on unicode code-points. 

 * Case-insensitive and locale-specific comparisons MUST NOT not be used.

### Security Assumptions

Except in the special section on security, this document assumes the client has
full access to the filesystem. Accordingly, the majority of items in the list
do not add the qualification "assuming the user has the rights to perform the
operation with the supplied parameters and paths"

The failure modes when a user lacks security permissions are not specified.

### Networking Assumptions
  
This document assumes this all network operations succeed -all statements
can be assumed to be qualified as *"assuming the operation does not fail due
to a network availability problem"*

* The final state of a filesystem after a network failure is undefined.

* The immediate consistency state of a filesystem after a network failure is undefined.

* If a network failure can be reported to the client, the failure MUST be an
instance of `IOException` or subclass thereof.

* The exception details SHOULD include diagnostics suitable for an experienced
Java developer _or_ operations team to begin diagnostics. For example: source
and destination hostnames and ports on a ConnectionRefused exception.

* The exception details MAY include diagnostics suitable for inexperienced
developers to begin diagnostics. For example Hadoop tries to include a
reference to [ConnectionRefused](http://wiki.apache.org/hadoop/ConnectionRefused) when a TCP
connection request is refused.

<!--  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->

## Core requirements of a Hadoop Compatible Filesystem

Here are the core expectations of a Hadoop-compatible filesystem.
Some filesystems do not meet all these requirements. As a result, some programs may not work as expected. 

### Atomicity

* Rename of a file MUST be atomic.

* Rename of a directory SHOULD be atomic. Blobstore filesystems MAY offer
non-atomic directory renaming.

* Delete of a file MUST be atomic.

* Delete of an empty directory MUST be atomic.

* Recursive directory deletion MAY be atomic. Although HDFS offers atomic
recursive directory deletion, none of the other FileSystems that Hadoop supports
offers such a guarantee -including the local filesystems.

* `mkdir()` SHOULD be atomic.

* `mkdirs()` MAY be atomic. [It is *currently* atomic on HDFS, but this is not
the case for most other filesystems -and cannot be guaranteed for future
versions of HDFS]

* If `append()` is implemented, each individual `append()` call SHOULD be atomic.

* `FileSystem.listStatus()` does not contain any guarantees of atomicity.
  Some uses in the MapReduce codebase (such as `FileOutputCommitter`) do
  assume that the listed directories do not get deleted between listing their
  status and recursive actions on the listed entries.

### Consistency

The consistency model of a Hadoop filesystem is *one-copy-update-semantics*;
that that of a traditional local Posix filesystem. (Note that even NFS relaxes
some constraints about how fast changes propagate)

* Create: once the `close()` operation on an output stream writing a newly
created file has completed, in-cluster operations querying the file metadata
and contents MUST immediately see the file and its data.

*  Update: Once the `close()`  operation on  an output stream writing a newly
created file  has completed,  in-cluster operations  querying the  file metadata
and contents MUST immediately see the new data.

*  Delete: once a `delete()` operation on a path other than "/"  has completed successfully,
it MUST NOT be visible or accessible. Specifically
`listStatus()`, `open()` ,`rename()` and `append()`
 operations MUST fail.

* When a file is deleted then a new file of the same name created, the new file
 MUST be immediately visible and its contents those that are accessed via the FileSystem APIs.
 
* Rename: after a `rename()`  has completed, operations against the new path MUST
succeed; attempts to access the data against the old path MUST fail.

* The consistency semantics out of cluster MUST be the same as that in-cluster:
All clients querying a file that is not being actively manipulated MUST see the
same metadata and data irrespective of their location in or out of the cluster.

### Concurrency

* The data added to a file during a write or append MAY be visible while the
write operation is in progress.

* If a client opens a file for a `read()` operation while another `read()`
operation is in progress, the second operation MUST succeed. 
Provided no writes/appends take place during this period both clients
MUST have a consistent view of the same data.

* If a file is deleted while a `read()` operation is in progress, the
`delete()` operation SHOULD complete successfully. Implementations MAY cause
`delete()` to fail with an IOException instead.

* If a file is deleted while a `read()` operation is in progress, the `read()`
operation MAY complete successfully. Implementations MAY cause `read()`
operations to fail with an IOException instead.

* Multiple writers MAY open a file for writing. If this occurs, the outcome
is undefined

* Undefined: action of `delete()` while a write or append operation is in
progress

### Operations and failures

* All operations MUST eventually complete, successfully or unsuccessfully,
  throw an `IOException` or subclass thereof.

* The time to complete an operation is undefined and may depend on
the implementation and on the state of the system.

* Operations MAY throw a `RuntimeException` or subclass thereof.

* Operations SHOULD raise all network, remote and high-level problems as
an `IOException` or subclass thereof, and SHOULD NOT raise a
`RuntimeException` for such problems.

* Operations SHOULD report failures by way of raised exceptions, rather
than specific return codes of an operation.

* In the text, when an exception class is named, such as `IOException`,
the raised exception MAY be an instance of or subclass of the named exception.
It MUST NOT be a superclass

* If an operation is not implemented in a class, the implementation must
throw an `UnsupportedOperationException`

* Implementations MAY retry failed operations until they succeed. If they do this,
they SHOULD do so in such a way that the *happens-before* relationship between
any sequence of operations meets the consistency and atomicity requirements
stated. (See [HDFS-4849](https://issues.apache.org/jira/browse/HDFS-4849))
for an example of this: HDFS does not implement any retry feature that
could be observable by other callers.

### Undefined limits

Here are some limits to filesystem capacity that have never been explicitly
defined.
  
1. The maximum number of files in a directory.

1. Max number of directories in a directory

1. Maximum total number of entries (files and directories) in a filesystem.

1. The maximum length of a filename under a directory (HDFS: 8000)

1. `MAX_PATH` - the total length of the entire directory tree referencing a
file. Blobstores tend to stop at ~1024 characters

1. The maximum depth of a path (HDFS: 1000 directories)

1. The maximum size of a single file

### Undefined timeouts

Timeouts for operations are not defined at all, including:

* The maximum completion time of blocking FS operations.
MAPREDUCE-972 documents how `distcp` broke on slow s3 renames.

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
incur a cost that is `O(entries)`. Hadoop 2 added iterative listing to
handle the challenge of listing directories with millions of entries without
buffering.

1. A `close()` of an `OutputStream` is fast, irrespective of whether or not
the file operation has succeeded or not.

1. The time to delete a directory is independent of the size of the number of 
child entries

Different filesystems not only have different behavior, under excess load or failure
conditions a filesystem may behave very differently. 

