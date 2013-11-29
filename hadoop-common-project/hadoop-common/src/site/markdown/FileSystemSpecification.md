<not --  Licensed under the Apache License, Version 2.0 (the "License"); -->
<not --  you may not use this file except in compliance with the License. -->
<not --  You may obtain a copy of the License at -->

<not --    http://www.apache.org/licenses/LICENSE-2.0 -->
<not --  -->
<not --  Unless required by applicable law or agreed to in writing, software -->
<not --  distributed under the License is distributed on an "AS IS" BASIS, -->
<not --  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. -->
<not --  See the License for the specific language governing permissions and -->
<not --  limitations under the License. See accompanying LICENSE file. -->


# Apache Hadoop FileSystem Specification

<not --  %{toc|section=1|fromDepth=0} -->

<not --  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->

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

<not --  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->

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
`listStatus()`, `open()`,`rename()` and `append()`
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

<not --  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->

## Formal specification of a Hadoop filesystem

This section attempts to model the contents a filesystem as a set of paths that 
are either directories, symbolic links or files; only files may contain data.

There is surprisingly little prior art in this: multiple specifications of
the Unix filesystems as a tree of inodes, but nothing public which defines the
notion of "Unix filesystem as a conceptual model for data storage access". 

This specification attempts to do that; to define the Hadoop File System model
and APIs so that multiple filesystems can implement the APIs and present a consistent
model of their data to applications. It does not attempt to specify any of the
concurrency  

The operations that a filesystem supports either examines the paths and
reference data, or updates it.

### Notation

A mathematically pure notation such as [The Z Notation](www.open-std.org/jtc1/sc22/open/n3187.pdf‎)
would be the strictest way to define the filesystem behavior, and could even
be used to prove some axioms.

However, it has a number of practical flaws
1. Such notations are not as widely used as they should be -so the broader software
development community is not going to have practical experience of it.
1. It's very hard to work with without dropping into tools such as LaTeX *and* add-on libraries.
1. Even those people who claim to understand such formal notations don't really.

Given the goals of this specification are to document Filesystem behavior in order for it
to be understood by developers, and to derive tests from the specification, broad
comprehensibility, ease of maintenance and the ease of deriving tests must take priority
over any Computer-Science strict-notation

### Mathematics Symbols in this document

This document does use a subset of [the notation in the Z syntax](http://staff.washington.edu/jon/z/glossary.html),
but in an ascii form and the use of Python list notation for manipulating lists and sets

* `iff` : `iff`: If and only if
* `⇒` : `implies`
* `→` : `-->` total function
* `↛` : `->` partial function


* `∩` : `^`: Set Intersection 
* `∪` : `|`: Set Union
* `\` : `-`: Set Difference

* `∃` : `exists` Exists predicate
* `∀` : `forall`: For all predicate
* `=` : `==` Equals operator
* `≠` : `!=` operator. In Java `z ≠ y` is written as `not  ( z == y )` provided that `z` and `y` are from simple types that can be compared
* `≡` : `equivalent-to` equivalence operator. This is stricter than equals.
* `∅` : `{}` Empty Set. `∅ ≡ {}`
* `≈` : `approximately-equal-to` operator
* `¬` : `not` Not operator. In Java, `not `
* `∄` : `does-not-exist`: Does not exist predicate. Equivalent to `not exists`
* `∧` : `and` : local and operator. In Java , `and`  
* `∨` : `or` : local and operator. In Java, `else`  
* `` : `` :  
* `∈` : `in` : element of
* `∉` : `not-in` : not an element of
* `:=` : `` :  

* `` : `#` :  Python-style comments

* `happens-before` : `happens-before` : Lamport's ordering relationship as defined in  
[Time, Clocks and the Ordering of Events in a Distributed System](http://research.microsoft.com/en-us/um/people/lamport/pubs/time-clocks.pdf)

#### Sets ,  Lists ,  Maps and Strings

The [python data structures](http://docs.python.org/2/tutorial/datastructures.html)
are used as the basis for this syntax as it is both plain ASCII and well known

##### Lists

* A list *L* is an ordered sequence of elements `[l1, l2, ... ln]`
* The size of a list `len(L)` is the number of elements in a list
* Items can be addressed by a 0-based index  `l1==L[0]`
* Python slicing operators can address subsets of a list `L[0:3]==[l1,l2]`, `L[:-1]==ln`
* Lists can be concatenated `L' = [ h ] + L`
* The membership predicate `in` returns true iff an element is a member of a List: `l2 in L`
* List comprehensions can create new lists: `L' = [ x for x in l where x < 5]`
* for a list *L*, `len(L)` returns the number of elements.


##### Sets

Sets are an extension of the List notation, adding the restrictions that there can
be no duplicate entries in the set, and there is no defined order.

* A set is an unordered collection of items surrounded by `{` and `}` braces. 
* When declaring one, the python constructor `set([list])` is used
* The empty set `set([])` has no elements
* All the usual set concepts apply
* The membership predicate is `in`
* Set comprehension uses the Python list comprehension
`S' = {s for s in S where len(s)==2}`
* for a set *S*, `len(S)` returns the number of elements.



##### Maps 

Maps are written as Python dictionaries; {"key":value, "key2",value2}

* `keys(Map)` represents the set of keys in a map
* `k in Map` holds iff `k in keys(Map)`


##### Strings

Strings are lists of characters represented in double quotes. e.g. `"abc"`

    "abc" == ['a','b','c']

#### State Immutability

All system state declarations are immutable.

The suffix "'" is used as the convention to indicate the state of the system after a operation:

    L' = L + ['d','e']


#### Function Specifications

A function is defined as a set of preconditions and a set of postconditions,
where the postconditions define the new state of the system and the return value from the function.

In classic Z-style specification languages, the preconditions define the predicates that MUST be
satisfied else some failure condition is raised. 

For Hadoop we need to be able to specify what failure condition results if a specification is not
met -usually what exception is to be raised.

The notation `raise <exception-name>` is used to indicate that an exception is to be raised.

It can be used in the if-then-else sequence to define an action if a precondition is not met.

Example:

    if not exists(FS, Path) then raise IOException

We also need to distinguish predicates that MUST be satisfied, along with those that SHOULD be met.
For this reason a function specification MAY include a section in the preconditions marked 'Should:'
All predicates declared in this section SHOULD be met, and if there is an entry in that section
which specifies a stricter outcome, it SHOULD BE preferred. Here is an example of a should-precondition


Should:

    if not exists(FS, Path) then raise FileNotFoundException

Functions can be divided into partial functions and total functions.

Total functions have a valid output for every input, for example `def double(i :Int)-->Int: i * 2` is valid for all integers

Partial functions are not valid for all inputs, for example, `def inverss:(i: Int)->Float: 1/i` is not valid for the input 0.

Total functions are notated with the term `-->`; partial functions -which form the majority of functions in this
specification, with `->`.



### A model of a Filesystem



#### Paths and Path Elements

A Path is a list of Path elements which represents a path to a file, directory of symbolic link

Path elements are non-empty strings. The exact set of valid strings MAY 
be specific to a particular filesystem implementation.

Path Elements MUST NOT be in `set(["", ".",  "..", "/"])`

Path Elements MUST NOT contain the characters `set(['/', ':')`.

Filesystems MAY have other strings that are not permitted in a path element.

When validating path elements, the exception `InvalidPathException` SHOULD
be raised when a path is invalid [HDFS]

Predicate: `valid-path-element:List<String>`

A path element `pe` is invalid if any character in it is in the set of forbidden characters,
or the element as a whole is invalid

    forall e in pe: not (e in set(['/', ':'))
    not pe in set(["", ".",  "..", "/"])


Predicate: `valid-path:List<PathElement>`

A Path `p` is valid if all path elements in it are valid

    def valid-path(pe): forall pe in Path: valid-path-element(pe)


The set of all possible paths is *Paths*; this is the infinite set of all lists of valid path elements.
  
The path represented by empty list, `[]` is the *root path*, and is denoted by the string `"/"`

The partial function `parent(path:Path)->Path` provides the parent path can be defined using
list slicing

    def parent(pe) : pe[0:-1] 

Preconditions:

    path != []


#### `Filename:Path->PathElement`

The last Path Element in a Path is called the filename:
  
    def filename(p) : p[:-1]
   
Preconditions:

    p != []

#### `childElements:(Path p, Path q)->Path`


The partial function `childElements:(Path p, Path q)->Path` 
is the list of path elements in `p` that follow the path `q`

    def childelements(p, q): p[len(q):] 

preconditions

    
    # The path 'q' must be at the head of the path 'p' 
    q == p[:len(q)]
  


### Defining the Filesystem


A filesystem `FS` contains a set of directories, a dictionary of paths and a set of symbolic links

    (directories:set<Path>, files:[Path->List[byte]], symlinks:set<Path>) 


    def filenames(FS): keys(files(FS)) 
    
The entire set of a paths finite subset of all possible Paths, and functions to resolve a path to data, a directory predicate or a symbolic link

    def paths(FS) : directories(FS) | filenames(FS) | symlinks(FS)) 

A path is deemed to exist if it is in this aggregate set

    def exists(FS, p): p in paths(FS)

*Root* The root path, "/" is a directory represented  by the path [], which must always exist in a filesystem
  
    def isRoot(p) : p == [].
    
    forall FS in FileSystems : [] in directories(FS)



#### Directory references
  
A path MAY refer to a directory in a filesystem
  
    isDir(FS, p): p in directories(FS) 


Directories may have children, that is, there may exist other paths
in the filesystem whose path begins with a directory. Only directories
may have children. This can be expressed
by saying that every path's parent must be a directory.

It can then be declared that a path has no parent in which case it is the root directory,
or it MUST have a parent that is a directory
   
    forall p in paths(FS) : isRoot(p) or isDir(FS, parent(p))
  
Because the parent directories of all directories must themselves satisfy
this criterion, it is implicit that only leaf nodes may be files or symbolic links

Furthermore, because every filesystem contains the root path, every filesystem
must contain at least one directory.
  
A directory may have children
  
    def children(FS, p) :  {q for q in paths(FS) where parent(q) == p}


There are no duplicate names in the child paths, because all paths are
taken from the set of lists of path elements: there can be no duplicate entries
in a set, hence no children with duplicate names.

A path *D* is a descendant of a path *P* if it is the direct child of the
path *P* or an ancestor is a direct child of path *P*
  
    def isDescendant(P, D) : parent(D) == P where isDescendant(P, parent(D)) 
  
The descendants of a directory P are all paths in the filesystem whose
path begins with the path P -that is their parent is P or an ancestor is P

    def descendants(FS, D): {p for p in paths(FS) where isDescendant(D, p)} 


#### File references

A path MAY refer to a file; that it it has data in the filesystem; its path is a key in the data dictionary

    def isFile(FS, p):  p in data(FS)

#### Symbolic references

A path MAY refer to a symbolic link

    def isSymlink(FS, p): p in symlinks(FS)



#### Exclusivity
A path cannot refer to more than one of a file, a directory or a symbolic link


    directories(FS) ^ keys(data(FS)) == {}
    directories(FS) ^ symlinks(FS) == {}
    keys(data(FS))(FS) ^ symlinks(FS) == {}
    

This implies that only files may have data.

Not covered: hard links in a filesystem. If a filesystem supports multiple
references in *paths(FS)* to point to the same data, the outcome of operations
are undefined.

This model of a filesystem is sufficient to describe all the filesystem
queries and manipulations -excluding metadata and permission operations.
The Hadoop `FileSystem` and `FileContext` interfaces can be specified
in terms of operations that query or change the state of a filesystem.



<not --  ============================================================= -->
<not --  CLASS: FileSystem -->
<not --  ============================================================= -->

## org.apache.hadoop.fs.FileSystem
  
All operations that take a Path to this interface MUST support relative paths.
In such a case, they must be resolved relative to the working directory
defined by `setWorkingDirectory()`.


### boolean exists(Path P)


    exists(FS, P)
  

### boolean isDirectory(Path P) 

    exists(FS, P) and isDir(FS, P)
  

### boolean isFile(Path P) 


    exists(FS, P) and isFile(FS, P)
  

### boolean isSymlink(Path P) 


    exists(FS, P) and isSymlink(FS, P)
  

### FileStatus getFileStatus(Path P)

#### Preconditions

  
    exists(FS, P) else raise FileNotFoundException

#### Postconditions


    FS' = FS
    result = FileStatus(length(F), isDirectory(F), [metadata], F)) 
  

<not --  ============================================================= -->
<not --  METHOD: mkdirs() -->
<not --  ============================================================= -->

### boolean (Path F, FsPermission Permission )

#### Preconditions


    isFile(F) =>
     raise (IOException | ParentNotDirectoryException | FileAlreadyExistsException) 
    isDir(F) => return true
    mkdirs(parent(F))

#### Postconditions

  
    FS' where (exists(FS', F) and isDir(FS', F))
    true

The probe for the existence and type of a path and directory creation MUST be
atomic. The combined operation, including `mkdirs(parent(F))` MAY be atomic.

The return value is always true - even if
a new directory is not created. (this is defined in HDFS)

<not --  ============================================================= -->
<not --  METHOD: create() -->
<not --  ============================================================= -->

### FSDataOutputStream create(Path f, ...)


    FSDataOutputStream create(Path P,
          FsPermission permission,
          boolean overwrite,
          int bufferSize,
          short replication,
          long blockSize,
          Progressable progress) throws IOException;


#### Preconditions

    #file must not exist for a no-overwrite create
    not overwrite and isFile(FS, P)  => raise FileAlreadyExistsException
    #it must not be a directory either; exception is the same
    not overwrite and isDir(FS, P)  => raise FileAlreadyExistsException
      
    #overwriting a directory must fail.    
    not isDir(FS,P) else raise FileAlreadyExistsException, FileNotFoundException
    
    
    # MUST raise FileAlreadyExistsException, FileNotFoundException
    not  exists(FS, P) else (overwrite and isFile(FS, P))
    isDir(FS, parent(P)) else mkdirs(parent(P))


#### Postconditions
  

    FS' where isFile(FS', P)) 
  

Return: `FSDataOutputStream`, where `FSDataOutputStream.write(byte)`,
will, after any flushing, sycing and committing, add `byte`
to the tail of the list returned by `data(FS, P)`.

* Filesystems may reject the request for other
reasons -such as the FS being read-only  (HDFS), 
the block size being below the minimum permitted (HDFS),
the replication count being out of range (HDFS),
quotas on namespace or filesystem being exceeded, reserved
names, ...etc. All rejections SHOULD be `IOException` or a subclass thereof
and MAY be a `RuntimeException` or subclass. (HDFS: `InvalidPathException`)

* S3N, Swift and other blobstores do not currently change the FS state
until the output stream `close()` operation is completed.
This MAY be a bug, as it allows >1 client to create a file with overwrite=false

* Local FS raises a `FileNotFoundException` when trying to create a file over
a directory, hence it is is listed as a possible exception to raise
in this situation.

<not --  ============================================================= -->
<not --  METHOD: append() -->
<not --  ============================================================= -->

### FSDataOutputStream append(Path P, int bufferSize, Progressable progress)

Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    exists(FS, P) else raise FileNotFoundException
    isFile(FS, P)) else raise FileNotFoundException or IOException

#### Postconditions
  

Return: `FSDataOutputStream`, where `FSDataOutputStream.write(byte)`,
will, after any flushing, syncing and committing, add `byte`
to the tail of the list returned by `data(FS, P)`.
  

<not --  ============================================================= -->
<not --  METHOD: open() -->
<not --  ============================================================= -->

### FSDataInputStream open(Path f, int bufferSize)

  Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    not exists(FS, P) else raise FileNotFoundException
    not isFile(FS, P)) else raise FileNotFoundException or IOException


#### Postconditions
  
  
#### HDFS implementation details

1. MAY throw `UnresolvedPathException` when attempting to traverse
symbolic links

1. throws `IOException("Cannot open filename " + src)` if the path
exists in the metadata, but copies of its blocks can be located;
-`FileNotFoundException` would seem more accurate and useful.
  

<not --  ============================================================= -->
<not --  METHOD: delete() -->
<not --  ============================================================= -->

### `FileSystem.delete(Path P, boolean recursive)`

#### Preconditions

    # a directory with children and recursive == false cannot be deleted
    # raise IOException
    isDir(FS, P) => (recursive else childen(FS, P) == {} )


#### Postconditions

    #return false if file does not exist; FS state does not change
    not exists(FS, P) => (FS, false)
    
    # a path referring to a file is removed, return value: true
    isFile(FS, P) => (FS' where (not exists(FS', P)), true)
  
    
    # deleting an empty root returns true or false
    isDir(FS, P) and isRoot(P) and childen(FS, P) == {} 
      => (FS, true)) 
  
    # deleting an empty directory that is not root will remove the path from the FS
    isDir(FS, P) and not isRoot(P) and childen(FS, P) == {} 
      => ((FS' where (not exists(FS', P) and not exists(descendents(FS', P))) , true) 
  
  
    # deleting a root path with children & recursive==true|false
    # removes the path and all descendents
    
    isDir(FS, P) and isRoot(P) and recursive  
      => (FS' where  not exists(descendents(FS', P)), true 
      
    # deleting a non-root path with children & recursive==true | false
    # removes the path and all descendents
    
    isDir(FS, P) and not isRoot(P) and recursive => 
     ( FS' where (not exists(FS', P) not exists(descendents(FS', P))): not parent(F,P))


* Deleting a file is an atomic action.

* Deleting an empty directory is atomic.

* A recursive delete of a directory tree SHOULD be atomic. (or MUST?)

* There's no consistent return code from an attempt to delete of the root directory


<not --  ============================================================= -->
<not --  METHOD: rename() -->
<not --  ============================================================= -->


### `FileSystem.rename(Path S, Path D)`

Rename includes the calculation of the destination path. 
If the destination exists and is a directory, the final destination
of the rename becomes the destination + the filename of the source path.
  
    D' := if (isDir(D) and Dnot =S) then (D :: filename(S)) else D.
  
#### Preconditions


    #src cannot be root (special case of previous condition)
    not isRoot(S)
  
    # src must exist
    # raise: FileNotFoundException
    exists(FS, S)
    
    
    #dest cannot be a descendent of src
    not isDescendent(S, D')
  
    #dest must be root, or have a parent that exists
    isRoot(FS, D') else exists(FS, parent(D'))
    
    #parent must not be a file 
    not isFile(FS, parent(D'))
    
    # a destination can be a file iff source == dest
    # raise FileAlreadyExistsException
    not isFile(FS, D') else S == D'
  
  

#### Postconditions



    #rename file to self is a no-op, returns true
    isFile(FS, S) and S==D' => (FS, true) 
    
    #renaming a dir to self is no op; return value is not specified
    # (posix => false, hdfs=> true)
    isDir(FS, S) and S==D' => (FS, ?)
  
    #renaming a file under dir adds file to dest dir, removes
    #old entry
    isFile(FS, S) and Snot =D' =>
      FS' where (not exists(FS', S) and isFile(FS', D') and data(FS', D') == data(FS, S))
  
    # for a directory the entire tree under S exists under D, while 
    # S and its descendents do not exist
    isDir(FS, D) and Snot =D' =>
      FS' where (
      (not exists(FS', S) 
        and isDir(FS', D')
        and forall C in descendents(FS, S) : not exists(FS', C)) 
        and forall C in descendents(FS, S) where isDir(C):
          exists C' in paths(FS) where isDir(C') 
          and childElements(D', C') == childElements(S, C)  
          and data(FS', C') == data(FS, C))
        )


#### Notes

* rename() MUST be atomic

* The behavior of `rename()` on an open file is unspecified.

* The return code of renaming a directory to itself is unspecified. 

#### HDFS specifics

1. Rename file over an existing file returns `(false, FS' == FS)`
1. Rename a file that does not exist returns `(false, FS' == FS)`



<not --  ============================================================= -->
<not --  METHOD: listStatus() -->
<not --  ============================================================= -->

### FileSystem.listStatus(Path P, PathFilter Filter) 

A `PathFilter` is a predicate function that returns true iff the path P
meets the filter's conditions.

#### Preconditions


    #path must exist
    exists(FS, P) else raise FileNotFoundException
  

#### Postconditions
  

    isFile(FS, P) and Filter(P) => [FileStatus(FS, P)]
    
    isFile(FS, P) and not Filter(P) => []
    
    isDir(FS, P) => [all C in children(FS, P) where Filter(C) == true] 
  


* After a file is created, all `listStatus()>` operations on the file and parent
  directory MUST find the file.

* After a file is deleted, all `listStatus()` operations on the file and parent
  directory MUST NOT find the file.

* By the time the `listStatus()` operation returns to the caller, there
is no guarantee that the information contained in the response is current.
The details MAY be out of date -including the contents of any directory, the
attributes of any files, and the existence of the path supplied. 

<not --  ============================================================= -->
<not --  METHOD: getFileBlockLocations() -->
<not --  ============================================================= -->

###  getFileBlockLocations(FileStatus F, int S, int L)
#### Preconditions

    S > 0  and L >= 0  else raise InvalidArgumentException
  

#### Postconditions
  


    F == null => null
    F not =null and F.getLen() <= S ==>  []
  


If the filesystem is location aware, it must return the list
of block locations where the data in the range (S, S+L ) can be found.

If the filesystem is not location aware, it SHOULD return

      [
        BlockLocation(["localhost:50010"] ,
                  ["localhost"],
                  ["/default/localhost"]
                   0, F.getLen())
       ] ;



* A bug in Hadoop 1.0.3 means that a topology path of the same number
of elements as the cluster topology MUST be provided, hence the 
`"/default/localhost"` path

* HDFS throws `HadoopIllegalArgumentException` for an invalid offset
or length; this extends `IllegalArgumentException`.

* There is no implicit check for the FileStatus referring to a 
directory. As `FileStatus.getLen()` for a directory is 0, it is
implictly returning []


  *REVIEW*: Action if `isDirectory(FS, P)` ? 
  
<not --  ============================================================= -->
<not --  METHOD: getFileBlockLocations() -->
<not --  ============================================================= -->

###  getFileBlockLocations(Path P, int S, int L)

#### Preconditions



    P not = null else raise NullPointerException
    exists(FS, P) else raise FileNotFoundException
  

#### Postconditions
  


    return getFileBlockLocations(getStatus(P), S, L)


###  getDefaultBlockSize(Path P), getDefaultBlockSize()

#### Preconditions

  

#### Postconditions


    return integer  >= 0 
  


Although there is no defined minimum value for this, as it
is used to partition work during job submission, a block size
that is too small will result in either too many jobs being submitted
for efficient work, or the `JobSubmissionClient` running out of memory.
Any FileSystem that does not break files into block sizes SHOULD
return a number for this that results in efficient processing. 
(it MAY make this user-configurable)


<not --  ============================================================= -->
<not --  METHOD: concat() -->
<not --  ============================================================= -->

### concat(Path T, Path Srcs[])

Joins multiple blocks together to create a single file. This
is a very under-implemented (and under-used) operation.
  
Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions


    Srcsnot =[] else raise IllegalArgumentException
    exists(FS, T)
    
    # all sources MUST be in the same directory
    forall S in Srcs: parent(S) == parent(T) 
      else raise IllegalArgumentException
    
    #HDFS: all block sizes to match the target
    forall S in Srcs: getBlockSize(FS, S) == getBlockSize(FS, T)
    
    #HDFS: no duplicate paths
    not  (exists P1, P2 in (Srcs+T) where P1==P2)
    
    #HFDS: All src files except the final one MUST be a complete block
    forall S in (Srcs[0..length(Srcs)-2] +T):
      (length(FS, S) % getBlockSize(FS, T)) == 0



#### Postconditions


    FS' where
     (data(FS', T) = data(FS, T) + data(FS, Srcs[0]) + ... + data(FS, Srcs[length(Srcs)-1]))
     and forall S in Srcs: not exists(FS', S)
   


HDFS's restrictions may be an implementation detail of how it implements
`concat` -by changing the inode references to join them together in 
a sequence.
  
<not --  ============================================================= -->
<not --  CLASS: FSDataOutputStream -->
<not --  INTERFACE: Seekable -->
<not --  ============================================================= -->

## FSDataOutputStream extends DataOutputStream implements Syncable

The specification of `DataOutputStream` is defined b   `java.io.DataOutputStream` 

    public interface Syncable {
      
      /** Flush out the data in client's user buffer. After the return of
       * this call, new readers will see the data.
       * @throws IOException if any error occurs
       */
      public void hflush() throws IOException;
      
      /** Similar to posix fsync, flush out the data in client's user buffer 
       * all the way to the disk device (but the disk may have it in its cache).
       * @throws IOException if error occurs
       */
      public void hsync() throws IOException;
    }



`FSDataOutputStream.hflush()` delegates to `OutputStream.flush()` unless
the stream it is wrapping implements `Syncable` -in which case it calls 
it.

`FSDataOutputStream.hsync()` delegates to `OutputStream.flush()` unless
the stream it is wrapping implements `Syncable`, in -which case it is called

  

<not --  ============================================================= -->
<not --  INTERFACE: Seekable -->
<not --  INTERFACE: InputStream -->
<not --  INTERFACE: PositionedReadable -->
<not --  ============================================================= -->

## InputStream, Seekable and PositionedReadable

### Invariants


1. After all operations on `Seekable`, `InputStream` and `PositionedReadable` of a file *F*, 
`length(F)` MUST be unchanged, and the contents of the file *F* must be unchanged.
 Metadata about the file (e.g. the access time) MAY change.

### Concurrency

1. All operations on an `InputStream` *S* are assumed to be thread-unsafe.

1. If the file which is being accessed is changed during a series
of operations, the outcome is not defined.

### InputStream

Implementations of `InputStream` MUST follow the Java specification of
`InputStream`. For an `InputStream` *S* created from
`FileSystem.open(F: Path): InputStream`, the following conditions MUST hold

1. After `S.close())` all `read` operations are SHOULD fail with
an exception. It MAY NOT, in which case the outcome of all further operations
are undefined.

1.`S.read()` where less than `length(F)` bytes have been served.
MUST return the next byte of data

1. `S.read()` where more than `length(F)` bytes have been served.
MUST return -1.

1. `S.read(Buffer, Offset, Len)`
where `Offset < 0 else  Offset >  Buffer.length else (Len > (Buffer.length - Offset)) else Len <0 MUST throw
`InvalidArgumentException` or another `RuntimeException`
including -but not limited to- `ArrayIndexOutOfBoundsException`

1. `S.read(null, int Offset, int Len )` MUST throw `NullPointerException`.

1. `S.read(Buffer,  Offset, 0 )` MUST  return 0 if all the preceding criteria have
been met.

1. `S.read(Buffer, Offset, Len)` MUST fill `Buffer` with `min(Len, length(F)-Len)` entries from
the input stream defined by `S.read()` and return the number of bytes written.

1. `S.read(Buffer, Offset, Len)` where S has already streamed `length(F)` bytes MUST return `-1`
and MUST make no changes to the contents of `Buffer`.

#### Notes:

1. the `InputStream` definition does not place any limit
on how long `read())` may take to complete.

### Seekable

The interface `Seekable` MAY be implemented by classes that extend
`InputStream` and MUST NOT be implemented that classes that do not.


    public interface Seekable {
      void seek(long pos) throws IOException;
      long getPos() throws IOException;
      boolean seekToNewSource(long targetPos) throws IOException;
    }


For an `InputStream` *S* created from
`FileSystem.open(F: Path): InputStream`, where `S instanceof Seekable`
is true and `S.seek()` does not throw an `UnsupportedOperation` exemption,
the following conditions MUST hold:-

1. For a newly opened input stream *S*, `S.getPos()` MUST
equal 0.

1. For all values of *P* in *Long* where `S.seek(P)` does 
throw an exception, the outcomes of `S.getPos()` and `S.read()`
operations are undefined.

1. After `S.close()`, `S.seek(P)` MUST fail with an `IOException`

1. For all values of *P1*, *P2* in *Long*,  `S.seek(P1)` followed
by `S.seek(P2)` is the equivalent of `S.seek(P2)`.
*this only holds if S.seek(P1) does not raise an exception -can we explicitly
declare that this elision can ignore that possibility?*

1. On `S.seek(P)` with *P < 0* an exception MUST be thrown.
It SHOULD be an `EOFException`. It MAY be an `IOException`, an `IllegalArgumentException `
or other `RuntimeException`.

1. `S.seek(0)` must succeed even if the file length is 0

1.  For all values of *P* in *Long* if `S.seek(P)` does not raise
an Exception, it is considered a successful seek.

1. For all successful `S.seek(P)` operations `S.getPos()` MUST equal <P>

1. This implies that a successful `S.seek(S.getPos())` does not
change the cursor position with in the file. This is considered a no-op
that MAY therefore be bypassed completely.

1. For all S, P where *P = S.getPos()* and *P < length(F)*,
after `S.read())` , `S.getPos()` MUST equal `P+1`.

1. After `S.seek(P)` with *P >= length(F)*, If an exception is not thrown 
then `S.read()` MUST equal -1. The value of `S.getPos()` MUST be unchanged.

1. If an exception is not thrown after `S.seek(P)` with *P >= length(F)*,
then `S.read(Buffer, Offset, Len)` MUST equal -1.
The value of `S.getPos()` MUST be unchanged.

1. On `S.seek(P)` with * P >length(F) *>,
if an `IOException` is not thrown, then `S.read()` and `S.read(byte[] buffer, int offset, int len )`
operation MUST return -1.

1. On `S.seek(P)` with *P  > length(F)*, if an `IOException` is not
thrown, `S.read(Buffer, Offset, Len )` operation MUST return -1, and the contents of *Buffer* unchanged.

1. After a `S.seek(P)` with `0<=P<length(file)`,
 `read(Buffer,0,1)` MUST set `Buffer[0]`
  to the byte at position `P` in the file,
 and `S.getPos()` MUST equal `P+1`
 
1. `S.seekToNewSource(Pos)` SHOULD return false if the
filesystem does not implement multiple-data sources for files.

1. After `S.seekToNewSource(Pos)` the value of `S.getPos()`
MUST be unchanged


Irrespective of whether or a `seekToNewSource(`) operation succeeds or fails,
the stream's `getPos()` value MUST be the value which it was before the seek
operation was invoked. Specifically, it is not a `seek()` operation, it is a
request to bind to a new location of data in expectation of a read or seek
operation fetching the new data.

## PositionedReadable

    public interface PositionedReadable {
      public int read(long position, byte[] buffer, int offset, int length)
        throws IOException;
      public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException;
      public void readFully(long position, byte[] buffer) throws IOException;
    }


1. The result *R* of `S.read(Pos, Buffer, Offset, Len )` MUST be identical
to the sequence


    P0 = S.getPos()
    try {
      S.seek(Pos)
      R = S.read( Buffer, Offset, Len )
    } finally {
      S.seek(P0)
    }
  
1. Degenerate case: The outcome of `S.read(S.getPos()), Buffer, Offset, Len )`
MUST be identical to that of the operation `S.read( Buffer, Offset, Len )`.

1. The outcome of `S.readFully(Pos, Buffer, Offset, Len )`
where `Pos + Len ` is less than the length of the file MUST be the same
as `S.read(Pos, Buffer, Offset, Len )`.

1. `S.readFully(Pos, Buffer, Offset, Len )`
where `Pos + Len ` is greater than the length of the file MUST raise an
`EOFException`.

1. `S.readFully(Pos, Buffer)` MUST be equivalent to `S.readFully(Pos, Buffer, 0, Buffer.length )`
    
  
<not --  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ -->



