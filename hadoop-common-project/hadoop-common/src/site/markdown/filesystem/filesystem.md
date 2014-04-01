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
  

<!--  ============================================================= -->
<!--  CLASS: FileSystem -->
<!--  ============================================================= -->

# class `org.apache.hadoop.fs.FileSystem`

The abstract `FileSystem` class is the original class to access Hadoop filesystems;
non-abstract subclasses exist for all Hadoop-supported filesystems. 
  
All operations that take a Path to this interface MUST support relative paths.
In such a case, they must be resolved relative to the working directory
defined by `setWorkingDirectory()`. 

For all clients, therefore, we also add the notion of a state component PWD: 
this represents the present working directory of the client. Changes to this
state are not reflected in the filesystem itself -they are unique to the instance
of the client.

**Implementation Note**: the static `FileSystem get(URI uri, Configuration conf) ` method MAY return
a pre-existing instance of a filesystem client class - a class that may also be in use in other threads. The implementations of `FileSystem` which ship with Apache Hadoop *do not make any attempt to synchronize access to the working directory field*. 

### Invariants

All the requirements of a valid filesystem are considered implicit preconditions and postconditions:
all operations on a valid filesystem MUST result in a new filesystem that is also valid


### Predicates and other state access operations


### `boolean exists(Path p)`


    def exists(FS, p) = p in paths(FS)
  

### `boolean isDirectory(Path p)` 

    def isDirectory(FS, p)= p in directories(FS)
  

### `boolean isFile(Path p)` 


    def isFile(FS, p) = p in files(FS)

###  `boolean isSymlink(Path p)`


    def isSymlink(FS, p) = p in symlinks(FS)
  

### `FileStatus getFileStatus(Path p)`

Get the status of a path

#### Preconditions

  
    if not exists(FS, p) : raise FileNotFoundException

#### Postconditions


    result = stat: FileStatus where:
        if isFile(FS, p) :
            stat.length = len(FS.Files[p])
            stat.isdir = False
        elif isDir(FS, p) :
            stat.length = 0
            stat.isdir = True
        elif isSymlink(FS, p) :
            stat.length = 0
            stat.isdir = false
            stat.symlink = FS.Symlinks[p]

### Path getHomeDirectory()

The function `getHomeDirectory` returns the home directory for the Filesystem 
and the current user account.

For some filesystems, the path is `["/","users", System.getProperty("user-name")]`.

However, for HDFS, the username is derived from the credentials used to authenticate the client with HDFS -this
may differ from the local user account name.

*It is the responsibility of the filesystem to determine actual the home directory
of the caller*


#### Preconditions


#### Postconditions

    result = p where valid-path(FS, p)

There is no requirement that the path exists at the time the method was called,
or, if it exists, that it points to a directory. However, code tends to assume
that `not isFile(FS, getHomeDirectory())` holds to the extent that follow-on
code may fail.


<!--  ============================================================= -->
<!--  METHOD: listStatus() -->
<!--  ============================================================= -->

### `FileSystem.listStatus(Path, PathFilter )` 

A `PathFilter` `f` is a predicate function that returns true iff the path `p`
meets the filter's conditions.

#### Preconditions

Path must exist

    if not exists(FS, p) : raise FileNotFoundException

#### Postconditions


    if isFile(FS, p) and f(p) :
        result = [getFileStatus(p)]

    elif isFile(FS, p) and not f(P) :
        result = []

    elif isDir(FS, p):
       result [getFileStatus(c) forall c in children(FS, p) where f(c) == True] 


**Implicit invariant**: the contents of a `FileStatus` of a child retrieved
via `listStatus()` are equal to those from a call of `getFileStatus()`
to the same path:

    forall fs in listStatus(Path) :
      fs == getFileStatus(fs.path)


### Atomicity and Consistency

By the time the `listStatus()` operation returns to the caller, there
is no guarantee that the information contained in the response is current.
The details MAY be out of date -including the contents of any directory, the
attributes of any files, and the existence of the path supplied. 

The state of a directory MAY change during the evaluation
process. This may be reflected in a listing that is split between the pre-
and post- updated filesystem states.


* After an entry at path `P` is created, and before any other
 changes are made to the filesystem, `listStatus(P)` MUST
find the file and return its status.

* After an entry at path `P` is deleted, `listStatus(P)`  MUST
raise a `FileNotFoundException`.

* After an entry is path `P` is created, and before any other
 changes are made to the filesystem, the result of `listStatus(parent(P))` SHOULD
include the value of `getFileStatus(P)`.

* After an entry is path `P` is created,  nd before any other
 changes are made to the filesystem, the result of `listStatus(parent(P))` SHOULD
NOT include the value of `getFileStatus(P)`.


This is not a theoretical possibility, it is observable in HDFS when a
directory contains many thousands of files.

Consider a directory "d" with the contents

	a
	part-0000001
	part-0000002
	...
	part-9999999
	
	
If the number of files is such that HDFS returns a partial listing in each
response, then, if a listing `listStatus("d")` takes place concurrently with the operation
`rename("d/a","d/z"))`, the result may be one of 

	[a, part-0000001, ... , part-9999999]
	[part-0000001, ... , part-9999999, z]

	[a, part-0000001, ... , part-9999999, z]
	[part-0000001, ... , part-9999999]

While this situation is likely to be a rare occurrence, it MAY happen. In HDFS
these inconsistent views are only likely when listing a directory with many children.

Other filesystems may have stronger consistency guarantees, or return inconsistent
data more readily.



<!--  ============================================================= -->
<!--  METHOD: getFileBlockLocations() -->
<!--  ============================================================= -->

### ` List[BlockLocation] getFileBlockLocations(FileStatus f, int s, int l)`
#### Preconditions

    if s <= 0 or l <= 0 : raise {HadoopIllegalArgumentException, InvalidArgumentException}

* HDFS throws `HadoopIllegalArgumentException` for an invalid offset
or length; this extends `IllegalArgumentException`.

#### Postconditions

If the filesystem is location aware, it must return the list
of block locations where the data in the range `[s:s+l]` can be found.


    if f == null :
        result = null
    elif f.getLen()) <= s
        result = []
    else result = [ locations(FS, b) for all b in blocks(FS, p, s, s+l)]

where

      def locations(FS, b) = a list of all locations of a block in the filesystem

      def blocks(FS, p, s, s +  l)  = a list of the blocks containing  data(FS, path)[s:s+l]


Note that that as `length(FS, f) ` is defined as 0 if `isDir(FS, f)`, the result of `getFileBlockLocations()` on a directory is []


If the filesystem is not location aware, it SHOULD return

      [
        BlockLocation(["localhost:50010"] ,
                  ["localhost"],
                  ["/default/localhost"]
                   0, F.getLen())
       ] ;


*A bug in Hadoop 1.0.3 means that a topology path of the same number
of elements as the cluster topology MUST be provided, hence Filesystems SHOULD
return that `"/default/localhost"` path


<!--  ============================================================= -->
<!--  METHOD: getFileBlockLocations() -->
<!--  ============================================================= -->

###  `getFileBlockLocations(Path P, int S, int L)`

#### Preconditions


    if p == null : raise NullPointerException
    if not exists(FS, p) :  raise FileNotFoundException


#### Postconditions

    result = getFileBlockLocations(getStatus(P), S, L)


###  `getDefaultBlockSize()`

#### Preconditions




#### Postconditions


    result = integer  >= 0 

Although there is no defined minimum value for this result, as it
is used to partition work during job submission, a block size
that is too small will result in either too many jobs being submitted
for efficient work, or the `JobSubmissionClient` running out of memory.


Any FileSystem that does not actually break files into block SHOULD
return a number for this that results in efficient processing. 
(it MAY make this user-configurable -the S3 and Swift filesystem clients do this)

###  `getDefaultBlockSize(Path P)`

#### Preconditions


#### Postconditions


    result = integer  >= 0 

The outcome of this operation is usually identical to `getDefaultBlockSize()`,
with no checks for the existence of the given path. 

Filesystems that support mount points may have different default values for
different paths, in which case the specific default value for the destination path
SHOULD be returned.


###  `getBlockSize(Path P)`

#### Preconditions

    if not exists(FS, p) :  raise FileNotFoundException


#### Postconditions


    result == getFileStatus(P).getBlockSize()

The outcome of this operation MUST be identical to that  contained in
the `FileStatus` returned from `getFileStatus(P)`.


## State Changing Operations  
 
### `boolean mkdirs(Path p, FsPermission permission )`

Create a directory and all its parents

#### Preconditions
 
 
     if exists(FS, p) and not isDir(FS, p) :
         raise [ParentNotDirectoryException, FileAlreadyExistsException, IOException]
     
 
#### Postconditions
 
   
    FS' where FS'.Directories' = FS.Directories + [p] + ancestors(FS, p)  
    result = True

  
The condition exclusivity requirement of a filesystem's directories,
files and symbolic links must hold.

The probe for the existence and type of a path and directory creation MUST be
atomic. The combined operation, including `mkdirs(parent(F))` MAY be atomic.

The return value is always true - even if
a new directory is not created. (this is defined in HDFS)


<!--  ============================================================= -->
<!--  METHOD: create() -->
<!--  ============================================================= -->

### `FSDataOutputStream create(Path, ...)`


    FSDataOutputStream create(Path p,
          FsPermission permission,
          boolean overwrite,
          int bufferSize,
          short replication,
          long blockSize,
          Progressable progress) throws IOException;


#### Preconditions

File must not exist for a no-overwrite create
  
    if not overwrite and isFile(FS, p)  : raise FileAlreadyExistsException
  
Writing to or overwriting a directory must fail.    

    if isDir(FS, p) : raise {FileAlreadyExistsException, FileNotFoundException, IOException}
  

Filesystems may reject the request for other
reasons -such as the FS being read-only  (HDFS), 
the block size being below the minimum permitted (HDFS),
the replication count being out of range (HDFS),
quotas on namespace or filesystem being exceeded, reserved
names, ...etc. All rejections SHOULD be `IOException` or a subclass thereof
and MAY be a `RuntimeException` or subclass. (HDFS may raise: `InvalidPathException`)

#### Postconditions

    FS' where :
       FS'.Files'[p] == []
       ancestors(p) is-subset-of FS'.Directories' 
       
    result = FSDataOutputStream
  
The updated (valid) filesystem must contains all the parent directories of the path, as created by `mkdirs(parent(p))`.

The result is `FSDataOutputStream`, which through its operations may generate new filesystem states with updated values of
`FS.Files[p]`


* S3N, Swift and other Object Stores do not currently change the FS state
until the output stream `close()` operation is completed.
This MAY be a bug, as it allows >1 client to create a file with overwrite=false, and potentially confuse file/directory logic

* Local FS raises a `FileNotFoundException` when trying to create a file over
a directory, hence it is is listed as a possible exception to raise
in this situation.

* Not covered: symlinks. The resolved path of the symlink is used as the final path argument to the `create()` operation

<!--  ============================================================= -->
<!--  METHOD: append() -->
<!--  ============================================================= -->

### `FSDataOutputStream append(Path p, int bufferSize, Progressable progress)`

Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException
    
    if not isFile(FS, p) : raise [FileNotFoundException, IOException]

#### Postconditions
  
    FS
    result = FSDataOutputStream

Return: `FSDataOutputStream`, which can update the entry `FS.Files[p]`
by appending data to the existing list

  
<!--  ============================================================= -->
<!--  METHOD: open() -->
<!--  ============================================================= -->

### `FSDataInputStream open(Path f, int bufferSize)`

Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    if not isFile(FS, p)) : raise [FileNotFoundException, IOException]

This is a critical precondition. Implementations of some FileSystems (e.g.
Object stores) could shortcut one round trip by postponing their HTTP GET
operation until the first `read()` on the returned `FSDataInputStream`.
However, much client code does depend on the existence check being performed
at the time of the `open()` operation -implementations MUST check for the 
presence of the file at the time of creation. (This does not imply that
the file and its data is still at the time of the following read()` or
any successors)

#### Postconditions
  
    result = FSDataInputStream(0, FS.Files[p])
  
The result provides access to the byte array defined by `FS.Files[p]`; whether that 
access is to the contents at the time the `open()` operation was invoked, 
or whether and how it may pick up changes to that data in later states of FS is
an implementation detail.

The result MUST be the same for local and remote callers of the operation.
  
  
#### HDFS implementation notes

1. MAY throw `UnresolvedPathException` when attempting to traverse
symbolic links

1. throws `IOException("Cannot open filename " + src)` if the path
exists in the metadata, but no copies of any its blocks can be located;
-`FileNotFoundException` would seem more accurate and useful.


  

<!--  ============================================================= -->
<!--  METHOD: delete() -->
<!--  ============================================================= -->

### `FileSystem.delete(Path P, boolean recursive)`

#### Preconditions

A directory with children and recursive == false cannot be deleted
 
    if isDir(FS, p) and not recursive and (children(FS, p) != {}) : raise IOException


#### Postconditions


##### Nonexistent path

If the file does not exist the FS state does not change
    
    if not exists(FS, p):
        FS' = FS
        result = False

The result SHOULD be `False, indicating that no file was deleted.


##### Simple File
    

A path referring to a file is removed, return value: `True`

    if isFile(FS, p) :
        FS' = (FS.Directories, FS.Files - [p], FS.Symlinks)
        result = True
  

##### Empty root directory

Deleting an empty root does not change the filesystem state
and may return true or false
  
    if isDir(FS, p) and isRoot(p) and children(FS, p) == {} :
        FS ' = FS
        result = (undetermined)

There's no consistent return code from an attempt to delete the root directory

##### Empty (non-root) directory

Deleting an empty directory that is not root will remove the path from the FS and
return true

    if isDir(FS, p) and not isRoot(p) and children(FS, p) == {} :
        FS' = (FS.Directories - [p], FS.Files, FS.Symlinks)
        result = True 
  

##### Recursive delete of root directory

Deleting a root path with children and `recursive==True`
 can do one of two things

The Unix/Posix model assumes that if the user has
the correct permissions to delete everything, 
they are free to do so -resulting in an empty filesystem

    if isDir(FS, p) and isRoot(p) and recursive :
        FS' = ({["/"]}, {}, {}, {})
        result = True 
        
In contrast, HDFS never permits the deletion of the root of a filesystem; the
filesystem be taken offline and reformatted if an empty
filesystem is desired.

    if isDir(FS, p) and isRoot(p) and recursive :
        FS ' = FS
        result = False 

##### Recursive delete of non-root directory

Deleting a non-root path with children `recursive==true` 
removes the path and all descendants

    if isDir(FS, p) and not isRoot(p) and recursive :
        FS' where:
            not isDir(FS', p)
            and forall d in descendants(FS, p):
                not isDir(FS', d)
                not isFile(FS', d)
                not isSymlink(FS', d)
        result = True



#### Atomicity

* Deleting a file MUST be an atomic action.

* Deleting an empty directory MUST be an atomic action.

* A recursive delete of a directory tree MUST be atomic.




<!--  ============================================================= -->
<!--  METHOD: rename() -->
<!--  ============================================================= -->


### `FileSystem.rename(Path s, Path d)`

In terms of its specification, `rename()` is one of the most complex operations within a filesystem .

In terms of its implementation, it is the one with the most ambiguity regarding when to return false
versus raise an exception.

Rename includes the calculation of the destination path. 
If the destination exists and is a directory, the final destination
of the rename becomes the destination + the filename of the source path.
  
    let dest = if (isDir(FS, s) and d != s) : 
            d + [filename(s)]
        else :
            d
  
#### Preconditions


source `s` must exist

    if not exists(FS, s) : raise FileNotFoundException

** HDFS does not fail here -it returns false from the operation**
  
`dest` cannot be a descendant of `s`

    if isDescendant(FS, s, dest) : raise IOException

This implicitly covers the special case of `isRoot(FS, s)`  
  
`dest` must be root, or have a parent that exists

    if not isRoot(FS, dest) and not exists(FS, parent(dest)) : raise IOException
  
The parent path of a destination must not be a file 

    if isFile(FS, parent(dest)) : raise IOException

This implicitly covers all the ancestors of the parent.



** Should **

A destination can only be a file if `s == dest`

    if isFile(FS, dest) and not s == dest : raise IOException
  
* HDFS Behavior: This check does not take place, instead the rename is [considered a failure](#hdfs-rename)
 
* Local Filesystem : the rename succeeds

#### Postconditions


##### Renaming a directory to self

Renaming a directory to itself is no-op; return value is not specified

In Posix the result is `False`;  in HDFS the result is `True`

    if isDir(FS, s) and s == dest :
        FS' = FS
        result = (undefined)


##### Renaming a file to self

 rename file to self is a no-op; the result is `True`

     if isFile(FS, s) and s == dest :
         FS' = FS
         result = True 


##### <a name="hdfs-rename">Renaming a file to self: HDFS</a>
 
In HDFS, a rename where the destination is a file such that `s != dest` results in

     FS' = FS
     result = False

That is: HDFS does not raise an `IOException`, merely rejects the request and returns false.

##### Renaming a file onto a directory

Renaming a file where the destination is a directory moves the file as a child of the destination directory, retaining the filename element of the source path.
 
    if isFile(FS, s) and s != dest: 
        FS' where:
            FS'.Files = FS.Files - s + { dest: FS.Files[src]}

A more declarative form of the postcondition would be:
  
      not exists(FS', s) and data(FS', dest) == data(FS, s)

##### Renaming a directory onto a directory
  
For a directory the entire tree under `s` will then exist under `dest`, while the path
`s` and its descendants do not exist.

    if isDir(FS, s) isDir(FS, dest) and s != dest :
        FS' where:
            not exists(FS', s)
            and dest in FS'.Directories]
            and forall c in descendants(FS, s) :
                not exists(FS', c)) 
            and forall c in descendants(FS, s) where isDir(FS, c):
                isDir(FS', dest + childElements(s, c)
            and forall c in descendants(FS, s) where not isDir(FS, c):
                    data(FS', dest + childElements(s, c)) == data(FS, c)
        result = True


#### Notes

* The core operation of `rename()` -moving one entry in the filesystem to
another MUST be atomic -some applications rely on this as a way to co-ordinate access to data.

* Some FileSystem implementations perform checks on the destination
filesystem before and after the rename -the `ChecksumFileSystem` used to provide checksummed access to local data is an example of this. The entire sequence MAY NOT be atomic.


* The behavior of `rename()` on an open file is unspecified: whether it is
allowed, what happens to later attempts to read from or write to the open stream

* The return code of renaming a directory onto itself is unspecified. 

#### HDFS specifics

Renaming a source file that does not exist is not an exception -it simply returns `False`

    FS' = FS
    result = false


<!--  ============================================================= -->
<!--  METHOD: concat() -->
<!--  ============================================================= -->

### concat(Path p, Path sources[])

Joins multiple blocks together to create a single file. This
is a very under-implemented (and under-used) operation.
  
Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions


    if not exists(FS, p) : raise FileNotFoundException

    if sources==[] : raise IllegalArgumentException
  
all sources MUST be in the same directory
  
    for s in sources: if parent(S) != parent(p) raise IllegalArgumentException

All block sizes must match that of the target

    for s in sources: getBlockSize(FS, S) == getBlockSize(FS, p)

No duplicate paths
    
    not (exists p1, p2 in (sources + [p]) where p1 == p2)
    

HFDS: All src files except the final one MUST be a complete block

    for s in (sources[0:length(sources)-1] + [p]):
      (length(FS, s) mod getBlockSize(FS, p)) == 0



#### Postconditions


    FS' where:
     (data(FS', T) = data(FS, T) + data(FS, sources[0]) + ... + data(FS, srcs[length(srcs)-1]))
     and for s in srcs: not exists(FS', S)
   

HDFS's restrictions may be an implementation detail of how it implements
`concat` -by changing the inode references to join them together in 
a sequence. A no other filesystem in the Hadoop core codebase
implements this method, there is no way to distinguish implementation detail.
from specification.
  


