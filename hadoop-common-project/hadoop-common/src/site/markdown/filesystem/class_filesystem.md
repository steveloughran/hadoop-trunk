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

# class org.apache.hadoop.fs.FileSystem

The abstract `FileSystem` class is the original class to access Hadoop filesystems;
non-abstract subclasses exist for all Hadoop-supported filesystems. 
  
All operations that take a Path to this interface MUST support relative paths.
In such a case, they must be resolved relative to the working directory
defined by `setWorkingDirectory()`. 

For all clients, therefore, we also add the notion of a state component PWD: 
this represents the present working directory of the client. Changes to this
state are not reflected in the filesystem itself -they are unique to the instance
of the client.

**Implementation Note**: the static `FileSystem get(URI uri, Configuration conf) ` method may return
a pre-existing instance of a filesystem client class - a class that may also be in use in other
threads. Current implementations of FileSystem *do not make any attempt to synchronize access
to the working directory field*. 

### Invariants

All the requirements of a valid filesystem are considered implicit preconditions and postconditions:
all operations on a valid filesystem MUST result in a new filesystem that is also valid


### Predicates and other state access operations


### boolean exists(Path p)


    def exists(FS, p) = p in paths(FS)
  

### boolean isDirectory(Path p) 

    def isDirectory(FS, p)= p in directories(FS)
  

### boolean isFile(Path p) 


    def isFile(FS, p) = p in files(FS)

###  isSymlink(Path p) boolean


    def isSymlink(FS, p) = p in symlinks(FS)
  

### FileStatus getFileStatus(Path p)

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


<!--  ============================================================= -->
<!--  METHOD: listStatus() -->
<!--  ============================================================= -->

### FileSystem.listStatus(Path, PathFilter ) 

A `PathFilter` `f` is a predicate function that returns true iff the path `p`
meets the filter's conditions.

#### Preconditions


path must exist

    if not exists(FS, p) : raise FileNotFoundException


#### Postconditions


    if isFile(FS, p) and f(p) :
        result = [getFileStatus(p)]

    elif isFile(FS, p) and not f(P) :
        result = []

    elif isDir(FS, p):
       result [c forall c in children(FS, p) where f(c) == True] 


* After a file is created, all `listStatus()>` operations on the file and parent
  directory MUST find the file.

* After a file is deleted, all `listStatus()` operations on the file and parent
  directory MUST NOT find the file.

* By the time the `listStatus()` operation returns to the caller, there
is no guarantee that the information contained in the response is current.
The details MAY be out of date -including the contents of any directory, the
attributes of any files, and the existence of the path supplied. 

* Atomicity and Consistency: the state of a directory may change during the evaluation
process. This may be reflected in a listing that is split between the pre-
and post- updated filesystem states.

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
of block locations where the data in the range (S, S+L ) can be found.


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
of elements as the cluster topology MUST be provided, hence the 
`"/default/localhost"` path


<!--  ============================================================= -->
<!--  METHOD: getFileBlockLocations() -->
<!--  ============================================================= -->

###  getFileBlockLocations(Path P, int S, int L)

#### Preconditions


    if p == null : raise NullPointerException
    if not exists(FS, p) :  raise FileNotFoundException


#### Postconditions

    result = getFileBlockLocations(getStatus(P), S, L)


###  getDefaultBlockSize(Path P), getDefaultBlockSize()

#### Preconditions



#### Postconditions


    result = integer  >= 0 



Although there is no defined minimum value for this result, as it
is used to partition work during job submission, a block size
that is too small will result in either too many jobs being submitted
for efficient work, or the `JobSubmissionClient` running out of memory.


Any FileSystem that does not actually break files into block SHOULD
return a number for this that results in efficient processing. 
(it MAY make this user-configurable)



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
files and symbolic links must hold; in `FS'` all 

The probe for the existence and type of a path and directory creation MUST be
atomic. The combined operation, including `mkdirs(parent(F))` MAY be atomic.

The return value is always true - even if
a new directory is not created. (this is defined in HDFS)


<!--  ============================================================= -->
<!--  METHOD: create() -->
<!--  ============================================================= -->

### FSDataOutputStream create(Path p, ...)


    FSDataOutputStream create(Path p,
          FsPermission permission,
          boolean overwrite,
          int bufferSize,
          short replication,
          long blockSize,
          Progressable progress) throws IOException;


#### Preconditions

    # file must not exist for a no-overwrite create
    if not overwrite and isFile(FS, p)  : raise FileAlreadyExistsException
    
    #writing to or overwriting a directory must fail.    
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
       FS'.Files[p] == []
       ancestors(p) is-subset-of FS'.Directories 
       
    result= FSDataOutputStream
  
The updated (valid) filesystem must contains all the parent directories of the path, as created by `mkdirs(parent(p))`.

The result is `FSDataOutputStream`, which through its operations may generate new filesystem states with updated values of
`FS.Files[p]`


* S3N, Swift and other blobstores do not currently change the FS state
until the output stream `close()` operation is completed.
This MAY be a bug, as it allows >1 client to create a file with overwrite=false, and potentially confuse file/directory logic

* Local FS raises a `FileNotFoundException` when trying to create a file over
a directory, hence it is is listed as a possible exception to raise
in this situation.

* Not covered: symlinks. The resolved path of the symlink is used as the final path argument to the `create()` operation

<!--  ============================================================= -->
<!--  METHOD: append() -->
<!--  ============================================================= -->

### FSDataOutputStream append(Path p, int bufferSize, Progressable progress)

Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    if not exists(FS, p) : raise FileNotFoundException
    
    if not isFile(FS, p)) : raise [FileNotFoundException, IOException]

#### Postconditions
  
    FS
    result = FSDataOutputStream

Return: `FSDataOutputStream`, which can update the entry FS.Files[p] by appending data to the existing list

  

<!--  ============================================================= -->
<!--  METHOD: open() -->
<!--  ============================================================= -->

### FSDataInputStream open(Path f, int bufferSize)

Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    if not isFile(FS, p)) : raise [FileNotFoundException, IOException]


#### Postconditions
  
    result = FSDataInputStream
  
The result provides access to the byte array defined by `FS.Files[p]`; whether that 
access is to the contents at the time the `open()` operation was invoked, 
or whether and how it may pick up changes to that data in later states of FS is
an implementation detail.
  
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
 
    if isDir(FS, p) and not recursive and (childen(FS, p) != {}) : raise IOException


#### Postconditions

return false if file does not exist; FS state does not change
    
    if not exists(FS, p):
        FS' = FS
        result = False
    

a path referring to a file is removed, return value: true

    if isFile(FS, p) :
        FS' = (FS.Directories, FS.Files - p, FS.Symlinks)
        result = True
  

deleting an empty root does not change the filesystem state
and may return true or false
  
    if isDir(FS, p) and isRoot(p) and childen(FS, p) == {} :
        FS ' = FS
        result = (undetermined)
  
Deleting an empty directory that is not root will remove the path from the FS and
return true

    if isDir(FS, p) and not isRoot(p) and childen(FS, p) == {} :
        FS' = (FS.Directories - [p], FS.Files, FS.Symlinks)
        result = True 
  
  
Deleting a root path with children and recursive==true removes all descendants

    if isDir(FS, p) and isRoot(p) and recursive :
        FS' where forall d in descendants(FS, p):
            not isDir(FS', d)
            and not isFile(FS', d)
            and not isSymlink(FS', d)
        result = True 

Deleting a non-root path with children & recursive==true  removes the path and all descendants

    if isDir(FS, p) and not isRoot(p) and recursive :
        FS' where:
            not isDir(FS', p)
            and forall d in descendants(FS, p):
                not isDir(FS', d)
                not isFile(FS', d)
                not isSymlink(FS', d)
        result = True 


* Deleting a file is an atomic action.

* Deleting an empty directory is atomic.

* A recursive delete of a directory tree SHOULD be atomic. (or MUST?)

* There's no consistent return code from an attempt to delete of the root directory


<!--  ============================================================= -->
<!--  METHOD: rename() -->
<!--  ============================================================= -->


### `FileSystem.rename(Path s, Path d)`

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

  
`dest` cannot be a descendant of `s`

    if isDescendant(FS, s, dest) : raise IOException

This implicitly covers the special case of `isRoot(FS, s)`  
  
`dest` must be root, or have a parent that exists

    if not isRoot(FS, dest) and not exists(FS, parent(dest)) : raise IOException
  
The parent path of a destination must not be a file 

    if isFile(FS, parent(dest)) : raise IOException

This implicitly covers all the ancestors of the parent.



** Should **

A destination can only be a file iff `s == dest`

    if isFile(FS, dest) and not s == dest : raise IOException
  
 ** HDFS Behavior*: This check does not take place, instead the rename is [considered a failure](#hdfs-rename)

#### Postconditions


Renaming a directory to itsself is no op; return value is not specified

In posix the result is false;  hdfs returns true

    if isDir(FS, s) and s == dest :
        FS' = FS
        result = (undefined)


 rename file to self is a no-op, returns true

     if isFile(FS, s) and s == dest :
         FS' = FS
         result = True 


 <a name="hdfs-rename"></a>** HDFS Behavior**: SHOULD NOT
 
 Rename file a over an existing file where `s != dest` results in

             FS' = FS
             result = False


 This situation arises iff attempts to rename a file onto a different file are not considered
 a failure of the preconditions -and so raise an error.

Renaming a file under a directory adds the file as a child of the dest dir, retaining the filename
of the source; the old entry is no longer present
 
    if isFile(FS, s) and s != dest: 
        FS' where:
            FS'.Files = FS.Files - s + { dest: FS.Files[src]}

A more declarative form of the postcondition would be:
  
      not exists(FS', s) and data(FS', dest) == data(FS, s)

  
For a directory the entire tree under `s` will then exist under `dest`, while the path
`s` and its descendants do not exist.

    if isDir(FS, s) isDir(FS, dest) and s != dest :
        FS' where:
            not exists(FS', s)
            and dest in FS'.Directories
            and forall c in descendants(FS, s) :
                not exists(FS', c)) 
            and forall c in descendants(FS, s) where isDir(FS, c):
                isDir(FS', dest + childElements(s, c)
            and forall c in descendants(FS, s) where not isDir(FS, c):
                    data(FS', dest + childElements(s, c)) == data(FS, c)
        result = True


#### Notes

* rename() MUST be atomic

* The behavior of `rename()` on an open file is unspecified: whether it is allowed, what happens to later attempts to read from or write to the open stream

* The return code of renaming a directory to itself is unspecified. 

#### HDFS specifics

Renaming a source file that does not exist returns

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
  


