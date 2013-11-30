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
  
All operations that take a Path to this interface MUST support relative paths.
In such a case, they must be resolved relative to the working directory
defined by `setWorkingDirectory()`.


### boolean exists(Path p)


    exists(FS, p)
  

### boolean isDirectory(Path p) 

    exists(FS, p) and isDir(FS, p)
  

### boolean isFile(Path p) 


    exists(FS, p) and isFile(FS, p)
  

###  isSymlink(Path p) boolean


    exists(FS, p) and isSymlink(FS, p)
  

### FileStatus getFileStatus(Path p)

#### Preconditions

  
    if not exists(FS, p) : raise FileNotFoundException

#### Postconditions


    FS' = FS
    result = FileStatus(length(p), isDirectory(p), [metadata], p)) 
  
 <!--  ============================================================= -->
 <!--  METHOD: mkdirs() -->
 <!--  ============================================================= -->
 
### `boolean mkdirs(Path p, FsPermission permission )`

#### Preconditions
 
 
     if isFile(p) :
         raise {IOException, ParentNotDirectoryException, FileAlreadyExistsException}
     
 
#### Postconditions
 

   
     FS' where FS'.Directories' = FS.Directories + [pe foreach pe in p]  
     result = true
 
 The condition exclusivity requirement of a filesystem's directories,
 files and symbolic links myst hold, 
 
 The probe for the existence and type of a path and directory creation MUST be
 atomic. The combined operation, including `mkdirs(parent(F))` MAY be atomic.
 
 The return value is always true - even if
 a new directory is not created. (this is defined in HDFS)




<!--  ============================================================= -->
<!--  METHOD: create() -->
<!--  ============================================================= -->

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

<!--  ============================================================= -->
<!--  METHOD: append() -->
<!--  ============================================================= -->

### FSDataOutputStream append(Path P, int bufferSize, Progressable progress)

Implementations MAY throw `UnsupportedOperationException`
  
#### Preconditions

    exists(FS, P) else raise FileNotFoundException
    isFile(FS, P)) else raise FileNotFoundException or IOException

#### Postconditions
  

Return: `FSDataOutputStream`, where `FSDataOutputStream.write(byte)`,
will, after any flushing, syncing and committing, add `byte`
to the tail of the list returned by `data(FS, P)`.
  

<!--  ============================================================= -->
<!--  METHOD: open() -->
<!--  ============================================================= -->

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
  

<!--  ============================================================= -->
<!--  METHOD: delete() -->
<!--  ============================================================= -->

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


<!--  ============================================================= -->
<!--  METHOD: rename() -->
<!--  ============================================================= -->


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



<!--  ============================================================= -->
<!--  METHOD: listStatus() -->
<!--  ============================================================= -->

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

<!--  ============================================================= -->
<!--  METHOD: getFileBlockLocations() -->
<!--  ============================================================= -->

###  getFileBlockLocations(FileStatus F, int S, int L)
#### Preconditions

    S > 0  and L >= 0  else raise InvalidArgumentException
  

#### Postconditions
  


    FS
    
    if (F == null => null
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
  
<!--  ============================================================= -->
<!--  METHOD: getFileBlockLocations() -->
<!--  ============================================================= -->

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


<!--  ============================================================= -->
<!--  METHOD: concat() -->
<!--  ============================================================= -->

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
  


