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
<!--  CLASS: FSDataInputStream -->
<!--  ============================================================= -->
  
  
  
  
# Class `FSDataInputStream extends DataInputStream`
  
The core behavior of `FSDataInputStream` is defined by `java.io.DataInputStream`,
with extensions that add key assumptions to the system

1. The source is a local or remote filesystem.
1. The stream being read references a finite array of bytes.
1. The length of the data does not change during the read process.
1. The contents of the data does not change during the process.
1. The source file remains present during the read process
1. Callers may use `Seekable.seek()` to positions within this array, with future
reads starting at this offset.
1. The cost of a forward or backwards seek is low.


Files are opened via `FileSystem.open(p)`, which, if successful, returns:

    result = FSDataInputStream(0, FS.Files[p])


The stream can be modeled as


    FSDIS = (pos, data[], isOpen)

with access functions
    
    pos(FDIS) 
    data(FDIS)
    isOpen(FDIS)

### `Closeable.close()`

The semantics of `java.io.Closeable` are defined in the interface definition
within the JRE.

The operation MUST be idempotent; the following sequence is not an error

    FSDIS.close();
    FSDIS.close();

Implementations SHOULD NOT raise `IOException` exceptions (or any other exception)
during this operation -client applications often ignore these, or may fail
unexpectedly.

#### Preconditions

    isOpen(FDIS) else raise IOException
    pos < len(data) else raise [EOFException, IOException]
     
#### Postconditions
    
     
    FSDIS' = (undefined), (undefined), false)
       
   

### `Seekable.getPos()`

Return the current position. The outcome when a stream is closed is undefined.

#### Preconditions

    isOpen(FDIS)
         
#### Postconditions
    
        
    result = pos(FSDIS)  
        

### `InputStream.read()`

Return the data at the current position. 

1. Implementations should fail When a stream is closed
1. There is no limit on how long `read())` may take to complete.

#### Preconditions

    isOpen(FDIS)
    pos < len(data) else raise [EOFException, IOException]
     
#### Postconditions
    
    
    if ( pos < len(data) ):
       FSDIS' = (pos+1, data, true)
       result = data[pos]
    else
        result = -1
        



### `InputStream.read(buffer[], offset, length)`

Read `length` bytes of data into the destination buffer, starting at offset
`offset`

#### Preconditions

    isOpen(FDIS)
    buffer != null else raise NullPointerException
    length >= 0 
    offset < len(buffer)
    length <= len(buffer) - offset
                           
Exceptions raised on precondition failure are                           
                           
    InvalidArgumentException
    ArrayIndexOutOfBoundsException
    RuntimeException

#### Postconditions
    
    if length==0 :
      result = 0
     
    elseif pos > len(data):
      result -1
    
    else
      let l = min(length, len(data)-length) : 
          buffer' = buffer where forall i in [0..l-1]:
              buffer'[o+i] = data[pos+i]
          FDIS' = (pos+l, data, true)
          result = l
    


### `Seekable.seek(s)`


#### Preconditions

Not all subclasses implement the Seek operation:

    supported(FDIS, Seekable.seek) else raise [UnsupportedOperation, IOException]

If the operation is supported, the file SHOULD be open:

    isOpen(FDIS)

Some filesystems do not perform this check, relying the `read()` contract
to reject reads on a closed stream. (RawLocalFileSystem)

Seek to offset 0 must always succeed, or the seek position must be less
than the length of the data:


    (s==0) or (s < len(data))) else raise [EOFException, IOException]

Some FileSystems do not raise an exception if this condition is not met. They
instead return -1 on any `read()` operation where, at the time of the read,
`len(data(FSDIS)) < pos(FSDIS)`.

#### Postconditions
    
    FDIS' = (s, data, true)
    

### `Seekable.seekToNewSource(offset)`

This operation instructs the source to retrieve `data[]` from a different
source from the current source. This is only relevant if the filesystem supports
multiple replicas of a file -and there is currently >1 valid replica of the
data at offset `offset`.


#### Preconditions

Not all subclasses implement the operation operation, and instead
either raise an exception -or return `False`.

    supported(FDIS, Seekable.seekToNewSource) else raise [UnsupportedOperation, IOException]

Examples: `CompressionInputStream` , `HttpFSFileSystem`

If supported, the file must be open

    isOpen(FDIS)   

#### Postconditions

The majority of subclasses that do not implement this operation simply
fail. 

    if not supported(FDIS, Seekable.seekToNewSource(s)):
        result = False
    
Examples: `RawLocalFileSystem` , `HttpFSFileSystem`

If the operation is supported and there is a new location for the data:
    
        FSDIS' = (pos, data', true)
        result = True

The new data is the original data (or an updated version of it, as covered
in the Consistency section below) -but the block containing the data at `offset`
sourced from a different replica.

If there is no other copy, `FSDIS` is  not updated; the response indicates this

        result = False

Outside of test methods, the primary use of this method is in the {{FSInputChecker}}
class, which can react to a checksum error in a read by attempting to source
the data elsewhere. It a new source can be found it attempts to reread and
recheck that portion of the file.

  
## Consistency 

* All readers, local and remote of a data stream FSDIS provided from a `FileSystem.open(p)`
are expected to receive access to the data `FS.Files[p]` at the time of opening.
* If the underlying data is changed during the read process, these changes MAY or
MAY NOT be visible.
* Such changes are visible MAY be partially visible. 


At time t0
 
    FSDIS0 = FS'read(p) = (0, data0[])
  
At time t1

    FS' = FS` where FS'.Files[p] = data1

From time `t >= t1`, the value of `FSDIS0` is undefined.
   
It may be unchanged

    FSDIS0.data == data0
    
    forall l in len(FSDIS0.data):
      FSDIS0.read() == data0[l]
   
   
It may pick up the new data

    FSDIS0.data == data1
   
    forall l in len(FSDIS0.data):
      FSDIS0.read() == data1[l]  
   
It may be inconsistent, such that a read of an offset returns
data from either of the datasets
    
    forall l in len(FSDIS0.data):
      (FSDIS0.read(l) == data0[l]) or (FSDIS0.read(l) == data1[l]))

That is, every value read may be from the original or updated file.

It may also be inconsistent on repeated reads of same offset, that is
at time `t2 > t1`:  
 
    r2 = FSDIS0.read(l) 

While at time `t3 > t2`:

    r3 = FSDIS0.read(l)

It may be that `r3 != r2`. (That is, some of the data my be cached or replicated,
and on a subsequent read, a different version of the file's contents are returned).

 
Similarly, if the data at the path `p`, is deleted, this change MAY or MAY
not be visible during read operations performed on `FSDIS0`. 
 

