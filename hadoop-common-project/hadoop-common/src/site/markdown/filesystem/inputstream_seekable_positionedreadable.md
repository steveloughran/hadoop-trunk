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
<!--  INTERFACE: Seekable -->
<!--  INTERFACE: InputStream -->
<!--  INTERFACE: PositionedReadable -->
<!--  ============================================================= -->

# InputStream, Seekable and PositionedReadable

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

    where Offset &lt; 0 else  Offset >  Buffer.length
    else (Len > (Buffer.length - Offset)) 
    else Len &lt; 0 MUST throw
      InvalidArgumentException or another RuntimeException
    including -but not limited to- `ArrayIndexOutOfBoundsException

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

1. After `S.close()`, `S.seek(P)` SHOULD fail with an `IOException`. (HDFS does, LocalFS does not)

1. For all values of *P1*, *P2* in *Long*,  `S.seek(P1)` followed
by `S.seek(P2)` is the equivalent of `S.seek(P2)`.
*this only holds if S.seek(P1) does not raise an exception -can we explicitly
declare that this elision can ignore that possibility?*

1. On `S.seek(P)` with *P &lt; 0* an exception MUST be thrown.
It SHOULD be an `EOFException`. It MAY be an `IOException`, an `IllegalArgumentException `
or other `RuntimeException`.

1. `S.seek(0)` MUST succeed even if the file length is 0

1.  For all values of *P* in *Long* if `S.seek(P)` does not raise
an Exception, it is considered a successful seek.

1. For all successful `S.seek(P)` operations `S.getPos()` MUST equal p

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
