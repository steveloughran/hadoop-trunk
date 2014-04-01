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
  
# The Hadoop Filesystem API Definition

This is an attempt to model the contents a filesystem as a set of paths that 
are either directories, symbolic links or files; only files may contain data.

There is surprisingly little prior art in this: multiple specifications of
the Unix filesystems as a tree of inodes, but nothing public which defines the
notion of "Unix filesystem as a conceptual model for data storage access". 

This specification attempts to do that; to define the Hadoop File System model
and APIs so that multiple filesystems can implement the APIs and present a consistent
model of their data to applications. It does not attempt to specify any of the
concurrency  

1. [Introduction](introduction.html)
1. [Notation](notation.html)
1. [Model](model.html)
1. [FileSystem class](filesystem.html)
1. [FSDataInputStream class](fsdatainputstream.md.html)
