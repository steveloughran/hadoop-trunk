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
  
# Registry Architecture

ZK is used as the underlying system of record. Writes update entries in the
ZK namespace, reads query it

1. all writes are asynchronous and eventually consistent across the ZK quorum.
All that is guaranteed is that updates from a single client are processed in
the order in which they are submitted.

1. The API appears RESTful even when the implementation is not. This is to
offer a consistent view for the (currently Read-only) REST view as well
as the read/write ZK operations.

1. In the hierarchy of entries, *components* may be ephemeral, but all other
nodes (znodes) are not. This is actually a requirement of ZooKeeper, but is
required here to support the scenario "service temporarily offline, registration data
still visible". There is, after all, no prerequisite that a service is actually
running for a client to communicate with it -only that the service must
be running for it to process the data and/or issue a response. 

## Hierarchy

    $base/users/$user/$service-class/$instance/components/*
    $base/users/$user/$service-class/$instance -> instance data