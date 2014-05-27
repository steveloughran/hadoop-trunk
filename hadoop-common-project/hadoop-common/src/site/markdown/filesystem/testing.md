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
  
# Testing the Filesystem Contract


## Adding a new test suite

1. New tests should be split up with a test class per operation -as is done for `seek()`, `rename()`, `create()`, etc.. This is to match up the way that the FileSystem contract specification is split up by operation. It also makes it easier for FileSystem implementors to work on a test suite at a time.
2. Subclass `AbstractFSContractTestBase` with a new abstract test suite class. Again, use `Abstract` in the title.
3. Look at `org.apache.hadoop.fs.contract.ContractTestUtils` for utility classes to aid testing, with lots of filesystem-centric assertions. Use these to make assertions about the filesystem state, and to incude diagnostics information such as directory listings and dumps of mismatched files when an assertion actually fails.
4. Write tests for the local, raw local and HDFS filesystems -if one of these fails the tests then there is a sign of a problem -though be aware that they do have differnces
5. Test on the object stores once the core filesystems are passing the tests.
4. Try and log failures with as much detail as you can -the people debugging the failures will appreciate it.
 

### Root manipulation tests

Some tests work directly against the root filesystem, attempting to do things like rename "/" and similar actions. The root directory is "special", and it's important to test this, especially on non-Posix filesystems such as object stores. These tests are potentially very destructive to native filesystems, so use care.

1. Add the tests under `AbstractRootDirectoryContractTest` or create a new test with (a) `Root` in the title and (b) a check in the setup method to skip the test if root tests are disabled:

          skipIfUnsupported(TEST_ROOT_TESTS_ENABLED);

1. Don't provide an implementation of this test suite to run against the local FS.

### Scalability tests

Tests designed to generate scalable load -and that includes a large number of small files, as well as fewer larger files, should be designed to be configurable, so that users of the test
suite can configure the number and size of files.

Be aware that on object stores, the directory rename operation is usually `O(files)*O(data)` while the delete operation is `O(files)`. The latter means even any directory cleanup operations may take time and can potentially timeout. It is important to design tests that work against remote filesystems with possible delays in all operations.

## Extending the specification

The specification is incomplete -it doesn't have complete coverage of the FileSystem classes, and there may be bits of the existing specified classes that are undercovered.

1. Look at the implementations of a class/interface/method to see what they do, especially HDFS and local -these are the documentation of what is done today.
2. Look at the POSIX API specification.
3. Search through the HDFS JIRAs for discussions on filesystem topics -and try nd understand what was meant to happen, as well as what does.
4. Use your IDE to find out how methods are used in Hadoop, HBase and oher parts of the stack -assumng these are representative of Hadoop applications, they show how applications *expect* a filesystem to behave.
5. Look in the java.io source to see how the bunded filesystem classes are expected to behave -and read their javadocs carefully.
5. If something is unclear -as on the hdfs-dev list.
6. Don't be afraid to write tests to act as experments and clarify what actually happens -use the HDFS behaviors as the normative guide.

## Adding contract tests for a new filesystem

The core part of supporting a new FileSystem for the contract tests is adding a new contract class, then creating a new non-abstract test class for every test suite that you wish to test.

1. Create a package in your own test source tree (usually)under `contract`, for the files and tests.
2. Subclass `AbstractFSContract` for your own contract implementation.
3. For every test suite you plan to support create a non-abstract subclass that implements the abstract method `createContract()`.


As an example, here is the implementation of the test of the `create()` tests for the local filesystem.

    package org.apache.hadoop.fs.contract.localfs;

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.contract.AbstractCreateContractTest;
    import org.apache.hadoop.fs.contract.AbstractFSContract;

    public class TestLocalCreateContract extends AbstractCreateContractTest {
      @Override
      protected AbstractFSContract createContract(Configuration conf) {
        return new LocalFSContract(conf);
      }
    }

The standard implementation technique for subclasses of `AbstractFSContract` is to be driven entirely by a Hadoop XML configuration file stored in the test resource tree -where the best practise is to store it under `/contract` with the name of the filesystem, such as `contract/localfs.xml`. Having the XML file define all filesystem options makes the listing of filesystem behaviors mmediately visible. 

The `LocalFSContract` is a special case of this, as it must adjust its case sensititivy policy based on the OS on which it is running: for both Windows and OS/X, the filesystem is case insensitive, so the `ContractOptions.IS_CASE_SENSITIVE` option must be set to false. Furthermore, the Windows filesystem does not support Unix file and directory permissions, so the relevant flag must also be set. This is done *after loading the XML contract file from the resource tree, simply by updating the now-loaded configuration options:

      getConf().setBoolean(getConfKey(ContractOptions.SUPPORTS_UNIX_PERMISSIONS), false);



### Handling test failures

If your new Filesystem test cases fails one of the contract tests, what you can you do?

It depends on the cause of the problem

1. Case: custom FileSystem subclass class doesn't correctly implement specification. Fix.
2. Case: Underlying filesystem doesn't behave in a way that matches Hadoop's expectations. Ideally, fix. Or try to make your `FileSystem` subclass hide the differences -e.g. by translating exceptions. 
3. Case: fundamental architectural differences between your filesystem and Hadoop. Example: different concurrency and consistency model. Recommendation: document and make clear that the filesystem is not compatible with HDFS.
4. Case: test does not match the specification. Fix: patch test, submit the patch to Hadoop.
5. Case: specification incorrect. The underlying specification is -with some minor exceptions- HDFS. If the specification does not match HDFS, HDFS should normally be assumed to be the real definition of what a FileSystem should do. If there's a mismatch -raise on the `hdfs-dev` mailing list. Note that while that filesystem tests live in the core Hadoop codebase -it is the HDFS team who owns the specification of the filesystem and the tests that accompany it.

### Reminder: passing the tests does not guarantee compatibility

Be aware that passing all the filesystem contract tests does not mean that a filesystem can be described as "compatible with HDFS". The tests try to look at the isolated functionality of each operation, and focus on the predconditions and postconditions of each action. Core areas not covered are concurrency -what happens if more than one client attempts to manipulate the same files or directories- and aspects of failure across a distributed system

* consistency: are all changes immediately visible?
* atomicity: are operations which HDFS guarantees to be atomic equally so on the new filesystem.
* idempotency: if the filesystem implements any retry policy, is idempotent even while other clients manipulate the filesystem?
* scalability: does it support files as large as HDFS, or as many in a single directory?
* durability: do files actually last -and how long for?

Proof that this is is true is the fact that the Amazon S3 and OpenStack Swift object stores are eventually consistent object stores with non-atomic rename and delete operations. Single threaded test cases are unlikely to see some of the conconcurrency issues, while concistency is very often only visible in tests that span a datacenter.

There are also some specific aspects of the use of the FileSystem API

* compatibility with the `hadoop -fs` CLI.
* whether the blocksize policy produces file splits that are suitable for analytics workss. (as an example, a blocksize of 1 matches the specification, but as it tells MapReduce jobs to work a byte at a time, unusable).

Tests that verify these behaviors are of course welcome. 


 

