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

## Running the tests

A normal Hadoop test run will test those filesystems that can be tested locally via the local filesystem -such as  `file://` and its underlying `LocalFS`, and those which can be tested via short-lived HDFS miniclusters -specifically `hdfs://`.

Other filesystems are skipped unless there is a specific configuration to the remote server providing the filesystem.


These filesystem bindings must be defined in an XML configuration file, usually the file `hadoop-common-project/hadoop-common/src/test/resources/contract/auth-keys.xml`. This file is excluded from subversion and git, so should not be unintentionally checked in.

### S3:

In `auth-keys.xml`, the filesystem name must be defined in the property `fs.contract.test.fs.s3`. The standard configuration options to define the S3 authentication details muse also be provided.

Example:

    <configuration>
      <property>
        <name>fs.contract.test.fs.s3</name>
        <value>s3://tests3hdfs/</value>
      </property>

      <property>
        <name>fs.s3.awsAccessKeyId</name>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>

      <property>
        <name>fs.s3.awsSecretAccessKey</name>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>
    </configuration>

### S3n://


In `auth-keys.xml`, the filesystem name must be defined in the property `fs.contract.test.fs.s3n`. The standard configuration options to define the S3N authentication details muse also be provided.

Example:


    <configuration>
      <property>
        <name>fs.contract.test.fs.s3n</name>
        <value>s3n://tests3contract</value>
      </property>

      <property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>

      <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>

### ftp://


In `auth-keys.xml`, the filesystem name must be defined in the property `fs.contract.test.fs.ftp`. The specific login options to connect to the FTP Server must then be provided. 

A path to a test directory must also be provided in the option `fs.contract.test.ftp.testdir`. This is the directory under which operations take place.

Example:


    <configuration>
      <property>
        <name>fs.contract.test.fs.ftp</name>
        <value>ftp://server1/</value>
      </property>

      <property>
        <name>fs.ftp.user.server1</name>
        <value>testuser</value>
      </property>
      
      <property>
        <name>fs.contract.test.ftp.testdir</name>
        <value>/home/testuser/test</value>
      </property>
  
      <property>
        <name>fs.ftp.password.server1</name>
        <value>secret-login</value>
      </property>
    </configuration>
   
   
### openstack swift, swift://

The openstack swift login details must be defined in the file `/hadoop-tools/hadoop-openstack/src/test/resources/contract/auth-keys.xml`. The standard hadoop-common `auth-keys.xml` resource file cannot be used, as that file does not get included in `hadoop-common-test.jar` -so cannot be picked up.


In `/hadoop-tools/hadoop-openstack/src/test/resources/contract/auth-keys.xml` the swift bucket name must be defined in the property `fs.contract.test.fs.swift`, along with the login details for the specific swift service provider in which the bucket is posted.

    <configuration>
      <property>
        <name>fs.contract.test.fs.swift</name>
        <value>swift://swiftbucket.rackspace/</value>
      </property>


      <property>
        <name>fs.swift.service.rackspace.auth.url</name>
        <value>https://auth.api.rackspacecloud.com/v2.0/tokens</value>
        <description>Rackspace US (multiregion)</description>
      </property>

      <property>
        <name>fs.swift.service.rackspace.username</name>
        <value>this-is-your-username</value>
      </property>

      <property>
        <name>fs.swift.service.rackspace.region</name>
        <value>DFW</value>
      </property>

      <property>
        <name>fs.swift.service.rackspace.apikey</name>
        <value>ab0bceyoursecretapikeyffef</value>
      </property>
  
    </configuration>

Often the different public cloud swift infrastructures exhibit different behaviors -authentication and throttling in particular. We recommand that testers create accounts on as many of these providers as possible -and test aainst each of them.

      
## Testing a new filesystem

The core part of supporting a new FileSystem for the contract tests is adding a new contract class, then creating a new non-abstract test class for every test suite that you wish to test.

1. Do not try and add these tests into Hadoop itself -they won't be added to the soruce tree. The tests must live with your own filesystem class.
1. Create a package in your own test source tree (usually)under `contract`, for the files and tests.
2. Subclass `AbstractFSContract` for your own contract implementation.
3. For every test suite you plan to support create a non-abstract subclass -with the name starting with `Test` and the name of the filesystem. Example: `TestHDFSRenameContract`.
4. These non-abstract classes must implement the abstract method `createContract()`.
4. Identify and ocument any filesystem bindings that must be defined in an `src/test/resources/contract/auth-keys.xml` file of the filesystem class in development. The hadoop-common auth-keys file cannot be used as it will not be found.
5. Run the tests until they work.


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

If a test needs to be skipped because a feature is not supported, look for a existing configuration option in the `ContractOptions` class -and use them. If there is no method, the short term fix is to override the method and use the `ContractTestUtils.skip()` message to log the fact that a test is skipped. Using this method prints the message to the logs, then tells the test runner that the test was skipped -which will highlight the problem.

A recomended strategy is to call the superclass, catch the exception and verify that the exception class and part of the error string matches that raised by the current implementation. It should also `fail()` if superclass actually succeeded -that is it failed the way that the implemention does not currently do.  This will ensure that the test path is still executed, any other failure of the test -possibly a regression- is picked up. And, if the feature does become implemented, that the change is picked up.

A long-term solution is to enhance the base test to add a new optional feature key. This will require collaboration with the developers on the `hdfs-dev` mailing list. 



### 'Lax vs Strict' exceptions

The contract tests include the notion of strict vs lax exceptions. *Strict* exception reporting means: reports failures using specific subclasses of `IOException`, such as `FileNotFoundException`, `EOFException` and suchlike. *Lax* reporting means throws `IOException` instances. 

While filesystems SHOULD raise the stricter exceptions, there may be reasons why they cannot. Raising the laxer exceptions is still allowed, it merely hampers diagnostics of failures in user applications. To declare that a filesystem does not support the stricter exceptions, set the option `fs.contract.supports-strict-exceptions` to false.

### Supporting filesystems with login and authenciation parameters

Tests against remote filesystems will require the URL to the filesystem to be specified;
tests againt remote filesystems that require login details require usernames/IDs and passwords.

All these details MUST be required to be placed in the file `src/test/resources/auth-keys.xml`, *and your SCM tools configured to never commit this file to subversion, git or
equivalent. Furthermore, the build MUST be configured to never bundle this file in any `-test` artifacts generated. The Hadoop build does this, excluding `src/test/**/*.xml` from the JAR files.

The `AbstractFSContract` class automatically loads this resource file if present; specific keys for specific test cases can be added. 

As an example, here are what S3N test keys look like.

    <configuration>
      <property>
        <name>fs.contract.test.fs.s3n</name>
        <value>s3n://tests3contract</value>
      </property>

      <property>
        <name>fs.s3n.awsAccessKeyId</name>
        <value>DONOTPCOMMITTHISKEYTOSCM</value>
      </property>

      <property>
        <name>fs.s3n.awsSecretAccessKey</name>
        <value>DONOTEVERSHARETHISSECRETKEY!</value>
      </property>
    </configuration>
    
The `AbstractBondedFSContract` automtically skips a test suite if the filesystem URL is not defined in the property `fs.contract.test.fs.%s`, where `%s` matches the schema name of the filesystem;



### Important: passing the tests does not guarantee compatibility

Passing all the filesystem contract tests does not mean that a filesystem can be described as "compatible with HDFS". The tests try to look at the isolated functionality of each operation, and focus on the predconditions and postconditions of each action. Core areas not covered are concurrency -what happens if more than one client attempts to manipulate the same files or directories- and aspects of failure across a distributed system

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
 

