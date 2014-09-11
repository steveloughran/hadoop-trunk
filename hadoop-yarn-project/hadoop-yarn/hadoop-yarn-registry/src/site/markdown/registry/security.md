
# Registry Security

The security model of the registry is designed to meet the following goals

1. Deliver functional security on a secure ZK installation.
1. Allow the RM to create per-user regions of the registration space
1. Allow applications belonging to a user to write registry entries into their part of the space. These may be short-lived or long-lived YARN applications,  or they may be be static applications.
1. Prevent other users from writing into another user's part of the registry.
1. Allow system services to register to a `/services` section of the registry.
1. Provide read access to clients of a registry.
1. Permit future support of DNS
1. Permit the future support of registering data private to a user. This allows a service to publish binding credentials (keys &c) for clients to use.
1. Not require a ZK keytab on every user's home directory in a YARN cluster. This implies that kerberos credentials cannot be used by YARN applications.


ZK security uses an ACL model, in which different authentication schemes may be used to restrict access to different znodes. This permits the registry to use a mixed Kerberos + Private password model.

* The YARN-based registry (the `RMRegistryOperationsService`), uses kerberos as the authentication mechanism for YARN itself.
* The registry configures the base of the registry to be writeable only by itself and other hadoop system accounts holding the relevant kerberos credentials.
* The user specific parts of the tree are also configured to allow the same system accounts to write and manipulate that part of the tree.
* User accounts are created with a `(username,password)` keypair granted write access to their part of the tree. 
* The secret part of the keypair is stored in the users' home directory on HDFS, using the Hadoop Credentials API.
* Initially, the entire registry tree will be world readable.


What are the limitations of such a scheme?

1. It is critical that the user-specific registry keypair is kept a secret. This relies on filesystem security to keep the file readable only by the (authenticated) user.
1. While it is possible to change the password for an account, this involves a recursive walk down the registry tree, and will stop all running services from being able to authenticate for write access until they reload the key.
1. A world-readable registry tree is exposing information about the cluster. 
