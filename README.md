Hibera
======

Hibera is a control plane for distributed applications. It's designed to
simplify operations for things like fault tolerance, fail-over, configuration.

Similar projects are:
* Zookeeper
* Google chubby
* Doozer

Why?
====

Why not?

Hibera adds a few things for distributed co-ordination that I believe are useful:
* Automatic node addition and removal.
* A useable command-line tool.
* Simple authentication (with namspaces).

It does have several glaring weaknesses at the moment:
* No real security model.
* Performance has not been a consideration.
* It's model for quorum is probably buggy.

As you may have guessed from the weaknesses, this is not yet production-ready.

Philosophy
==========

This is not a tool built for a static world.

Nodes are automatically added and removed quickly. This means that if you power
off some instance running Hibera, then it will quickly get shuffled out of the
cluster and other nodes will take on its data footprint. If you power it back
on, then it will rejoin and resyncronize with the cluster. This can generate a
lot of needless traffic.

Hibera is designed for systems that are designed for failure. When a node
fails, it is presumed dead. This is realistic for modern cloud platforms, where
instances are ephemeral and eventual failure is assured. However, as a whole
the system must go on.

Building
========

Hibera requires at least `go1.1`.

To build Hibera, setup your `GOROOT` and `PATH` appropriately, then type `make`.

You can build packages via `make deb` and `make rpm`.

Command line
============

Type `hibera` to see command line usage.

Internal API
============

Locks
-----
```
    client := NewHiberaClient(addrs, auth, 0, "")

    // Acquire a lock (fires an event).
    //   POST /v1.0/sync/{key}?timeout={timeout}&data={data}&limit={limit}
    //
    // data -- Use the empty string for the default data.
    // limit -- For a lock, this is the number of holders (1 is a mutex).
    // timeout -- Use 0 for no timeout.
    index, rev, err := client.SyncJoin(key, data, limit, timeout)

    // Releasing a lock (fires an event).
    //   DELETE /v1.0/sync/{key}?data={data}
    rev, err = client.SyncLeave(key, data)

    // Wait for a lock to be acquired / released.
    //   GET /v1.0/event/{key}?rev={rev}&timeout={timeout}
    //
    // rev -- Use 0 for any revision.
    // timeout -- Only wait for a fixed time.
    rev, err := client.EventWait(key, rev, timeout)
```

Groups
------
```
    client := NewHiberaClient(addrs, auth, 0, "")

    // Joining a group (fires an event).
    // NOTE: By default, the member data used will be the address of the
    // client socket received on the server end. This can be overriden by
    // providing data in the join call below. You can actually join multiple
    // times by providing different data.
    //   POST /v1.0/sync/{key}?data={data}&limit={limit}&timeout={timeout}
    //
    // data -- Use the empty string to use the default data.
    // limit -- For a pure group, you should use 0.
    // timeout -- Not used if limit is 0.
    index, rev, err := client.SyncJoin(group, data, 0, 0)

    // Leaving a group (fires an event).
    //   DELETE /v1.0/sync/{key}?data={data}
    //
    // data -- Use the empty string to use the default data.
    rev, err = client.SyncLeave(group, data)

    // List the members of the group.
    // NOTE: Members returned have a strict ordering (the first member is
    // the leader). You can easily implement a service that needs N leaders
    // by selecting the first N members of the group, which will be stable.
    // Also, you own index for {data} will be returned via the index.
    // with a prefix of '*'.
    //   GET /v1.0/sync/{key}/?data={data}&limit={limit}
    //
    // data -- The data to use for the index.
    // limit -- Use 0 to specify no limit.
    index, members, rev, err := client.SyncMembers(group, data, limit)

    // Wait for group members to change.
    //   GET /v1.0/event/{key}?rev={rev}&timeout={timeout}
    //
    // rev -- Use 0 for any rev.
    // timeout -- Only wait for a fixed time.
    rev, err := client.EventWait(key, rev, timeout)
```

Data
----
```
    client := NewHiberaClient(addrs, auth, 0, "")

    // Reading a value.
    // Will return when the value is *not* the given revision.
    //   GET /v1.0/data/{key}?rev={rev}&timeout={timeout}
    //
    // rev -- Use 0 for any rev.
    // timeout -- Only wait for a fixed time.
    value, rev, err := client.DataGet(key, rev, timeout)

    // Writing a value (fires an event).
    for {
        // Will fail if the rev is not the same.
        //   POST /v1.0/data/{key}?rev={rev}
        //
        // rev -- Use 0 for any rev.
        ok, rev, err := client.DataSet(key, rev.Next(), newvalue)
        if ok && rev.Equals(rev.Next()) {
            break
        }
    }

    // Delete the data under a key.
    //   DELETE /v1.0/data/{key}?rev={rev}
    //
    // rev -- Use 0 for any rev.
    ok, rev, err := client.DataRemove(key, rev)

    // List all data.
    // (Should be used sparingly).
    //   GET /v1.0/data/
    items, err = client.DataList()
```

Events
------
```
    client := NewHiberaClient(addrs, auth, 0, "")

    // Wait until the key is not at given rev.
    //   GET /v1.0/event/{key}?rev={rev}&timeout={timeout}
    // rev -- Use 0 for any rev.
    rev, err = client.EventWait(key, rev, timeout)

    // Fire an event manually.
    //   POST /v1.0/event/{key}?rev={rev}
    //
    // rev -- Use 0 for any rev.
    rev, err := client.EventFire(key, rev)
```

HTTP API
========

The service API is entirely HTTP-based.

Data distribution is done by 30X redirects where necessary, so clients *must*
support this operation.

Headers
-------
Revisions are always returned in the header `X-Revision`.  This is true for
revisions of sync objects, data objects and the full cluster revision.

Clients should specify an `X-Client-Id` header with a unique string. This will
allow them to connect using multiple names and associate ephemeral nodes.  For
example, supporose a client connects to a server with two TCP sockets, A and B.

    Client                                              Server
           ---A---> SyncJoin w/ X-Client-Id ---A--->     Ok
           ---B---> EventWait w/ X-Client-Id ---B--->    Ok
           ---A---X Connection dies.
                    Client is still in joined group.

Namespaces
----------

Data and synchronization is available under independent namespaces. By default,
the `Host` header is used, but a namespace can be explicitly selected using the
`X-Namespace` header. Note that only the default namespace (empty string) will
always exist, no other namespace can be accessed until authorization tokens are
defined.

URLs
====

/v1.0/
---

* GET
    Fetches cluster info (JSON).

/v1.0/sync/{key}
---------------

* GET

    List syncronization group members.
    
    `limit` -- List only up to N members.
    
    `data` -- Use this data for computing the index.

* POST

    Join a synchronization group.
    
    `data` -- Use to specify a data to join under.
              One client may join multiple times.

    `limit` -- Block on joining until < limit members are in.

    `timeout` -- If `limit` is specified, timeout after 
                 a fixed number of milliesconds.

* DELETE

    Leave the given group.
    
    `data` -- The data to leave the group.

/v1.0/data
------

* GET

    List all data keys.
    This is an expensive operation, don't do it.

/v1.0/data/{key}
-----------

* GET

    Get the current data.

    `rev` -- Return when the rev is not `rev`.

    `timeout` -- Return after `timeout` milliseconds.

* POST

    Update the given key.

    `rev` -- Update only if the current rev is `rev`.
             Use 0 to update always.

* DELETE

    Delete the given key.

    `rev` -- Delete only if the current rev is `rev`.
             Use 0 to delete always.

/v1.0/event/{key}
------------

* POST

    Fires an event on the given key.
    
    `rev` -- Fire only if the revision is currently `rev`.

* GET

    Wait for synchronization events on the given key.
    
    `rev` -- Return when the rev is not `rev`.
             Use 0 to return on the first change.

    `timeout` -- Return after `timeout` milliseconds.

/v1.0/data/{key}
-----------

* GET

    Get the auth token (JSON).

* POST

    Create or update the given auth token (JSON).

* DELETE

    Delete the given auth token.

/v1.0/event/{key}
------------

* POST

    Fires an event on the given key.
    
    `rev` -- Fire only if the revision is currently `rev`.

* GET

    Wait until the key is not at revision `rev`.

    `rev` -- Fire only when the revision is not `rev`.

/v1.0/access
-------

* GET

    List all access tokens.

/v1.0/access/{key}
-----------

* GET

    Get the given access token.

* POST

    Update access to a given path for the token.

    `path` -- The path to modify.
    `read` -- True / false for read permission.
    `write` -- True / false for write permission.
    `execute` -- True / false for synchronization permission.

URL aliases
===========

/{.*}
---

* GET
* POST
* DELETE

    Treat paths as /v1.0/data/${value} where ${value) includes the '/'.
