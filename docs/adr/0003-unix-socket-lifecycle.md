---
status: accepted
---

# Unix-domain socket files: probe then replace on bind, remove on close

A listener binding an `Endpoint::Unix` whose path already exists first checks with `lstat` that
the existing object is a socket; anything else — a regular file, a directory, a symbolic link
even if it points at a socket — is left untouched and the bind fails with an error naming the
path, never a panic. For a socket it then connects to the path without blocking: a refused
connection means the file is a stale remnant of a process that did not clean up, so it is
unlinked and the bind proceeds; a connection that completes or is left pending — a live owner,
even one whose accept queue is full — fails the bind with `AddrInUse`; any other error from the
probe is returned as it is, naming the path. The `lstat` check is what makes the unlink safe,
because a refused `connect` alone proves nothing about the object's type: connecting to a
regular file is refused too. Closing a listener unlinks its path. The socket file is created
with mode `0777` less the umask bits, and a client needs write permission on it to connect, so
the usual `022` umask yields `0755` — owner-only connections — and an operator who wants group
or world access sets the umask or changes the mode; flux offers no mode or ownership setting.
Outbound Unix endpoints reconnect at the ConnectionGroup's interval exactly like TCP (`ENOENT` and
`ECONNREFUSED` both retry). Removing the file on close is what nginx and Go's `net.Listen` do; a
stale file after a crash must not block a restart; and an unconditional unlink would let a
misconfigured second process silently take a live node's path — the probe is the cheapest guard
against that.

## Considered options

- **Bare `bind`, no unlink anywhere.** Rejected: every crash leaves a file that blocks the
  next start until an operator removes it.
- **Unconditional unlink before bind.** Rejected: steals a live path on misconfiguration,
  and the probe costs one connect.
- **Bind a temporary path and rename over the target.** Rejected: atomic, but the same steal
  semantics as unconditional unlink with more code.
- **Mode and ownership settings on the listener.** Not offered: the operator controls both through
  the umask and the directory. An explicit setting is a speculative extension, not a decision.
