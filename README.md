# Field Engineering Power Tools

This repository contains Go utility packages which are common to a
number of Cockroach Labs Field Engineering projects.

Refer to [Go Multi-Module
Workspaces](https://go.dev/doc/tutorial/workspaces) for an easy way to
edit this repository concurrently with another project.

```shell
git clone git@github.com:cockroachdb/replicator.git
git clone git@github.com:cockroachdb/field-eng-powertools.git

cd replicator
go work init
go work use .
go work use ../powertools
```
