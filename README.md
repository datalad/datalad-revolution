# Revolutionary DataLad extension

[![Travis tests status](https://secure.travis-ci.org/datalad/datalad-revolution.png?branch=master)](https://travis-ci.org/datalad/datalad-revolution) [![Build status](https://ci.appveyor.com/api/projects/status/8jtp2fp3mwr5huyi?svg=true)](https://ci.appveyor.com/project/mih/datalad-revolution) [![codecov.io](https://codecov.io/github/datalad/datalad-revolution/coverage.svg?branch=master)](https://codecov.io/github/datalad/datalad-revolution?branch=master)

This repository contains a [DataLad](http://datalad.org) extension that equips
DataLad with new core commands that are potentially faster, or just better looking.
Moreover, it enhanced the core dataset and repository abstraction classes with
additional functionality that is written with enhanced cross-platform compatibility
and speed in mind.

Additional commands

- `rev-status` -- like `git status`, but simpler and working with dataset hierarchies
- `rev-save` -- a 2-in-1 replacement for `save` and `add`
- `rev-create` -- a faster `create`

Additional base class functionality

- `GitRepo.status()`
- `GitRepo.diff()`
- `GitRepo.annexstatus()`

Path handling in all additional functionality is using the `pathlib` module.


### Try it?

For a demo, clone this repository and install the extension via

    pip install -e .
