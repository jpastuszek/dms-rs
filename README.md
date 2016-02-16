# dms-rs

This is my attempt on rewriting my distributed monitoring system prototype from Ruby into Rust.
I think Rust will be much better language to implement this systems monitoring component.

To build you need installed:
* rust v1.6
* capnp v0.5.2
* nanomsg v0.8-beta

To install nanomsg:
* download sources from GitHub: https://github.com/nanomsg/nanomsg/releases/download/0.8-beta/nanomsg-0.8-beta.tar.gz
* untar beside this project
* cd into unarred directory and run: `./configure && make`

Capnp can be installed with **brew** on MacOS
