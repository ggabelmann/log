Voice Over
==========

Kevin Flynn: The Log. A storage abstraction. I tried to picture clusters of information as they were stored by the computer. 
What did they look like? InputStreams? Byte arrays? Were the calls non-blocking? 
I kept dreaming of a service I thought I'd never use. And then one day...

Young Sam Flynn: You wrote it!

Kevin Flynn: That's right, man. I wrote it.

Log
===

This project is my implementation of a "log", which is an append-only sequence of items. 
There's a great article on Linkedin (https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) about what a log is and how it can be used.
In the course of thinking about what a log is and how to write one I've settled on a simple implementation that will work for my other projects:

- The FileLogService class is an implementation of a Log and a Guava Service.
  - Items can be appended with an auto-generated ID.
  - Items can be appended with a required ID (or fail-fast). This allows "optimistic appends".
  - Reads can occur in parallel (if the OS/platform allows it).
  - Writes are blocked by other reads/writes. This was simpler to do for this first version.
- The LogServer class allows a FileLogService to be accessed with standard REST calls.
  - It uses the Undertow library to start a server and respond to REST calls.

Usage
=====

LogServer has a main() method and can be run.

Future
======

- More tests.
- Investigate removing the ReentrantReadWriteLock that prevents parallel reads/writes.
- Learn more about Undertow and non-blocking I/O.
- Improve LogServer documentation.