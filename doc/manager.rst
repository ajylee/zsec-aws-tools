Resource Managers
==================

Each resource has a manager attached to it. This is used in garbage collection and consistency and drift
checks. The manager is represented by a string. Most often, it is a URL to a git repository.


Behavioral specification
-------------------------

1. Manager is persisted for a live resource on creation. This makes it possible to detect drift between
   code and deployment environment.
2. On put, manager is checked for consistency. By default, block on inconsistency, if override, warn.
   (TODO: warn and log when overrides occur.)
3. Manager is used in cloud resource "garbage collection" scoping rules.
