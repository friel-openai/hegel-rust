RELEASE_TYPE: minor

This does a big rationalization of the API to what I hope will be close to the final one we release with.

Key changes:

* Move most things from top-level functions to methods on a `TestCase` object explicitly passed in
* `draw` now silently drops printing when not at the top level, and compose! just takes a TestCase object that it can call draw on.
* New `draw_silent` method that doesn't ever print, and lacks the Debug bound.
* Rename Generate trait to Generator
* Make draw take generators by value, so you no longer need draw(& most of the time (A reference to a Generator is a Generator though, so you can still pass by reference if you want)
