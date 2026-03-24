# RELEASE_TYPE: minor

This release makes a bunch of last-minute cleanups to places where our API obviously needed fixing that emerged during docs review.

* Removes `none()` which is a weird Python anachronism
* Makes various places where we had a no-arg method to take a boolean to match `unique(bool)`
* Replaces our various tuplesN functions with a tuples! macro
