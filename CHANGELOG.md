# Changelog

## 0.1.7 - 2026-02-27

Add the `#[hegel::test]` macro as an ergonomic way to declare a hegel test.


## 0.1.6 - 2026-02-27

Minor code style cleanup: elide unnecessary named lifetimes.


## 0.1.5 - 2026-02-26

Rename the `gen` module to `generators`, avoiding a conflict with rust edition 2024, which made `gen` a reserved keyword.


## 0.1.4 - 2026-02-26

Refactor internals for better encapsulation of per-test-case state.


## 0.1.3 - 2026-02-25

Change how to draw a value from a generator from `generator.generate()` to `hegel::draw(generator)`.


## 0.1.2 - 2026-02-25

This patch adds support for setting `seed` as an option to `hegel`.


## 0.1.1 - 2026-02-24

Add `gen::from_type`, for use together with `#[derive(Generate)]`.

