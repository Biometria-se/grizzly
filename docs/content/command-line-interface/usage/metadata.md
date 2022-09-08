---
title: Metadata
---
@anchor command-line-interface.usage.metadata
# Metadata

It is possible to inject `grizzly-cli` arguments via comments anywhere in a feature file (recommended to add them at the top though).
When executing a feature file, `grizzly-cli` will add the specified arguments automagically, if they match the command being executed.

This makes it possible to add arguments needed for a specific feature file to be documented in the feature file itself, and one does not
have to remember all combinations in memory.

## Format

``` gherkin
# grizzly-cli <[sub]parser> <argument>
```

No validation is done that an argument actually exists in the subparser, other than that `grizzly-cli` will fail with an argument error.
Which is solved by checking {@link command-line-interface.usage} usage and correct the metadata comments.

## Examples

E.g., the `run` subparser is used by both `dist` and `local`, so when specifying an metadata comment for `run` arguments it should be:

``` gherkin title="example.feature"
# grizzly-cli run --verbose
Feature: Example Feature
  Scenario: Example Scenario
    ...
```

This means that executing `example.feature` either in mode `local` or `dist`, the argument `--verbose` will be injected unless already manually specified.

``` plain
grizzly-cli local run example.feature -> grizzly-cli local run example.feature --verbose
grizzly-cli dist run example.feature -> grizzly-cli dist run example.feature --verbose
```

If metadata comments adds arguments for a subparser that is not used when executing the feature file the following message will be seen when executing
`grizzly-cli`:

``` plain
?? ignoring <arguments>
```

Given the following feature file:

``` gherkin title="example-dist.feature"
# grizzly-cli dist --health-retries 999
# grizzly-cli dist --workers 6
# grizzly-cli run --verbose
Feature: Example Feature
  Scenario: Example Scenario
    ...
```

When executed with `grizzly-cli local run example-dist.feature`, the output will contain:

``` plain
?? ignoring dist --health-retries 999
?? ignoring dist --workers 6
```

And the command that is actually executed is `grizzly local run example-dist.feature --verbose`.
