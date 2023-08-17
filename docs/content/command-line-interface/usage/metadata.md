---
title: Metadata
---
@anchor command-line-interface.usage.metadata Metadata
# Metadata

It is possible to add metadata in feature files that `grizzly-cli` will use. Metadata comments can be added anywhere in the feature file, but
it is recommended to add them in the top for readability.

## Arguments

Inject `grizzly-cli` arguments via metadata comments.
When executing a feature file, `grizzly-cli` will add the specified arguments automagically, if they match the command being executed.

This makes it possible to add arguments needed for a specific feature file to be documented in the feature file itself, and one does not
have to remember all combinations in memory.

### Format

``` gherkin
# grizzly-cli <[sub]parser> <argument>
```

No validation is done that an argument actually exists in the subparser, other than that `grizzly-cli` will fail with an argument error.
Which is solved by checking {@link command-line-interface.usage} and correct the metadata comments.

### Examples

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

## Notices

It is possible to tell `grizzly-cli` show confirmation notices with metadata in a a feature file.

This is useful to remind the user about manual steps och checks that should be done before running the feature.

### Format

``` gherkin
# grizzly-cli:notice <message>
```

Everything after `# grizzly-cli:notice ` (notice the space) will be displayed in the confirmation prompt.

### Examples

``` gherkin title="example.feature"
# grizzly-cli:notice have you piped the fork in a loop?
Feature: Example Feature
  Scenario: Example Scenario
    ...
```

Running `example.feature` will in additional to the normal `grizzly-cli` input/output also trigger the following prompt:

``` plain
have you piped the fork in a loop? [y/n]
```

If `run` argument `-y/--yes` is provided, it will only print the message and not ask for confirmation.

