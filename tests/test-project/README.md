# README

To run this project distributed, you have to build the image first:

```bash
cd ../../
grizzly-cli dist --project-name test-project build --no-cache --local-install
```

The last argument (`--local-install`) is important so that the version you currently are developing is included on the image.

Then start the feature:
```bash
cd tests/test-project/
grizzly-cli dist run test.feature
```
