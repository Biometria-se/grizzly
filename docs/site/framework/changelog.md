---
title: Changelog
---
# Changelog

{{ changelog('framework', 'framework') }}

<!-- static, generated from old repo -->

{% raw %}
## v3.2.5 **legacy**{.chip-feature .info}

- <a href="https://github.com/Biometria-se/grizzly/commit/152d0623a7efe8d0680aa78cdc63c56f5d6a53d1" target="_blank">`152d0623`</a>: revert release workflow to use python 3.12

## v3.2.4

- <a href="https://github.com/Biometria-se/grizzly/commit/66ed9d161ad4296ee7f06d69c1cc4ace860199df" target="_blank">`66ed9d16`</a>: problem building docs in CI workflow

## v3.2.3

- <a href="https://github.com/Biometria-se/grizzly/commit/4e99dc36d9487eae141aa611cc06f8a8908a97e3" target="_blank">`4e99dc36`</a>: use `inspect.getmro` do determine argument type (#375)

## v3.2.2

- <a href="https://github.com/Biometria-se/grizzly/commit/e21141efe76b89f9d5dc9955b87fc09bff36f9f5" target="_blank">`e21141ef`</a>: hanging after test finished (#374)

## v3.2.1

- <a href="https://github.com/Biometria-se/grizzly/commit/a48049223c6c651ddab890f29a7c0ece848aae2a" target="_blank">`a4804922`</a>: replace pydoc-markdown with mkdocstrings (#373)
- <a href="https://github.com/Biometria-se/grizzly/commit/41147350add9b0868b8eeadb550deff2b6b60aaf" target="_blank">`41147350`</a>: dependencies update (#371)
- <a href="https://github.com/Biometria-se/grizzly/commit/f56d40fb548dc77643927c5901e5a440290648c8" target="_blank">`f56d40fb`</a>: retry can raise failure_exception if set (#370)
- <a href="https://github.com/Biometria-se/grizzly/commit/e7bc9b81ef84dbb5911e6643d3368e2c6832da13" target="_blank">`e7bc9b81`</a>: own identity for TestdataConsumer logger, to avoid confusion, since it's shared between all user instances of the same type running on the same worker. (#369)
- <a href="https://github.com/Biometria-se/grizzly/commit/c19a4565f1707e6341d804e005a760478d37aa46" target="_blank">`c19a4565`</a>: keystore: more ambigous message in `RuntimeError` when no value is found for key (#368)
- <a href="https://github.com/Biometria-se/grizzly/commit/5158f7f59101e326c7e46a8d7d60cff5f225adf3" target="_blank">`5158f7f5`</a>: IotHubUser: remove verbose info logging (#367)
- <a href="https://github.com/Biometria-se/grizzly/commit/d0fb8e09b4f080ec8eeeb38bf88b96c4de22bd15" target="_blank">`d0fb8e09`</a>: HttpClientTask: paths to client cert and key should be relative to context root (#366)
- <a href="https://github.com/Biometria-se/grizzly/commit/8490d6ffa5acdbad7d27386dda9d6e72e5256a55" target="_blank">`8490d6ff`</a>: RestApiUser: assume that cert/key files are relative to context root (GRIZZLY_CONTEXT_ROOT) (#365)
- <a href="https://github.com/Biometria-se/grizzly/commit/df745b389d7951624d029f02f0470631a1b3d03a" target="_blank">`df745b38`</a>: request task failure handling + spawning complete synchronization (#364)
- <a href="https://github.com/Biometria-se/grizzly/commit/d8da1c4efdc582d6efd674803ebbd8ffae3ac86b" target="_blank">`d8da1c4e`</a>: bump locust version (#363)
- <a href="https://github.com/Biometria-se/grizzly/commit/f02b765bde852210d28b1bce629cf21e69dd165b" target="_blank">`f02b765b`</a>: no http retries (#362)
- <a href="https://github.com/Biometria-se/grizzly/commit/fd12f3f2135080c56265f1d33c390bc10927bea3" target="_blank">`fd12f3f2`</a>: IotHubUser: `allow_already_exist` argument for requests (#361)

## v3.2.0

- <a href="https://github.com/Biometria-se/grizzly/commit/71c6aa313f2f523529a152c22833b109106a57a3" target="_blank">`71c6aa31`</a>: bad PR -- a lot of changes (#360)
- <a href="https://github.com/Biometria-se/grizzly/commit/cf9409ebf1cc8648d0c8991f6befa733dc9e877e" target="_blank">`cf9409eb`</a>: rewrite of testdata communication, and other improvements (#357)
- <a href="https://github.com/Biometria-se/grizzly/commit/f5c079b8c9e544e1e3bec7f30f93484c91ddce41" target="_blank">`f5c079b8`</a>: ServiceBus improvements in async-messaged (#356)
- <a href="https://github.com/Biometria-se/grizzly/commit/95bbbb8759fd9b8ab04192e1e038ccba5c28f66c" target="_blank">`95bbbb87`</a>: CSV writers keep files open during test + user events for the writing (#355)

## v3.1.1

- <a href="https://github.com/Biometria-se/grizzly/commit/392efca5e4beff7e85c0d152a960c2e610b16327" target="_blank">`392efca5`</a>: new permutation interface (#354)
- <a href="https://github.com/Biometria-se/grizzly/commit/1187653de299ef3b49cda93f36515ac3000de800" target="_blank">`1187653d`</a>: Custom failure handling (#353)

## v3.1.0

- <a href="https://github.com/Biometria-se/grizzly/commit/1e902cdb09d80bcce15be18a72e7eba8bede2edc" target="_blank">`1e902cdb`</a>: testdata variable message handlers should run concurrent (#351)
- <a href="https://github.com/Biometria-se/grizzly/commit/c19bdfa5ab363931cdb676fa1fbb573dd9461309" target="_blank">`c19bdfa5`</a>: dump include variables in `--dry-run` (#350)
- <a href="https://github.com/Biometria-se/grizzly/commit/e1a01cec160c7e491a234533e8c4f1666aca97a0" target="_blank">`e1a01cec`</a>: improved handling of abort (ctrl + c) (#349)
- <a href="https://github.com/Biometria-se/grizzly/commit/854886bc8d89e0a3645748f743b0df06d651c3bc" target="_blank">`854886bc`</a>: Async-messaged fixes; (#348)
- <a href="https://github.com/Biometria-se/grizzly/commit/3b98bc39fe199272cce8c7989fbffa87a5037a6f" target="_blank">`3b98bc39`</a>: `IotHubUser` support for receiving (C2D) messages (#346)
- <a href="https://github.com/Biometria-se/grizzly/commit/1c7a89bfb688e55b630fdb181f4ffb113fc5145f" target="_blank">`1c7a89bf`</a>: improvements of keystore protocol: `push`, `pop` and `del` (#345)
- <a href="https://github.com/Biometria-se/grizzly/commit/cd58490dfdbd85fb8e2110d628e630666ff3ec6f" target="_blank">`cd58490d`</a>: sub-render variables if they contain templates in `SetVariableTask` and `HttpClientTask` (#344)
- <a href="https://github.com/Biometria-se/grizzly/commit/a589ef6ad0e22d75d39d0605e6ee50afced7528a" target="_blank">`a589ef6a`</a>: PUT support in `HttpClientTask` and file contents when declaring variables (#343)
- <a href="https://github.com/Biometria-se/grizzly/commit/95f7a27dfb0cd16c536c019c13ceece6e57f53a1" target="_blank">`95f7a27d`</a>: handle `Mod` nodes in jinja2 templates (#342)
- <a href="https://github.com/Biometria-se/grizzly/commit/20bd1104e63455a515aa57c21af773641fa18720" target="_blank">`20bd1104`</a>: new atomic variable: `AtomicJsonReader` (#341)

## v3.0.0

- <a href="https://github.com/Biometria-se/grizzly/commit/982e8d0f8864b82982592733518823b723e63544" target="_blank">`982e8d0f`</a>: rewrite of variable separation per scenario (#339)
- <a href="https://github.com/Biometria-se/grizzly/commit/60c783639e2e2639ed1197abde0b28ef00158a9f" target="_blank">`60c78363`</a>: update locust version to a official release (#337)
- <a href="https://github.com/Biometria-se/grizzly/commit/69e4c9a6de33208f5ab4fc4af458367850d3e456" target="_blank">`69e4c9a6`</a>: geventhttpclient does not accept `verify` in request arguments (#336)
- <a href="https://github.com/Biometria-se/grizzly/commit/00338035384df5e2ae1fbbda89f372bc3ee0dcfd" target="_blank">`00338035`</a>: variables per scenario (#335)
- <a href="https://github.com/Biometria-se/grizzly/commit/96bd4ff26f0907547226c9e3411f10c54d49609f" target="_blank">`96bd4ff2`</a>: fix for "Aborting test can fail when running distributed with MQ" (#333)
- <a href="https://github.com/Biometria-se/grizzly/commit/158e715acd825070e62ddcd052da6f2d241d6197" target="_blank">`158e715a`</a>: override influxdb event with special named context key-values (#332)
- <a href="https://github.com/Biometria-se/grizzly/commit/fd54641dd7ff8026e9bf11e3bc424da9ca818471" target="_blank">`fd54641d`</a>: general improvements (#330)
- <a href="https://github.com/Biometria-se/grizzly/commit/c42570d0543c5f4d88a8572ed24766ba3731fe2f" target="_blank">`c42570d0`</a>: increment value in keystore (#327)
- <a href="https://github.com/Biometria-se/grizzly/commit/47485f51f83326776340efa9cf142031af315e8e" target="_blank">`47485f51`</a>: merge environment configuration files (#326)
- <a href="https://github.com/Biometria-se/grizzly/commit/9dfabf940adb15a3e3ca4ddc60ce8ad1c9710e11" target="_blank">`9dfabf94`</a>: jsonpath filter one of, `=|` (#325)

## v2.10.2

- <a href="https://github.com/Biometria-se/grizzly/commit/2d0336a74645dea9a608eb0bb71db5e41ac204ac" target="_blank">`2d0336a7`</a>: retry creating a service bus connection if it times out (#322)
- <a href="https://github.com/Biometria-se/grizzly/commit/953852b337914e3b48b81f6f54dcd1b795db2f37" target="_blank">`953852b3`</a>: fixing stability issues identified during ~5h tests (#321)

## v2.10.1

- <a href="https://github.com/Biometria-se/grizzly/commit/ed82ea4d4107ec7a492880d85af8a1698af704b4" target="_blank">`ed82ea4d`</a>: do not touch cookies or authorization headers (#320)

## v2.10.0

- <a href="https://github.com/Biometria-se/grizzly/commit/16c03465e63e2f05a5889d86670f0d58430485fe" target="_blank">`16c03465`</a>: refresh token bug (#319)
- <a href="https://github.com/Biometria-se/grizzly/commit/830abee4d7570dccb866f6bb256348cc154748a7" target="_blank">`830abee4`</a>: credential support for `ServiceBus*` and `BlobStorage*` resources (#318)
- <a href="https://github.com/Biometria-se/grizzly/commit/366f039803da3534d722e4cf44b619772adf4382" target="_blank">`366f0398`</a>: AAD refactoring (#317)
- <a href="https://github.com/Biometria-se/grizzly/commit/a5969568e0a895b322df7367ddbe0682ba6c9885" target="_blank">`a5969568`</a>: `ServiceBusUser` and `IteratorScenario` improvements (#316)
- <a href="https://github.com/Biometria-se/grizzly/commit/af9b164795caf3b3c4a900eaac764dc3f76b5922" target="_blank">`af9b1647`</a>: update databind version to 4.5.1 (#315)
- <a href="https://github.com/Biometria-se/grizzly/commit/8c561786c2b70d5a83373d3e60cdba14e70095cc" target="_blank">`8c561786`</a>: dry run (#314)
- <a href="https://github.com/Biometria-se/grizzly/commit/82eec240546f347a7953ee26f17b0bc6d950ff33" target="_blank">`82eec240`</a>: improved AST parsing of jinja2 templates (#313)
- <a href="https://github.com/Biometria-se/grizzly/commit/907cdaf2f07ad2d0215d69b210da46ff7def25e2" target="_blank">`907cdaf2`</a>: set variable task resolve file value (#311)
- <a href="https://github.com/Biometria-se/grizzly/commit/aa93dae77a408d0ade3f92a2663d22af0c22d9e7" target="_blank">`aa93dae7`</a>: disable sketchy dispatcher unittests (#312)
- <a href="https://github.com/Biometria-se/grizzly/commit/e9c5e3e79efe48b92792151c23ae98f68e2d4e71" target="_blank">`e9c5e3e7`</a>: azure-servicebus version 7.12.1 (#310)
- <a href="https://github.com/Biometria-se/grizzly/commit/5c5b2e5441e07a2bdd1680b89c555689b38c23a6" target="_blank">`5c5b2e54`</a>: execute python script step improvements (#309)
- <a href="https://github.com/Biometria-se/grizzly/commit/3e067ebe8414eca57e23a7d2ee92b367fc25719b" target="_blank">`3e067ebe`</a>: Execute python script runs at module level (#308)
- <a href="https://github.com/Biometria-se/grizzly/commit/a22acea86d3ef09663e1a97a9b3db53a32152111" target="_blank">`a22acea8`</a>: fix for incorrect step expression for inline script (#307)
- <a href="https://github.com/Biometria-se/grizzly/commit/dfdbd292cafdf646600fc91f54d0e51da43dc12e" target="_blank">`dfdbd292`</a>: initialize variable values from files (#306)

## v2.9.3

- <a href="https://github.com/Biometria-se/grizzly/commit/c74c22d084089c9938f9884c28c021ad653e0e68" target="_blank">`c74c22d0`</a>: influxdb: add additional tags to writes (#305)

## v2.9.2

- <a href="https://github.com/Biometria-se/grizzly/commit/53fe3e35c9587122aa8fd086944974af4dcc491f" target="_blank">`53fe3e35`</a>: resolve_variable: allow both environment configuration/variables and jinja variables in an input string (#304)

## v2.9.1

- <a href="https://github.com/Biometria-se/grizzly/commit/12724cf78d6928015be7559590021ad45df8d18f" target="_blank">`12724cf7`</a>: execute python scripts during behave phase of grizzly (#303)

## v2.9.0

- <a href="https://github.com/Biometria-se/grizzly/commit/7dfa643e4bd7abf53e83b069b94183c7cb0e58a3" target="_blank">`7dfa643e`</a>: custom dispatcher logic to be able to isolate user types to workers (#302)
- <a href="https://github.com/Biometria-se/grizzly/commit/d68318f0041975356487ceac725de8df0721e7db" target="_blank">`d68318f0`</a>: Support for gzipping payload sent to IoT hub (#301)
- <a href="https://github.com/Biometria-se/grizzly/commit/ee162bf71aa8a21bbf2d9bf658cfe7230d161ecd" target="_blank">`ee162bf7`</a>: support templating for `WaitBetweenTask` (#300)
- <a href="https://github.com/Biometria-se/grizzly/commit/73ecf04081e87e4b282a9c252968f91a0d1077c1" target="_blank">`73ecf040`</a>: change context variables during runtime via `SetVariableTask` (#292)
- <a href="https://github.com/Biometria-se/grizzly/commit/229092f5c82702c2f4da7b90bf4d1cc9d90a7939" target="_blank">`229092f5`</a>: handle ENAMETOOLONG when creating a request task with source in context text. (#290)
- <a href="https://github.com/Biometria-se/grizzly/commit/45af414eff866d209bac4f3b976719e76511a446" target="_blank">`45af414e`</a>: handle property validation on flat objects (#289)

## v2.8.0

- <a href="https://github.com/Biometria-se/grizzly/commit/0295702216b5ab0e45b31228b699d262cc43d119" target="_blank">`02957022`</a>: geventhttpclient does not support `cookies` in `request` (#288)
- <a href="https://github.com/Biometria-se/grizzly/commit/f0c635c2977eddcd0b39ff6d740be55f99d6f7df" target="_blank">`f0c635c2`</a>: async-messaged logging refatored (#287)
- <a href="https://github.com/Biometria-se/grizzly/commit/55da160b7fa1d631bf3fb38fe25c5e57442045cf" target="_blank">`55da160b`</a>: find used variables in assig expressions `{% .. %}` (#285)
- <a href="https://github.com/Biometria-se/grizzly/commit/ab8b7e20885852d98dac83bb2625c0a035550592" target="_blank">`ab8b7e20`</a>: rewrite of request events (#286)
- <a href="https://github.com/Biometria-se/grizzly/commit/05f63241030551faae6bbbccd0b423c3ad2ef57f" target="_blank">`05f63241`</a>: Update issue templates
- <a href="https://github.com/Biometria-se/grizzly/commit/8a44416e586c797a6bda10de5684f0d661404a33" target="_blank">`8a44416e`</a>: replace pylint+flake8 with ruff (#282)
- <a href="https://github.com/Biometria-se/grizzly/commit/b2101ede707d9dbeb24fcecbf364a4022e262261" target="_blank">`b2101ede`</a>: service bus client unquote bug (#279)
- <a href="https://github.com/Biometria-se/grizzly/commit/7a1f1eccbdc77f8044f311e762c3112e890a957e" target="_blank">`7a1f1ecc`</a>: BlobStorageUser: support for RECEIVE/GET (#277)
- <a href="https://github.com/Biometria-se/grizzly/commit/97869ec3caa5857421c14a86c60d214ad9de51b4" target="_blank">`97869ec3`</a>: major documentation overhaul (#276)

## v2.7.4

- <a href="https://github.com/Biometria-se/grizzly/commit/b4987581e98cddc0be0ffce805a0adfe8f7be1a2" target="_blank">`b4987581`</a>: update IBM MQ redist URL (#274)

## v2.7.3

- <a href="https://github.com/Biometria-se/grizzly/commit/cd70468af667c8d69bb7f06a9ba585a86b20123a" target="_blank">`cd70468a`</a>: bump pymqi version (#265)
- <a href="https://github.com/Biometria-se/grizzly/commit/cb6eab8b2c89b7d893a3d799762d9613ab0eb114" target="_blank">`cb6eab8b`</a>: AAD: check for error message before updating state (#264)
- <a href="https://github.com/Biometria-se/grizzly/commit/3116d546008a6295c4d0d75b9adeaabf294bf329" target="_blank">`3116d546`</a>: support for templating of `expected_matches` in response handlers (#263)

## v2.7.2

- <a href="https://github.com/Biometria-se/grizzly/commit/fe4d1d9ce0224e71abaf53cbe5d75b3ed9e3a7dd" target="_blank">`fe4d1d9c`</a>: software based TOTP for AAD user authentication (#262)
- <a href="https://github.com/Biometria-se/grizzly/commit/37c021c53edb03612a1a0bd6d628e862ede4c0a1" target="_blank">`37c021c5`</a>: keystore -- share testdata between scenarios (#261)
- <a href="https://github.com/Biometria-se/grizzly/commit/b97fe65a9252f16faf7b07f218c5bb15ef813b6a" target="_blank">`b97fe65a`</a>: do not fire ResponseEvent for exceptions that are `StopUser` or `RestartScenario` (#258)
- <a href="https://github.com/Biometria-se/grizzly/commit/5e7f4b455cc8663388b71182a2fc2cc11ad03249" target="_blank">`5e7f4b45`</a>: include code of conduct in generated documentation (#257)
- <a href="https://github.com/Biometria-se/grizzly/commit/74cd98a822c2facd779711efb456c979d0701d0a" target="_blank">`74cd98a8`</a>: Create code of conduct (#255)
- <a href="https://github.com/Biometria-se/grizzly/commit/7a8d79dfe71884dcfb3d28b9233464f915b1a694" target="_blank">`7a8d79df`</a>: async-messaged: reconnect to MQ queue manager if connection is broken (#253)
- <a href="https://github.com/Biometria-se/grizzly/commit/6ac36e6e8076e3d3df98bc55dc562cfe367f56c7" target="_blank">`6ac36e6e`</a>: add support for ISO 8601-ish format in DateTask (no separators) :( (#254)
- <a href="https://github.com/Biometria-se/grizzly/commit/aa91d5dd43e162c3d554d799cf2cc951c146077e" target="_blank">`aa91d5dd`</a>: add support for sub log directories in `requests/logs` (#252)

## v2.7.1

- <a href="https://github.com/Biometria-se/grizzly/commit/3e5085018ceb59a290a09cfebac0ece7bd233664" target="_blank">`3e508501`</a>: grizzly.auth.aad: flow token in step 2 now changes (#251)

## v2.7.0

- <a href="https://github.com/Biometria-se/grizzly/commit/9a3ca4e4e62583028f1b57fa996c606f6f8631de" target="_blank">`9a3ca4e4`</a>: bump versions (#249)
- <a href="https://github.com/Biometria-se/grizzly/commit/47a1201a2ad84f2ce9bcf138054ba62c4af33ad5" target="_blank">`47a1201a`</a>: Update issue templates (#245)
- <a href="https://github.com/Biometria-se/grizzly/commit/157797769c0fd32060bfbd650bd9ddd7434ff428" target="_blank">`15779776`</a>: persist flagged variables when test is stopping (#247)
- <a href="https://github.com/Biometria-se/grizzly/commit/b40cfcb20f6a12b20cfed46fb6db31130e113a8f" target="_blank">`b40cfcb2`</a>: refactoring of handling RequestTask (#246)
- <a href="https://github.com/Biometria-se/grizzly/commit/0c2cbab46534c91919c51918de9f064651683b53" target="_blank">`0c2cbab4`</a>: do not log `scenario.failure_exception` in `LoopTask` (#244)
- <a href="https://github.com/Biometria-se/grizzly/commit/52c72d1dd99512e03188a975e70d4a5cae1eab77" target="_blank">`52c72d1d`</a>: update novella (#243)
- <a href="https://github.com/Biometria-se/grizzly/commit/34b306266435f8d7e5edaa9cca6936dac028bdb3" target="_blank">`34b30626`</a>: refactoring regarding `GrizzlyContextScenario` references for users (#241)
- <a href="https://github.com/Biometria-se/grizzly/commit/a2ea894129cffbedb8a444b22b9f1baa391a96d8" target="_blank">`a2ea8941`</a>: response handlers should execute even though payload/metadata is empty (#242)
- <a href="https://github.com/Biometria-se/grizzly/commit/7edebccb473a504e9a5c5246d2a8e14444ae1813" target="_blank">`7edebccb`</a>: custom jinja2/templating filters (#240)
- <a href="https://github.com/Biometria-se/grizzly/commit/c624dcb77a6e8e697cac8fb75ac4f724c584cf8d" target="_blank">`c624dcb7`</a>: handling of SIGINT/SIGTERM to gracefully stop test (#238)

## v2.6.5

- <a href="https://github.com/Biometria-se/grizzly/commit/10942c9d83e70684b63ae1b467bad5897a501d6a" target="_blank">`10942c9d`</a>: only allow writing a complete row at a time in AtomicCsvWriter (#237)
- <a href="https://github.com/Biometria-se/grizzly/commit/79a6f3f519454c04d5a43f3e0a55798442ddefeb" target="_blank">`79a6f3f5`</a>: rewrite grizzly.tasks.clients.servicebus to support more than one parent (#236)

## v2.6.4

- <a href="https://github.com/Biometria-se/grizzly/commit/d41a2b157c5746916a1c31260db18a42df0f4090" target="_blank">`d41a2b15`</a>: catch exceptions from task `on_stop` and log as errors (#235)
- <a href="https://github.com/Biometria-se/grizzly/commit/b8bf9e659992a7f8c45e768efbdcc7540b6beeda" target="_blank">`b8bf9e65`</a>: AAD authentication improvements (#234)
- <a href="https://github.com/Biometria-se/grizzly/commit/df73fdcd1d58cf308d7cc18dd34fee3c6fbe265a" target="_blank">`df73fdcd`</a>: HTTP authentication support outside of `RestApiUser` (#231)
- <a href="https://github.com/Biometria-se/grizzly/commit/e9e62d4294c09af6d847172f00d0eea323529c7a" target="_blank">`e9e62d42`</a>: correct handling of return code from behave (#233)
- <a href="https://github.com/Biometria-se/grizzly/commit/56dc753184dd9175a53a5c389ef19957746d0d79" target="_blank">`56dc7531`</a>: use docker composer v2 in code-quality workflow (#230)

## v2.6.3

- <a href="https://github.com/Biometria-se/grizzly/commit/a52cfc5c15989624dc26fd0e623f0b06d8b75ba8" target="_blank">`a52cfc5c`</a>: async messaged improvments (#228)
- <a href="https://github.com/Biometria-se/grizzly/commit/fa51b3ce006759655fa746f837828e943878bbbd" target="_blank">`fa51b3ce`</a>: wrapper tasks implements on_start/stop (#227)
- <a href="https://github.com/Biometria-se/grizzly/commit/fd245ca6b97d35b543cc257b3f43c9dce33f83b1" target="_blank">`fd245ca6`</a>: support for jsonpath expression filter on "flat" objects (#225)
- <a href="https://github.com/Biometria-se/grizzly/commit/f289d6d33475778c89ba5c2e8e621594d5945003" target="_blank">`f289d6d3`</a>: prefix subscription name with id of user instance (#224)
- <a href="https://github.com/Biometria-se/grizzly/commit/c4e8acf247657df1baa50803f253bc9fa13c6610" target="_blank">`c4e8acf2`</a>: docs generate sort version (#223)
- <a href="https://github.com/Biometria-se/grizzly/commit/c79b59274ada4031fbb9755b374e77bca95682db" target="_blank">`c79b5927`</a>: E2E distributed tests takes too long time (#222)
- <a href="https://github.com/Biometria-se/grizzly/commit/55fb70609490b836bb59c2a376761bf96b256d7a" target="_blank">`55fb7060`</a>: backing out PR #200, since interfere with logging of other tasks. (#221)
- <a href="https://github.com/Biometria-se/grizzly/commit/61db08891ac5d437cd6bb65c92e4e4b77c3d2f0d" target="_blank">`61db0889`</a>: abort if `async-messaged` process is gone (#219)
- <a href="https://github.com/Biometria-se/grizzly/commit/9dbac3e5aa81e5d1b974acc80315d5228e49b98a" target="_blank">`9dbac3e5`</a>: metadata is a dict, but when saved in a variable it should be a (json) string (#218)
- <a href="https://github.com/Biometria-se/grizzly/commit/c557a6ebadfdfc64405aaa1829c1d1c23eafadec" target="_blank">`c557a6eb`</a>: client task implementation for saving response metadata in variable (#217)

## v2.6.2

- <a href="https://github.com/Biometria-se/grizzly/commit/5a00f7069b84b09f74386dcc2e414984173186fb" target="_blank">`5a00f706`</a>: fixed documentation for missing path and fragment variables (#215)
- <a href="https://github.com/Biometria-se/grizzly/commit/7d653c53c90132a3b660bae4ade20bab241aee92" target="_blank">`7d653c53`</a>: get current tasks the right way (#214)
- <a href="https://github.com/Biometria-se/grizzly/commit/1ac1c4d143f3ae274e74114d316ab78ceb6055c7" target="_blank">`1ac1c4d1`</a>: RestApiUser, oauth2 token v2.0 flow (#213)

## v2.6.1

- <a href="https://github.com/Biometria-se/grizzly/commit/d541500e65adb41736133f3b9ca889e0ed060bc7" target="_blank">`d541500e`</a>: servicebus client task (#212)

## v2.6.0
- <a href="https://github.com/Biometria-se/grizzly/commit/c01805fbbd209a28ad91ae3ccf34ce6afedf9edd" target="_blank">`c01805fb`</a>: implementation of AtomicCsvWriter (#211)
- <a href="https://github.com/Biometria-se/grizzly/commit/1c2b824d5b921a77d1ca0ed493f409c69c147bc0" target="_blank">`1c2b824d`</a>: code maintenance 2023-03 (#209)
- <a href="https://github.com/Biometria-se/grizzly/commit/b0e3a68bc4349d3bed19c32d87cc439b304710f0" target="_blank">`b0e3a68b`</a>: task `on_start` and `on_stop` functionality (#208)
- <a href="https://github.com/Biometria-se/grizzly/commit/16efa80af2aa66f90d5b9b8e95183e7efe16c3e6" target="_blank">`16efa80a`</a>: grizzly.user implementation of on_start and on_stop (#207)
- <a href="https://github.com/Biometria-se/grizzly/commit/8906711a335e0968ec3dcfb428e4a9f7f9b752dc" target="_blank">`8906711a`</a>: use packaging.version instead of distutils.version (#206)
- <a href="https://github.com/Biometria-se/grizzly/commit/9e6eeec1ca9ba621fea4b162103647bb8038fc79" target="_blank">`9e6eeec1`</a>: rename `grizzly.environment` to `grizzly.behave` (#205)
- <a href="https://github.com/Biometria-se/grizzly/commit/47a61445b54e65202d1b943d11fb6eb6566186c9" target="_blank">`47a61445`</a>: updated locust version (#204)
- <a href="https://github.com/Biometria-se/grizzly/commit/69938441408146b5ea85dda1bd66ed8cdb5b4f3a" target="_blank">`69938441`</a>: suppress error procuded in wrapped tasks to be logged to the error summary (#203)

## v2.5.11

- <a href="https://github.com/Biometria-se/grizzly/commit/85650a559f625feb71cb16604de4242589a5a1d4" target="_blank">`85650a55`</a>: set a fixed time for one iteration of a scenario (#201)
- <a href="https://github.com/Biometria-se/grizzly/commit/30c9eadb8bbb1dda71cf0f5c549977d94c0ddeaf" target="_blank">`30c9eadb`</a>: do not log internal flow exceptions as errors in conditionals (#200)
- <a href="https://github.com/Biometria-se/grizzly/commit/a7e13f04eb630f7832b68098649c41d3877a5989" target="_blank">`a7e13f04`</a>: improvements of information printed when grizzly is running locust (#198)
- <a href="https://github.com/Biometria-se/grizzly/commit/5f09a9dce638ad1027be619f7bb2cfad3fecceeb" target="_blank">`5f09a9dc`</a>: fixed tags sorted incorrectly (#191)
- <a href="https://github.com/Biometria-se/grizzly/commit/8594a4fa5c6a6c3f9640ef85ca25e50be3e3925d" target="_blank">`8594a4fa`</a>: improved UntilTask traceability (#194)

## v2.5.10

- <a href="https://github.com/Biometria-se/grizzly/commit/da7eb36bf6d5ecf0a475cf68986ce7f0450c973c" target="_blank">`da7eb36b`</a>: async-messaged: gracefully close handler connections when terminating… (#190)

## v2.5.9

- <a href="https://github.com/Biometria-se/grizzly/commit/3241411829a63d06ad820fb36565f7876fc9a1dd" target="_blank">`32414118`</a>: AtomicServiceBus: add argument `consume` (#189)

## v2.5.8

- <a href="https://github.com/Biometria-se/grizzly/commit/1334c917ff1eaf11d9576d64e58fa8ca76a32f1e" target="_blank">`1334c917`</a>: updated shield.io url for github workflow status (#188)
- <a href="https://github.com/Biometria-se/grizzly/commit/595601939ac459762df1ed5b71d69f9a82a86bff" target="_blank">`59560193`</a>: IoT hub, error handling (#187)

## v2.5.7

- <a href="https://github.com/Biometria-se/grizzly/commit/fc436358ad0d6ae4dd0af87903cccd29556e2b9b" target="_blank">`fc436358`</a>: tag measurements with scenario they belong to (#185)
- <a href="https://github.com/Biometria-se/grizzly/commit/67abbb504f36d0fa8664d047e37e0609a00b3be3" target="_blank">`67abbb50`</a>: write user count per user class name to influx every 5 seconds (#184)

## v2.5.6

- <a href="https://github.com/Biometria-se/grizzly/commit/516b119fc5409f8b0530095bbecf0b41017335c2" target="_blank">`516b119f`</a>: background variable declaration (#183)
- <a href="https://github.com/Biometria-se/grizzly/commit/2ae0c0b2dc36ee98b13eba46ee0229a47181b407" target="_blank">`2ae0c0b2`</a>: updated workflow actions to remove warnings (#182)

## v2.5.5

- <a href="https://github.com/Biometria-se/grizzly/commit/2a4041eb04b7fd463c936378f8d83346eade24be" target="_blank">`2a4041eb`</a>: csv logging (#181)

## v2.5.4

- <a href="https://github.com/Biometria-se/grizzly/commit/65de8865261abb5f568323b9cc57ebd7096f1443" target="_blank">`65de8865`</a>: concurrency fixes in MQ and SB related code (#180)

## v2.5.3

- <a href="https://github.com/Biometria-se/grizzly/commit/d9c891992269ac25560fe7cf7526d9e9f8b5474c" target="_blank">`d9c89199`</a>: get messages from MQ with SYNCPOINT and configuration of max message size (#179)
- <a href="https://github.com/Biometria-se/grizzly/commit/784f6b899bf1933f67d825cafd235dce4dc2ad53" target="_blank">`784f6b89`</a>: IotHubUser, for uploading files to Azure IoT Hub (#177)
- <a href="https://github.com/Biometria-se/grizzly/commit/1a4a826f05ad4f128fff6ffef1171c2b62ef2f56" target="_blank">`1a4a826f`</a>: improved support for jinja2 expressions (#176)
- <a href="https://github.com/Biometria-se/grizzly/commit/c28e52d841429b150b1c898650444710a8d9deb8" target="_blank">`c28e52d8`</a>: `grizzly.tasks.client.messagequeue` needs unique worker for each instance of a scenario (#175)
- <a href="https://github.com/Biometria-se/grizzly/commit/5cceb5778f5d9c32975ecbffa12b02af968c045e" target="_blank">`5cceb577`</a>: print returncode of locust to stdout (#174)

## v2.5.2

- <a href="https://github.com/Biometria-se/grizzly/commit/70d1d2454f8b81503ea0174bcb578a8e152f8e68" target="_blank">`70d1d245`</a>: azure.servicebus receiver sometimes returns no message, even if there... (#173)
- <a href="https://github.com/Biometria-se/grizzly/commit/7b63d1b61922fb3a88b757e13609685160e1c3ef" target="_blank">`7b63d1b6`</a>: better handling of arthmetic when parsing out variables from templates (#172)
- <a href="https://github.com/Biometria-se/grizzly/commit/caf9693da2abc4bac3d75545865d2322dbb7e4c0" target="_blank">`caf9693d`</a>: __on_consumer__ testdata variables needs information about current scenario (#171)

## v2.5.1

- <a href="https://github.com/Biometria-se/grizzly/commit/599db8eb063bfa8d3265563378374cbc0c8dc1d7" target="_blank">`599db8eb`</a>: instructions in example docs on how to install vscode extension (#169)
- <a href="https://github.com/Biometria-se/grizzly/commit/e57eea5da64d87ff2c09ebd0ec8e43bfaf866b85" target="_blank">`e57eea5d`</a>: corrected grizzly-cli run commands in example (#167)
- <a href="https://github.com/Biometria-se/grizzly/commit/161af06769a737856b8c8163261d657674cf811c" target="_blank">`161af067`</a>: async-messaged returns un-encoded RFH2 payload after PUT (#165)
- <a href="https://github.com/Biometria-se/grizzly/commit/7d034b2672d57e045d08329398cce0aaa5c6f58e" target="_blank">`7d034b26`</a>: removed deprecated set-output commands in workflow (#164)
- <a href="https://github.com/Biometria-se/grizzly/commit/5a35ffef4408a7de103ac3335f016c65d3c98886" target="_blank">`5a35ffef`</a>: more MQ information (#162)

## v2.5.0

- <a href="https://github.com/Biometria-se/grizzly/commit/ef6a4675752e74ed7f12af07886d9ac0d2f8b9e0" target="_blank">`ef6a4675`</a>: move out `docs` extras from pyproject.toml (#158)
- <a href="https://github.com/Biometria-se/grizzly/commit/d4880ee214e2fd26eb1196f548c7b581fc2502d8" target="_blank">`d4880ee2`</a>: env conf inline resolving and "generic" `UntilRequestTask` (#157)
- <a href="https://github.com/Biometria-se/grizzly/commit/7285294b58fb4a5ff8e31e0202958c623dd81cca" target="_blank">`7285294b`</a>: refactoring of get_templates (#156)
- <a href="https://github.com/Biometria-se/grizzly/commit/b694acaaf0e18581573eaf06e3f453846fb02e66" target="_blank">`b694acaa`</a>: implementation of %g (GUID/UUID) formatter for AtomicRandomString (#152)
- <a href="https://github.com/Biometria-se/grizzly/commit/a385484263953caf4de18d7c01470e4291cbb4bc" target="_blank">`a3854842`</a>: Post XML, multipart/form-data and metadata per request (#151)
- <a href="https://github.com/Biometria-se/grizzly/commit/10883c87407bba7119d04fe28f071495769fdd17" target="_blank">`10883c87`</a>: dependency update 2209 (#150)
- <a href="https://github.com/Biometria-se/grizzly/commit/1ce3a43ca5083dc01aabda48138df23f6c4ffc9e" target="_blank">`1ce3a43c`</a>: persist variable values between runs (override initial value based on previous runs) (#149)
- <a href="https://github.com/Biometria-se/grizzly/commit/b39247e8872f8f9c54e758de811f592477f5272b" target="_blank">`b39247e8`</a>: declared and found variable cross-check (#148)
- <a href="https://github.com/Biometria-se/grizzly/commit/e4e353deab8e709896a641e1484713bace4ca5ae" target="_blank">`e4e353de`</a>: fixed broken release workflow (#147)

## v2.4.6

- <a href="https://github.com/Biometria-se/grizzly/commit/1a6e07da1dd4db0cf3032103699bdfab6e9ef649" target="_blank">`1a6e07da`</a>: fix missing variables due to filter (pipe) in templates (#145)
- <a href="https://github.com/Biometria-se/grizzly/commit/e7e606b7abce9839e8d1fa3f5e7f9dc03973fee3" target="_blank">`e7e606b7`</a>: checkout release tag correctly so edit url in docs is correct (#146)
- <a href="https://github.com/Biometria-se/grizzly/commit/cc5bf649d2b6c1eb8618c9264e2b4d834f9696d3" target="_blank">`cc5bf649`</a>: added documentation for editor support (#144)
- <a href="https://github.com/Biometria-se/grizzly/commit/c37140aa13f49fe434ef5d59414c04bcbde3c62c" target="_blank">`c37140aa`</a>: correct typings for release workflow inputs (#143)
- <a href="https://github.com/Biometria-se/grizzly/commit/c3ed2eb7e7ba98a17df7c789019b572cf6559b51" target="_blank">`c3ed2eb7`</a>: document response handler expression arguments (#142)
- <a href="https://github.com/Biometria-se/grizzly/commit/3d2aa051267be00088533664bc85f688d3c5adcb" target="_blank">`3d2aa051`</a>: documentation of metadata comments in feature files (#141)
- <a href="https://github.com/Biometria-se/grizzly/commit/5db8bfe307ab85a4b8fdb0cc19da68977eef07ac" target="_blank">`5db8bfe3`</a>: allow templating strings as input to WaitTask (#140)
- <a href="https://github.com/Biometria-se/grizzly/commit/e297da03b905db95664e5b6e3cd2af84bcd46269" target="_blank">`e297da03`</a>: e2e dist (#139)
- <a href="https://github.com/Biometria-se/grizzly/commit/7a491161ff7d8602dc6a140984cf8701f786e93c" target="_blank">`7a491161`</a>: run e2e tests distributed (#138)
- <a href="https://github.com/Biometria-se/grizzly/commit/b24db98bf23a1f0670e422386a68a04619db1e88" target="_blank">`b24db98b`</a>: create zmq socket for each request (#136)
- <a href="https://github.com/Biometria-se/grizzly/commit/9777db829168be6cb3f897ccc4fa96b8b3802fec" target="_blank">`9777db82`</a>: fix for last task not being executed when user is stopping (#135)

## v2.4.5

- <a href="https://github.com/Biometria-se/grizzly/commit/eba5ba1dc38a384d41324468fae086a9d8930029" target="_blank">`eba5ba1d`</a>: no testdata address, when running distributed (#134)
- <a href="https://github.com/Biometria-se/grizzly/commit/606c7a801c7301df37588e96e89d43a561de2756" target="_blank">`606c7a80`</a>: changed mq version (#132)

## v2.4.4

- <a href="https://github.com/Biometria-se/grizzly/commit/501b5960bb0a6a50fcfe381ede9b826f68cc4b0e" target="_blank">`501b5960`</a>: allow an arbritary number of matches (#130)
- <a href="https://github.com/Biometria-se/grizzly/commit/d8406a558d6de7790432264d1e92332694937f1a" target="_blank">`d8406a55`</a>: loop task (#129)
- <a href="https://github.com/Biometria-se/grizzly/commit/a7928eea48270a63b4bc58dca8806ff6a4d614ed" target="_blank">`a7928eea`</a>: TransformerContentType should be permutated in y direction (#128)
- <a href="https://github.com/Biometria-se/grizzly/commit/2bb8c04585a247464935173debe2f2e597c2e167" target="_blank">`2bb8c045`</a>: updated dependencies due to lxml security fix (#127)
- <a href="https://github.com/Biometria-se/grizzly/commit/aea3d97bb4110460de980b09dd0c7fc2ab9344e0" target="_blank">`aea3d97b`</a>: grizzly implementation of print_percentile_stats (#125)
- <a href="https://github.com/Biometria-se/grizzly/commit/4ba8066d3a779f7bfbabbde5762eb5bae825fea9" target="_blank">`4ba8066d`</a>: annotate non-enum custom types with `__vector__` (#124)
- <a href="https://github.com/Biometria-se/grizzly/commit/fa4a2b16d39a6e74991728d7cb44d7296e0fcc3c" target="_blank">`fa4a2b16`</a>: annotations for enums used in step expressions (#123)

## v2.4.3

- <a href="https://github.com/Biometria-se/grizzly/commit/27ebe1086eadc4344e86011634bde360c624730c" target="_blank">`27ebe108`</a>: mq rfh2 support (#122)
- <a href="https://github.com/Biometria-se/grizzly/commit/8b794ad1d9d35b891162beb0856f2c6aa2ad735e" target="_blank">`8b794ad1`</a>: create list of tasks for pointer when switching (#121)
- <a href="https://github.com/Biometria-se/grizzly/commit/3e16160cd2f9d5ad12008376663b188fb2c2941d" target="_blank">`3e16160c`</a>: fixed examples for conditional task (#120)

## v2.4.2

- <a href="https://github.com/Biometria-se/grizzly/commit/da3c335d7d34cd5e4fdda6bdcfc6b76c146d6abe" target="_blank">`da3c335d`</a>: conditional tasks task (#119)
- <a href="https://github.com/Biometria-se/grizzly/commit/cbcb1b8b44682330f82bb4d24904fb6601b6f1b0" target="_blank">`cbcb1b8b`</a>: new location for logo in repo (#118)

## v2.4.1

- <a href="https://github.com/Biometria-se/grizzly/commit/1ce7f0add9f0b10f6327e7f65338ab3a4b322014" target="_blank">`1ce7f0ad`</a>: fix for documentation build and deploy (#117)
- <a href="https://github.com/Biometria-se/grizzly/commit/484f80101d5aeb7c35b9a7fc2b096e5e3cdd5edb" target="_blank">`484f8010`</a>: changed to novella build backend and improved documentation (#116)

## v2.4.0

- <a href="https://github.com/Biometria-se/grizzly/commit/8e026ad8c0e1564d88c9ee786b5c02f2d99f9416" target="_blank">`8e026ad8`</a>: updated dependencies (#115)
- <a href="https://github.com/Biometria-se/grizzly/commit/5c747f53ed3f23ff1bebe75d74fa0c57f782d2dd" target="_blank">`5c747f53`</a>: request wait task (#114)
- <a href="https://github.com/Biometria-se/grizzly/commit/6684c35735d0d99475a48ef71e1aa37a5f7bc1ad" target="_blank">`6684c357`</a>: `TimerTask` to measure "response time" for a group of tasks (#113)
- <a href="https://github.com/Biometria-se/grizzly/commit/5ceca1e203a99e82172473494f47697a8622a3e6" target="_blank">`5ceca1e2`</a>: bug fixes in `BlobStorageClient.put` and scenario iterator (#112)
- <a href="https://github.com/Biometria-se/grizzly/commit/53ed0ad579badb6baf0a248e61a6438fe9431661" target="_blank">`53ed0ad5`</a>: remove debug print statement (#111)
- <a href="https://github.com/Biometria-se/grizzly/commit/4f0f01f7a18685dc390e39b63f70fcd199e678ce" target="_blank">`4f0f01f7`</a>: sort request statistics per scenario (#110)
- <a href="https://github.com/Biometria-se/grizzly/commit/35091c96361a9f876f7e5ffbb9aae8b3822d5cdd" target="_blank">`35091c96`</a>: `grizzly.tasks.client` must have a name (#109)

## v2.3.1

- <a href="https://github.com/Biometria-se/grizzly/commit/c754a8aa58662bed0cd5aec1a8aab52031943ddc" target="_blank">`c754a8aa`</a>: support ISO 8601 formated DateTime and Time (#107)
- <a href="https://github.com/Biometria-se/grizzly/commit/e33cd9410d8743cb64d8803a7ed4d4cf44869ec5" target="_blank">`e33cd941`</a>: client task ibm messagequeue (#103)
- <a href="https://github.com/Biometria-se/grizzly/commit/eabe7b8f6cd7098914a1473928135c1e05758af7" target="_blank">`eabe7b8f`</a>: possibility to implement custom (non-grizzly) atomic variables (#102)
- <a href="https://github.com/Biometria-se/grizzly/commit/e4193dd4bf2d0acd7b8c6dd77ffc918ee3dbf249" target="_blank">`e4193dd4`</a>: possibility to register custom locust message types and handlers (#101)
- <a href="https://github.com/Biometria-se/grizzly/commit/13b6c44435c6630257ebed8241ae661c04bdf7d4" target="_blank">`13b6c444`</a>: scenario statistics (#98)
- <a href="https://github.com/Biometria-se/grizzly/commit/ba37b60adc5e78ecfa87846bace0836d70d61a11" target="_blank">`ba37b60a`</a>: validating response step expression updated (#97)
- <a href="https://github.com/Biometria-se/grizzly/commit/89149d9da1b8d65dc32135ad9003d6020f035a2a" target="_blank">`89149d9d`</a>: "end to end" (E2E) test cases (#95)

## v2.3.0

- <a href="https://github.com/Biometria-se/grizzly/commit/b4fc150f9b38cd75f9be6d88b70165cd0f108313" target="_blank">`b4fc150f`</a>: Change scenario hash to numerical index (#94)
- <a href="https://github.com/Biometria-se/grizzly/commit/6ecc7ab35e3c06ea55e2b321723f28fc600c228d" target="_blank">`6ecc7ab3`</a>: support for parallell tasks (#92)
- <a href="https://github.com/Biometria-se/grizzly/commit/2f7025d9f4046688c2dd9380a3c71183f0705bfe" target="_blank">`2f7025d9`</a>: make tests runnable on windows (#91)

## v2.2.1

- <a href="https://github.com/Biometria-se/grizzly/commit/0ae1b373aecf5dcb52efd6e93cdba548af986ed7" target="_blank">`0ae1b373`</a>: automagically check pypi for package url if unknown (#90)
- <a href="https://github.com/Biometria-se/grizzly/commit/e6ef9d8f21c8ae16cb8b0e8b3c8dd7df904bfa2c" target="_blank">`e6ef9d8f`</a>: install additional script dependencies correct (#89)
- <a href="https://github.com/Biometria-se/grizzly/commit/c0a53532859204964e461b51bb41ff6b1d6bd444" target="_blank">`c0a53532`</a>: add support for requirements-script.txt to pip-sync wrapper (#88)
- <a href="https://github.com/Biometria-se/grizzly/commit/67f3d2690b47528d490d419c9db6b550c40fca65" target="_blank">`67f3d269`</a>: install and cache script requirements (#87)
- <a href="https://github.com/Biometria-se/grizzly/commit/ddd9929cef7d44d817bcfe2b8d254d96d48e3dd7" target="_blank">`ddd9929c`</a>: restructuring of code-quality workflow (#86)

## v2.2.0

- <a href="https://github.com/Biometria-se/grizzly/commit/d45b62d1369e0a7b6f8f118d230581026762c786" target="_blank">`d45b62d1`</a>: github action action-push-tag@v1 is broken (#85)
- <a href="https://github.com/Biometria-se/grizzly/commit/283fe3fe3d44237759e3a77017546de4c6e2b8b2" target="_blank">`283fe3fe`</a>: bug fix for iterations to stop when not finishing (#84)
- <a href="https://github.com/Biometria-se/grizzly/commit/0ed596f2e5e7257d673b88cc61967ac04f7176ce" target="_blank">`0ed596f2`</a>: Feature/clients tasks (#81)
- <a href="https://github.com/Biometria-se/grizzly/commit/6e6496ed58e3e25085ec9da406868fe1dba1a84b" target="_blank">`6e6496ed`</a>: create docs/licenses if it does not exist, before trying to write md file (#79)
- <a href="https://github.com/Biometria-se/grizzly/commit/0dd489a3c4cab4aaff1fedb8dfb405eebbd799d6" target="_blank">`0dd489a3`</a>: restructure of documentation (#78)

## v2.1.0

- <a href="https://github.com/Biometria-se/grizzly/commit/c4260a9e69c74ce0a154711529ffc5f27bec543e" target="_blank">`c4260a9e`</a>: Feature/response handlers (#77)
- <a href="https://github.com/Biometria-se/grizzly/commit/8278d17ab74a71838438d3a369133e4d9dd13d5b" target="_blank">`8278d17a`</a>: step to set user metadata/header key values (#75)
- <a href="https://github.com/Biometria-se/grizzly/commit/11de9b7f2064d849fbcf27e692451c24c5994de3" target="_blank">`11de9b7f`</a>: clearer job name in code-quality workflow (#70)
- <a href="https://github.com/Biometria-se/grizzly/commit/b6845b79b15446dfbed9c6106881f50453d70dc4" target="_blank">`b6845b79`</a>: Feature/issue 64 pep518 (#69)
- <a href="https://github.com/Biometria-se/grizzly/commit/fbd96cfafed4279217ff5861063f66dc34e4362a" target="_blank">`fbd96cfa`</a>: Feature/issue 61 pytz (#68)

## v2.0.0

- <a href="https://github.com/Biometria-se/grizzly/commit/af5e639b352b6e28ed88342f21d316d9e3419f93" target="_blank">`af5e639b`</a>: twine: command not found (#67)
- <a href="https://github.com/Biometria-se/grizzly/commit/052a5ab9d117006c2319c3deb842b3ad36c8caa5" target="_blank">`052a5ab9`</a>: Feature/dependency update round 2 (#66)
- <a href="https://github.com/Biometria-se/grizzly/commit/d4a3b935fe3e9a425ccb776b266a1928b7c61c35" target="_blank">`d4a3b935`</a>: Feature/mq heartbeat (#65)
- <a href="https://github.com/Biometria-se/grizzly/commit/884b6761ca353d3e640ddf97e15860fff7470376" target="_blank">`884b6761`</a>: Feature/dependencies update (#63)
- <a href="https://github.com/Biometria-se/grizzly/commit/02c8804578a46d2df2a7faf32b46572a52e23418" target="_blank">`02c88045`</a>: Plain text transformer fix, plus added rendering of date offset value (#62)
- <a href="https://github.com/Biometria-se/grizzly/commit/001cc5e52818cc16606c7320094d71d2cf53eb38" target="_blank">`001cc5e5`</a>: Bug/until task aborts (#60)

## v1.5.3

- <a href="https://github.com/Biometria-se/grizzly/commit/7c2b7a91a440b52704fb57a982f35658933325df" target="_blank">`7c2b7a91`</a>: Feature/cli docs (#59)
- <a href="https://github.com/Biometria-se/grizzly/commit/a53f915902ac9eff12638a24f5529db0710c5aac" target="_blank">`a53f9159`</a>: Bug/until stops too soon (#58)
- <a href="https://github.com/Biometria-se/grizzly/commit/f53bb8b82e69b976d186152d7ce2fafd4b29c2a2" target="_blank">`f53bb8b8`</a>: removed debug print statements (#56)
- <a href="https://github.com/Biometria-se/grizzly/commit/fb9f493329de462a2bf84a8ded6014ab093eb8c1" target="_blank">`fb9f4933`</a>: removed debug print statements (#56)

## v1.5.2

- <a href="https://github.com/Biometria-se/grizzly/commit/fb9f493329de462a2bf84a8ded6014ab093eb8c1" target="_blank">`fb9f4933`</a>: removed debug print statements (#56)

## v1.5.1

- <a href="https://github.com/Biometria-se/grizzly/commit/a28575699d920e1d336d5a988e6cb52ccbb43d5b" target="_blank">`a2857569`</a>: verify_certificates bug fixed (#55)

## v1.5.0

- <a href="https://github.com/Biometria-se/grizzly/commit/1c57c7f0885daa9799b89cfba3528a3b1af9590c" target="_blank">`1c57c7f0`</a>: Feature/restart scenario (#54)
- <a href="https://github.com/Biometria-se/grizzly/commit/9326218f14aca5eb807806af847ba2fab796b561" target="_blank">`9326218f`</a>: MQ concurrency fix (#53)
- <a href="https://github.com/Biometria-se/grizzly/commit/cc178860e60e2ab15ca5a6ba26644076e64c842f" target="_blank">`cc178860`</a>: renamed grizzly.tasks to grizzly.scenarios (#52)
- <a href="https://github.com/Biometria-se/grizzly/commit/281e9beb8c7ede15d256733793e51ff984e01de7" target="_blank">`281e9beb`</a>: fixed bug in parse_arguments if an argument value contained comma (#51)

## v1.4.4

- <a href="https://github.com/Biometria-se/grizzly/commit/e13ff471cce7e5aa589132151eb6317694606861" target="_blank">`e13ff471`</a>: Feature/date parse task (#50)
- <a href="https://github.com/Biometria-se/grizzly/commit/d16ef8e1a27c7665f987899f696f3f237c7f4f12" target="_blank">`d16ef8e1`</a>: handle exceptions during until retries (#49)
- <a href="https://github.com/Biometria-se/grizzly/commit/0994f9af9510badc36c3f7afe1b35df3f7baa127" target="_blank">`0994f9af`</a>: increased test coverage (#48)
- <a href="https://github.com/Biometria-se/grizzly/commit/eaaeab22d0178ffbbf97b6ef0d34f8b20e55086d" target="_blank">`eaaeab22`</a>: Fix for doing retry upon receiving MQRC_TRUNCATED_MSG_FAILED while browsing messages (#47)
- <a href="https://github.com/Biometria-se/grizzly/commit/88202428cc51091610fef229f71595543af270f9" target="_blank">`88202428`</a>: support for templating in arguments in condition (#46)

## v1.4.3

- <a href="https://github.com/Biometria-se/grizzly/commit/3e47adbdd512a895edb6af2168b6b5e79864d2db" target="_blank">`3e47adbd`</a>: fixed alignment i scenario summary (#45)

## v1.4.2

- <a href="https://github.com/Biometria-se/grizzly/commit/e976144bb4cc671bfb84be40cd137a9e609f5ae6" target="_blank">`e976144b`</a>: Bug/async messaged logging (#44)
- <a href="https://github.com/Biometria-se/grizzly/commit/b4b0be63b5cf614bfa85b12595773257a3727a54" target="_blank">`b4b0be63`</a>: Feature/request until (#42)

## v1.4.1

- <a href="https://github.com/Biometria-se/grizzly/commit/e6d8fb3f6f7a30fb90ad028d41b5685e148336c6" target="_blank">`e6d8fb3f`</a>: fix for ensuring correct data type in metric written to influx (#41)

## v1.4.0

- <a href="https://github.com/Biometria-se/grizzly/commit/26b813054a504c6b56954dc242c2d241a8693d78" target="_blank">`26b81305`</a>: print start and stop date and time when finished (#40)
- <a href="https://github.com/Biometria-se/grizzly/commit/4aef0eef36623b040c7528828d645e86345bec48" target="_blank">`4aef0eef`</a>: request response_time fixes (#39)
- <a href="https://github.com/Biometria-se/grizzly/commit/fb95268ffb5613e9aca7d9caa75d3eee177b212b" target="_blank">`fb95268f`</a>: updated dependencies and devcontainer (#38)
- <a href="https://github.com/Biometria-se/grizzly/commit/f98d904d07274da1d1a822517b9dada2c96e5161" target="_blank">`f98d904d`</a>: Feature/scenario info (#37)

## v1.3.1

- <a href="https://github.com/Biometria-se/grizzly/commit/73827bf4ffc3a9ee642b25554cd651d516b322d3" target="_blank">`73827bf4`</a>: error logging in transformer class (#35)
- <a href="https://github.com/Biometria-se/grizzly/commit/3e5e03aaa483b5b42e1991c9565a58b9c66224cf" target="_blank">`3e5e03aa`</a>: init racecondition (#36)
- <a href="https://github.com/Biometria-se/grizzly/commit/dd0c75a5a18dbedb99e882d27b1aea36a9c9b4ad" target="_blank">`dd0c75a5`</a>: support for request template files in combination with data tables (#34)
- <a href="https://github.com/Biometria-se/grizzly/commit/8d56f4fe20da7bd21e4fae6b64c6c48e1a1612ca" target="_blank">`8d56f4fe`</a>: Feature/parameterize more (#33)

## v1.3.0

- <a href="https://github.com/Biometria-se/grizzly/commit/60f399884fef5b9e8b08701f2b606d06d64421a4" target="_blank">`60f39988`</a>: transparent support for setting content type in endpoint (#32)
- <a href="https://github.com/Biometria-se/grizzly/commit/423bc99401f47738a67be0d5118b7a3cb5369d0e" target="_blank">`423bc994`</a>: expression support for service bus functionality (#31)
- <a href="https://github.com/Biometria-se/grizzly/commit/2e2695df05163ce81107a01f9a80cedbd57e32c1" target="_blank">`2e2695df`</a>: support for offset in AtomicDate (#30)
- <a href="https://github.com/Biometria-se/grizzly/commit/41b78e8930465908f2ecaf5fec9aa209d431954f" target="_blank">`41b78e89`</a>: AtomicMessageQueue content type support (#29)
- <a href="https://github.com/Biometria-se/grizzly/commit/979d4bbb8af7564c51f3d473e934e2cb0020fd49" target="_blank">`979d4bbb`</a>: unified arguments handling through out grizzly (#28)
- <a href="https://github.com/Biometria-se/grizzly/commit/6fad96e4a697d252fc3fe53b2031bc10c04d764e" target="_blank">`6fad96e4`</a>: simplified AtomicMessageQueue and AtomicServiceBus (#27)
- <a href="https://github.com/Biometria-se/grizzly/commit/64888c054333a57dc187716880cde8c49771d795" target="_blank">`64888c05`</a>: implementation of getter tasks (http) (#26)
- <a href="https://github.com/Biometria-se/grizzly/commit/2d6862d7d4cfee81ccc51d4f1503e1dcdf993edd" target="_blank">`2d6862d7`</a>: Restored dummy_pymqi.py, the added stuff wasn't needed
- <a href="https://github.com/Biometria-se/grizzly/commit/4f21a8d38448560b1ff078e6ca0e7be0dcf88e24" target="_blank">`4f21a8d3`</a>: MessageQueueUser: get messages that matches expression
- <a href="https://github.com/Biometria-se/grizzly/commit/1e1edc448e97812881ad09c3fc56be2fdfd0f524" target="_blank">`1e1edc44`</a>: run code quality workflow when PR is updated
- <a href="https://github.com/Biometria-se/grizzly/commit/6dc839c9d74363e30c703e5fc5e26c34f7d7f8cd" target="_blank">`6dc839c9`</a>: corrected sentence in documentation for SleepTask

## v1.2.0

- <a href="https://github.com/Biometria-se/grizzly/commit/6ee5fffe47ed5ba305cdbd3f6db43a52448b5fae" target="_blank">`6ee5fffe`</a>: added documentation for the different task types
- <a href="https://github.com/Biometria-se/grizzly/commit/1b91d79b8e850ab36bb5bfd663a1e5ea47f37e36" target="_blank">`1b91d79b`</a>: implementation of AtomicServiceBus variable
- <a href="https://github.com/Biometria-se/grizzly/commit/b867d471710c64d609ffa34096d0fd93e5d5c4d3" target="_blank">`b867d471`</a>: ServiceBus support in async-messaged
- <a href="https://github.com/Biometria-se/grizzly/commit/83c0ac9c077fbcbbcccba38e481b0abf0325a2aa" target="_blank">`83c0ac9c`</a>: refactoring of grizzly_extras.messagequeue
- <a href="https://github.com/Biometria-se/grizzly/commit/9b9953e621e318e16c0121a4debc0804f762a89f" target="_blank">`9b9953e6`</a>: included mypy extension in devcontainer
- <a href="https://github.com/Biometria-se/grizzly/commit/aea0c288b22b20b4a0d96ddd45662bca4e7ad6dd" target="_blank">`aea0c288`</a>: implemented RECEIVE for ServiceBusUser
- <a href="https://github.com/Biometria-se/grizzly/commit/b046b60cc6ac268dcadb33d2fac8132e0a16fb32" target="_blank">`b046b60c`</a>: Changed spawn_rate to float and fixed tests
- <a href="https://github.com/Biometria-se/grizzly/commit/3ce0b102e220bc0fd586fad23cebabc9d52d8adf" target="_blank">`3ce0b102`</a>: Changed spawn rate from int to float
- <a href="https://github.com/Biometria-se/grizzly/commit/a789a71df3bde45cc880761aa55400b22e75ce4f" target="_blank">`a789a71d`</a>: Added support for user weight
- <a href="https://github.com/Biometria-se/grizzly/commit/29f7d047f0236446dd135ad721da0eeee57f4592" target="_blank">`29f7d047`</a>: updated atomic variables getting value and arguments
- <a href="https://github.com/Biometria-se/grizzly/commit/81ccbed152267b3883d72bec62ea2eb6ee870de2" target="_blank">`81ccbed1`</a>: change log level for grizzly_extras if started with verbose

## v1.1.0

- <a href="https://github.com/Biometria-se/grizzly/commit/b78be958e54d6cbe04f1ef155f58f3eb3937906a" target="_blank">`b78be958`</a>: new task to parse data
- <a href="https://github.com/Biometria-se/grizzly/commit/b2ed98a1b82bf3411d854e586b6a9fb75dae241c" target="_blank">`b2ed98a1`</a>: XmlTransformer: match parts of a document, and dump it to string
- <a href="https://github.com/Biometria-se/grizzly/commit/9c55e5a3533bf472a2a0b5c67c770263b58fa741" target="_blank">`9c55e5a3`</a>: move testdata production if variable has __on_consumer__ = True
- <a href="https://github.com/Biometria-se/grizzly/commit/cfe9875b1688738da260c6c19741fd5eb9784de7" target="_blank">`cfe9875b`</a>: specify external dependencies for users and variables in the objects themself
- <a href="https://github.com/Biometria-se/grizzly/commit/2140fe7948915bce027417a4dc5dc77fbc48b856" target="_blank">`2140fe79`</a>: new variable AtomicMessageQueue
- <a href="https://github.com/Biometria-se/grizzly/commit/1f6dba03f68187d8b5802ceb9bcce9452721bd5a" target="_blank">`1f6dba03`</a>: refactoring for clearer distinction between utils and step helpers.
- <a href="https://github.com/Biometria-se/grizzly/commit/99fab953d963e103dd69356bcb05bb0f17ea6b58" target="_blank">`99fab953`</a>: fixing empty changelog in workflow@github
- <a href="https://github.com/Biometria-se/grizzly/commit/c5ddeeb9e9b7c1f29eb5bb41a0259ae51e7f0f30" target="_blank">`c5ddeeb9`</a>: generate changelog when building documentation
- <a href="https://github.com/Biometria-se/grizzly/commit/5e595637fadb30757a63c698b027797f9996c31c" target="_blank">`5e595637`</a>: fixed missed float -> SleepTask change in test
- <a href="https://github.com/Biometria-se/grizzly/commit/2c6885544e383860d24945afb4a340e8c0ac2a6c" target="_blank">`2c688554`</a>: improved base for adding different types of tasks
- <a href="https://github.com/Biometria-se/grizzly/commit/cb8db86278182da418857d852d27e8edfc006545" target="_blank">`cb8db862`</a>: reafactor LocustContext* to GrizzlyContext*
- <a href="https://github.com/Biometria-se/grizzly/commit/3f1137b960c37e0f4068359d0c25ca65e9848db2" target="_blank">`3f1137b9`</a>: refactoring of grizzly.tasks
- <a href="https://github.com/Biometria-se/grizzly/commit/bd3382e0c1d35d55d6d3b8f782060fd726dd15f8" target="_blank">`bd3382e0`</a>: refactoring RequestContext to RequestTask
- <a href="https://github.com/Biometria-se/grizzly/commit/031a959517b453849a97bc95a305e0b583b60e73" target="_blank">`031a9595`</a>: handle edge cases with Getitem nodes
- <a href="https://github.com/Biometria-se/grizzly/commit/ca7c17d78a3a316a2df83618fe0156433e773fa9" target="_blank">`ca7c17d7`</a>: handle Getitem nodes when parsing templates for variables
- <a href="https://github.com/Biometria-se/grizzly/commit/a1cabb3116d443ecb4d1c7583f6cda18a2da6fe5" target="_blank">`a1cabb31`</a>: only try to remove secrets from dicts
- <a href="https://github.com/Biometria-se/grizzly/commit/1145a65133fa09e8adbc240f12648198600fee26" target="_blank">`1145a651`</a>: possibility to store json objects/list in variables

## v1.0.1

- <a href="https://github.com/Biometria-se/grizzly/commit/dae7be58162b76b2c9d362c1565f889555606df3" target="_blank">`dae7be58`</a>: Corrected string comparison operator
- <a href="https://github.com/Biometria-se/grizzly/commit/4b8a8470eee0122399c2aa89c26b627f7c372282" target="_blank">`4b8a8470`</a>: Adjusted test for messagequeue
- <a href="https://github.com/Biometria-se/grizzly/commit/bd9bb9775f66c0df1ace69d4de6ace8194cce9f5" target="_blank">`bd9bb977`</a>: Fix for being able to log MQ request payload
- <a href="https://github.com/Biometria-se/grizzly/commit/cc5dfff65a1a4233af300ad86dc2d706b4224e92" target="_blank">`cc5dfff6`</a>: updated mkdocs to 1.2.3 due to CVE-2021-40978
- <a href="https://github.com/Biometria-se/grizzly/commit/604f5704fe043c7b1cc5c50b9df32bada8d0a0ba" target="_blank">`604f5704`</a>: fixed url
{% endraw %}
