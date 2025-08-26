---
title: Changelog
---
# Changelog

{{ changelog('editor-support', 'ls') }}

<!-- static, generated from old repo -->
{% raw %}
## v2.1.20 **legacy**{.chip-feature .info}

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/843f59625461f1eda88107c3ee7f34baff576370" target="_blank">`843f5962`</a>: fix conflicinting jinja2 dependency version with grizzly (#73)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/2f3d6fa1c47745ee54b5bea84f456ee753022de9" target="_blank">`2f3d6fa1`</a>: do not fail workspace diagnostics if all open files has been closed (#72)

## v2.1.19

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/e25a6016d94d6e781eae0ac47d568556276688db" target="_blank">`e25a6016`</a>: better logging when failing to load steps from a path (#71)

## v2.1.18

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/08831eb5b0f7a1574415464f18a6d3015003125e" target="_blank">`08831eb5`</a>: render feature file in cli (#70)

## v2.1.17

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/02408e78f5903fff04c36b3a99e316a6fb7a5ac3" target="_blank">`02408e78`</a>: use new interface (if available) to get what values to permutate step expressions (#69)

## v2.1.16

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/c9357adc9b28b617c82983812c03da0c78146c66" target="_blank">`c9357adc`</a>: fix build (#68)

## v2.1.15

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/1e053bf53da74d9c9f3877b9dac024055fba0f3f" target="_blank">`1e053bf5`</a>: update packages to fix a couple of CVEs (#67)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/af74e59234f1d9010c6a6a17c804afcdb196d455" target="_blank">`af74e592`</a>: fixed rendering bugs (#66)

## v2.1.14

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/ee0f1735282b29733c625940ba3f769c95ce1d51" target="_blank">`ee0f1735`</a>: conditional steps render (#65)

## v2.1.13

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/b583958471d1ccb04e04caa7c2bc423460bea497" target="_blank">`b5839584`</a>: better test coverage for new features (#64)

## v2.1.12

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/704210e1f58a6418b65a12901e9bfe610c5b6d84" target="_blank">`704210e1`</a>: fixed sanatizing bug related to empty lines (#63)

## v2.1.11

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/2e869b0ac7d3b0a9a0c621c47cd51d7715aaa18b" target="_blank">`2e869b0a`</a>: error handling and fixes (#62)

## v2.1.10

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/d78fb5289f665ce2e2a1eb33cf003d500752322f" target="_blank">`d78fb528`</a>: support grizzly-cli#94 (#61)

## v2.1.9

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/0201da95b6266d0ebbc27b81f0e6cbef9216f67f" target="_blank">`0201da95`</a>: Removed duplicate logging
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/1eff9775f4a9cb57608be469181269a7d0ad88dd" target="_blank">`1eff9775`</a>: Update after review + server startup error msg
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/576c01e25385082016436f2bec6096ece6fed43c" target="_blank">`576c01e2`</a>: debugpy typing stub
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/c9d813fc55eeda93d1bb1c593ba50056e7c66cd4" target="_blank">`c9d813fc`</a>: Improved support for relative paths + debugging

## v2.1.8

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/1424bf70aeb762dfed4c8e4dd0308afb0558ab43" target="_blank">`1424bf70`</a>: fix relative feature-file paths in scenario tag (#59)

## v2.1.7

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/a37beb0df2e6303f21f888533657d971dce1ea44" target="_blank">`a37beb0d`</a>: fixed bug related to variable completion (#58)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/e59e8152fe19230ac841e1831ce950ab4899c78f" target="_blank">`e59e8152`</a>: typo for black line-length
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/89ec6cb4bf82ddf062ae3947c165453ccc825681" target="_blank">`89ec6cb4`</a>: some projects wraps behave step decorators
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/bffe03f1958499ae6d37ab6e993a97a170830416" target="_blank">`bffe03f1`</a>: debug step definition for implementation

## v2.1.6

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/f2cbabcdd7d279d2152ca0e742e41da170c0a4c1" target="_blank">`f2cbabcd`</a>: handle the slow mac os github runners
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/62ee03a292837ea8aeb26a27bcb8c63062f0449b" target="_blank">`62ee03a2`</a>: make sure file locations are absolute paths

## v2.1.5

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/2d7abe690f804cef625cca3e8c68e92434ae4e4d" target="_blank">`2d7abe69`</a>: Typing
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/6eb3a6f138d3963167b35c75987470422bd03db1" target="_blank">`6eb3a6f1`</a>: Updated after review
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/2cc6f34a8c7776ee13695ceed297408aeced2a6e" target="_blank">`2cc6f34a`</a>: Updated after review and put path matching in separate method
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/c273dd6e81c09509aba869af0d25f0928270ae7b" target="_blank">`c273dd6e`</a>: Added settings for ignoring source files

## v2.1.4

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/0a7d8f4dcf258194f929adcd68ea2adf27475369" target="_blank">`0a7d8f4d`</a>: support for grizzly `{% scenario ... %}` tag (#54)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/d6ec56a688cfac539373bae536d0c221ba064aea" target="_blank">`d6ec56a6`</a>: validate gherkin/feature-files via cli (#53)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/bf6bc72ef165f1088e187cb23a3ef7f5bfea9db8" target="_blank">`bf6bc72e`</a>: clear diagnostics for a file when it is closed (#52)

## v2.1.3

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/3ba0c764f4c6a18c57c546b467049ad127d1103a" target="_blank">`3ba0c764`</a>: completion improvements (#51)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/30ca84e6a7a9f3ecbc2db1a066b138527d509ce4" target="_blank">`30ca84e6`</a>: better information load step loading fails (#50)

## v2.1.2

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/0db16c50ba66932c1ac772358abf0a7da8722da8" target="_blank">`0db16c50`</a>: bug fixes related to comments, freetext and tables (#49)

## v2.1.1

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/391485ca1ede4b3984cfad0e9059848b2a295c43" target="_blank">`391485ca`</a>: fixed diagnostics bugs (#48)

## v2.1.0

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/f0c3a6bc18fdeb17e96f6db2a32aa7c45a76a655" target="_blank">`f0c3a6bc`</a>: major refactoring (#47)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/80853b1410113e529df097382133586560d287f7" target="_blank">`80853b14`</a>: implement `textDocument/diagnostic` (#46)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/adab2fce7655fa79c82e436db91c912a8deef28d" target="_blank">`adab2fce`</a>: "go to definition" brings up the step implementation (#45)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/bdb7dadc6a7ff5f9ff272b7689dc2e685833b192" target="_blank">`bdb7dadc`</a>: use `text_edit` instead of `insert_text` in `CompletionItem` (#44)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/5841a74d4f710281c1ba934c6bf3b6d18f7028bd" target="_blank">`5841a74d`</a>: complete partial variable names (#43)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/b84296d14ea09d39dd38553a86e4d47fb5bf109e" target="_blank">`b84296d1`</a>: normalize and extra validate `grizzly.variable_pattern` client settings (#42)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/e5217820470ec7d135ce341ab7029605285a8db0" target="_blank">`e5217820`</a>: better logic on which directories to check for step implementations (#41)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/05f3236671e0c0af0e0c79749084940d3c918a21" target="_blank">`05f32366`</a>: fix broken `--version` (#40)

## v2.0.0

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/229272e7e448461d5844ad0770b338c98c8de517" target="_blank">`229272e7`</a>: fixed workflow syntax error (#39)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/65aeefd040025256ed3132d663f39b500ec3a478" target="_blank">`65aeefd0`</a>: add activationEvents again (#38)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/d45635e31dc20a9c068d8e77576c716af0afba98" target="_blank">`d45635e3`</a>: general behave support (#37)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/caf2b07bb1e7fbc3a8d527fd4f96fe5510493611" target="_blank">`caf2b07b`</a>: complete variable names (#36)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/9c28a8e3de449a30fb1e2f6014cdf93084ecb457" target="_blank">`9c28a8e3`</a>: removed git dependency (#34)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/fd15b6de6b63daa954f39937a9aa026cbef7706a" target="_blank">`fd15b6de`</a>: add python 3.11 support (#33)

## v1.0.1

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/476297045b0699d708d4d75e8afac781b4fc3b93" target="_blank">`47629704`</a>: Bump json5 from 1.0.1 to 1.0.2 in /client/vscode (#29)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/d0293d8a732246235f086a4dab676d9ab58b568b" target="_blank">`d0293d8a`</a>: pip-licenses 4.2.0 is broken, exclude (#32)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/2401807486f53eb654c16594b2bfe1626628ae39" target="_blank">`24018074`</a>: fixes Biometria-se/grizzly#220 (#31)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/85e7a37b2545b196845028c9c558e34b924ca697" target="_blank">`85e7a37b`</a>: refreshed devcontainer, so it builds (#30)

## v1.0.0

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/e174a6d70eb410c6777415a5e40796205f05ed3e" target="_blank">`e174a6d7`</a>: upgrade to pygls 1.0.0 (#28)

## v0.0.9

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/08f3a4f6c7777812a0485246b857cfb70fdb577d" target="_blank">`08f3a4f6`</a>: updated action versions to get rid of warnings (#27)

## v0.0.8

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/1ca7171d6db2d5bc0822aec16e332a3832bbfcc6" target="_blank">`1ca7171d`</a>: return snippet strings for step expressions containing variables (#26)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/29c0308282766471d48ba0d6495ee3e4b96e8ade" target="_blank">`29c03082`</a>: step completion improvements (#25)

## v0.0.7

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/b082f8af6b0ca78fa2e1c09707d76c34244140fa" target="_blank">`b082f8af`</a>: fix hover help for alias step keywords (#24)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/abb3c607b5effabd2a6a554beb3cfd3998e7a122" target="_blank">`abb3c607`</a>: provide `name` and `version` in `LanguageServer` constructor (#23)

## v0.0.6

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/a4f553ed40af6ff6a8be6169710c9649a22562e9" target="_blank">`a4f553ed`</a>: only try to find help text if keyword is valid (#22)

## v0.0.5

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/0b734bc1f7de00fb33eb6793ed0f99d603acee35" target="_blank">`0b734bc1`</a>: map step implementation docs to correct normalized step expression (#21)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/71e5773c4a51159e1677c06c6ec84d552b693b16" target="_blank">`71e5773c`</a>: add link to vscode marketplace in extension readme (#20)

## v0.0.4

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/b658074ba29c32fdaa7ec5a57490197160068bd2" target="_blank">`b658074b`</a>: help on hover (#19)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/82fbd485fddd8a61dd1a5acd138faffb2ba958a7" target="_blank">`82fbd485`</a>: packages metadata (#18)

## v0.0.3

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/375eb6054751da1b5e1e0f4adfaa3ff3653184ab" target="_blank">`375eb605`</a>: client vscode icon (#17)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/5d3e3da807c426d00a6869f5ba2b06e576e8bcb8" target="_blank">`5d3e3da8`</a>: documentation and bugs (#16)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/1c913841a0bfa82d4e178186f37bf70184c352fd" target="_blank">`1c913841`</a>: added missing project description (#15)

## v0.0.2

- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/38ada9123653afee03ba62a96c533b1563bd49c2" target="_blank">`38ada912`</a>: metadata update (#14)
- <a href="https://github.com/Biometria-se/grizzly-lsp/commit/1de67efc4798a240d90fe5bba2cfee33bb0bc6ce" target="_blank">`1de67efc`</a>: action-push-tag@v1 is broken (#13)
{% endraw %}
