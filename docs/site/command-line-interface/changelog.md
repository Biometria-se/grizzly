---
title: Changelog
---
# Changelog

{{ changelog('command-line-interface', 'cli') }}

<!-- static, generated from old repo -->
{% raw %}
## v3.2.27 **legacy**{.chip-feature .info}

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/b5c5eae912f507e48d639cfe04ba09d059c5a6d8" target="_blank">`b5c5eae9`</a>: pinned packaging version incompatible with grizzly@main (#114)

## v3.2.26

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/b97ce3a22419f025c88c089836eee6fe2290bbba" target="_blank">`b97ce3a2`</a>: pin pyyaml version that is compatible with grizzly (#113)

## v3.2.25

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c466cddac86beee3f864bec8edcb91f2fe5b597e" target="_blank">`c466cdda`</a>: if `--validate-config` is supplied, always return even when it was a success (#112)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4e247a2fb66ade75b432237d98d3d35fb30a2f33" target="_blank">`4e247a2f`</a>: update jinja2 and requests, to fix CVE's

## v3.2.24

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/cba03fda0ad9dfcda3ef6397d852b3def0412d84" target="_blank">`cba03fda`</a>: replaced pylint+flake8 with ruff (#110)

## v3.2.23

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/6869b3f18c8181c00fcbdd45e4039cf2b6dad24e" target="_blank">`6869b3f1`</a>: keyvault improvements (#109)

## v3.2.22

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/da3aee8c35c538ead952a66fa3e77734085505b0" target="_blank">`da3aee8c`</a>: reference keyvault certificates in environment configuration (#108)

## v3.2.21

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/fdd3fdb12c830e72ce450759f1f8f540fedc3599" target="_blank">`fdd3fdb1`</a>: get_context_root should look for the "closest" environment.py

## v3.2.20

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/213f71e6ab52a814e13785224e1a6cf4e5ed756c" target="_blank">`213f71e6`</a>: temporary file permissions (#106)

## v3.2.19

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/3e8aab14a715066e52157f2d2e0aa5dc2d9f89b8" target="_blank">`3e8aab14`</a>: keyvault configuration bugs (#105)

## v3.2.18

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/fe71240006d73a25d119a8dfcabcf47c2cb0edb5" target="_blank">`fe712400`</a>: new subcommand for keyvault

## v3.2.17

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/90bb369960acf13fa8b8a5dbde788e86c4da61f8" target="_blank">`90bb3699`</a>: remove support for python 3.8
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/9aebbf2b67d14a8857f8b6e0257923f3c06a12a5" target="_blank">`9aebbf2b`</a>: store environment configuration in an azure keyvault, as secrets

## v3.2.16

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/bace69069faeeee56f31e099e1ff9d73f883a090" target="_blank">`bace6906`</a>: strange behavior between github runner py 3.12.9 and devcontainer py 3.12.6
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4d7dd700af24efda903fac45195ef997c1cf0ab5" target="_blank">`4d7dd700`</a>: show which environment file is used, if specified

## v3.2.15

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ebbe24e8c189f47ad796783fda95698628fbcf68" target="_blank">`ebbe24e8`</a>: problems with mq redist 9.4.1.0 which is linked by the short-url.

## v3.2.14

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/47811737b9f2d7f393fa5acc94b5a79e5efe3ee1" target="_blank">`47811737`</a>: strange locust error when trying to write stats to CSV file, skip it in E2E test for now.
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/3559c352268bbd2a02842931b496f84c1de5ce40" target="_blank">`3559c352`</a>: fix unit tests
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/406346a38319c85c2e2218a1cc2659da94b68fde" target="_blank">`406346a3`</a>: values used to calculate user distribution can contain variables

## v3.2.13

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/b0ee7c14b34ba071ddc1d7be44ff9d1895df7dba" target="_blank">`b0ee7c14`</a>: update jinja2 to patch cve-2024-22195
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/8f36acafa71f1b1745c106f8f09a4827c995a24c" target="_blank">`8f36acaf`</a>: fix for failing unit tests py < 3.10
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/199402f441543475566a0a210b2de57a77ded36b" target="_blank">`199402f4`</a>: fixed bug Biometria-se/grizzly#347

## v3.2.12

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ac2ed000f306fa6b99172fc1e2ac7c1d4819114c" target="_blank">`ac2ed000`</a>: new arguments to get more information from building

## v3.2.11

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/3ebd7d66b1141cec59c2e03a19574c6d37c33698" target="_blank">`3ebd7d66`</a>: inherit parent feature-file correctly
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/34f858c1a3743943816e22fc00ff5d3fc897139f" target="_blank">`34f858c1`</a>: remove if-statements containing grizzly variables in "root" feature-file.
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0200a5c36099de5a725a6b0086ee58360a0f4ea5" target="_blank">`0200a5c3`</a>: extract scenario scenario from feature file before rendering
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/3e4e131641632b24c0ba1278740b1a290f7f1557" target="_blank">`3e4e1316`</a>: fixed bug where the conents/body of a if-statement block would be rendered, even though it shouldn't at this point

## v3.2.10

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ed93a78f312d3d1911ae07105261c5c0542297e2" target="_blank">`ed93a78f`</a>: support for `if`-statements in templates

## v3.2.9

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/2503b7919958993b966a3937b06096cf618833b0" target="_blank">`2503b791`</a>: possible fix for conflict between setup-python action and homebrew formulas.
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/32706775b5b7de1223fe07af02335dfdae20b9e9" target="_blank">`32706775`</a>: handle relative paths in nested scenario-tags correctly.

## v3.2.8

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/99fac3a2703b57055c537fb8587ea10f4fbbf356" target="_blank">`99fac3a2`</a>: allow nested `{% scenario .. %}` tags and special tag variables (`{$ .. $}`) (#94)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/733e7886caad72139bcdff126f9ac06e7ba06262" target="_blank">`733e7886`</a>: bump devcontainer and dist image to python 3.12 (#93)

## v3.2.7

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/26555784636385faeeef377b64c4efbe8e8ae1dc" target="_blank">`26555784`</a>: remove top-level version element in compose.yaml (#92)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e2949eb0af51af0ee5cf03a0e59ac0e65386169f" target="_blank">`e2949eb0`</a>: implementation of `--dry-run` in `run` command (#91)

## v3.2.6

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/2496d9ee41bd5e9238978793ba253c2803f730ed" target="_blank">`2496d9ee`</a>: support for relative upstream paths for feature files in scenario tag (#90)

## v3.2.5

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/2fa1dd39e9bcdeb33e310dfbab5c0cf79652aba2" target="_blank">`2fa1dd39`</a>: create temporary .lock.feature file

## v3.2.4

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/dcf742aea11d281a34ab23199502628cc19adf4c" target="_blank">`dcf742ae`</a>: remove `--stop` argument for behave
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/8163280dc421288da4cc9e527129724fdec61851" target="_blank">`8163280d`</a>: new user step that sets user type, host, fixed count
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/6157e7a677dba8fa697676561e71ef7a44f3997c" target="_blank">`6157e7a6`</a>: handle step expressions that assign users to a scenario with a tag
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/89959fd81fb70e6550f5885d4983b10a9f5ce503" target="_blank">`89959fd8`</a>: fixed bug for missing step text and table
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/169e1afa11e1093e69d8bfb6a9e96fd80a3c6808" target="_blank">`169e1afa`</a>: bug with included steps fixed
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/1629fab1fee5a309d0f0ddb17d806adb51bf8207" target="_blank">`1629fab1`</a>: adaptations to grizzly version that supports the new locust dispatcher
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/151de2a0eca044108cc15113354338138447b1ae" target="_blank">`151de2a0`</a>: Scenario tag fix, copying variable substrings to preserve whitespacing (#87)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/9ebf592a5d943d128611274347894d7d32ea6b07" target="_blank">`9ebf592a`</a>: remove norwegian debugging statements
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/87db970ab86d168932d3057f1ed13a104f4ea603" target="_blank">`87db970a`</a>: remove level classvar
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/71833170ace84afdd434993b546d95d5af79b99c" target="_blank">`71833170`</a>: no need for temporary files
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/af9f753ab443995e4d8435b6f0f92d48b4f6f506" target="_blank">`af9f753a`</a>: windows cannot open a file twice
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0c62cea1a3ee7d03fd8370835e8e41f20ab6b8ea" target="_blank">`0c62cea1`</a>: anything outside of `{% scenario ...%}` should be treated as data, e.g. plain text.
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/d46da8987836d508dc58a5f5457a8b2c848da634" target="_blank">`d46da898`</a>: custom jinja2 tag to include all steps from a scenario in another feature file
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/17fecd62a9b0d4ee0d008ec3d9270fba2408edf1" target="_blank">`17fecd62`</a>: fix for auto-completing "partial" directories and exact file matches
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/f42ef1663ad9d5eba3d719d16e1ef81679cb7526" target="_blank">`f42ef166`</a>: error detection in distribution_of_users_per_scenario

## v3.2.3

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/a5530234f189ca6ab3c1ed51c97f5a38f8718396" target="_blank">`a5530234`</a>: included missed output in log file when `-l/--log-file` is used

## v3.2.2

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0a1eb8998cfe6ecc45839b1e6d906fa61ce9b274" target="_blank">`0a1eb899`</a>: change flask version
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/b21ef107c5d5c025c8b3e78505b09826b70d95bc" target="_blank">`b21ef107`</a>: updated URL to IBM MQ dependencies

## v3.2.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/42403257b227e6bfddf14753bbb47add6ed8a6a3" target="_blank">`42403257`</a>: fixed E2E tests
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/b01702d9208aab5666a19a5285be52f30fdc9196" target="_blank">`b01702d9`</a>: stateless authenticator "app"
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/6c0a0b61828673447181fa9d5d2c99f2cc7bc5d7" target="_blank">`6c0a0b61`</a>: add support for sub log directories in `requests/logs`

## v3.2.0

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/01221bb2256aa90baa7a334a63f1b78b79986d6f" target="_blank">`01221bb2`</a>: add python 3.11 support (#78)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ef90eb6d29a9e4355ac48f34044d024b61f4bccb" target="_blank">`ef90eb6d`</a>: special case for mac os also in log file
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/983cb64e3f4277f93a19fcab7c29aec67c6798f0" target="_blank">`983cb64e`</a>: add run argument to save all output to log file
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c37efac1a4dff6d2ca8e3da3d4aab575a22a883d" target="_blank">`c37efac1`</a>: fixed node numbering for workers
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ac6177174f7a92d88905928641ee1e748dbc0c9e" target="_blank">`ac617717`</a>: install wheel package in venv
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/12b7747d1585150d0e5eee28218f79f71ed65f21" target="_blank">`12b7747d`</a>: prefix missed output with master node name, as it would've been done in the `docker compose up` output
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e0379bacaaa220c8b1470885cfc20fc0b90f9165" target="_blank">`e0379bac`</a>: do not look for grizzly.returncode= in output
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c0aa586c8126d8992ea94ae3c1f2cbaa12221e60" target="_blank">`c0aa586c`</a>: wrap results from `run_command` in a dataclass
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/8aa3ea3aff86f63c4aa1f1312cfca2bb74911b13" target="_blank">`8aa3ea3a`</a>: find output that docker compose hide when aborting a dist run
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/562ab958d91871ae48cdc855a975b6ee7580f342" target="_blank">`562ab958`</a>: cleaner implementation of handling sig traps
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4f21bf37c575187b443a556d098a217a86f0f673" target="_blank">`4f21bf37`</a>: add signal handler that terminates process

## v3.1.8

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/b43f5c6bea2eb891f3bc4898219c8f9bdeba2182" target="_blank">`b43f5c6b`</a>: added missing e2e test for Biometria-se/grizzly#232 (#75)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/48064a43d4886026f4817b26d30d79d1a5cc9436" target="_blank">`48064a43`</a>: fixes Biometria-se/grizzly#232 (#74)

## v3.1.7

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0ed37c06aa8d6b1eed02d4d7c89650df9769734e" target="_blank">`0ed37c06`</a>: do not create a default requirements.txt if it doesn't exist
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/633fb212ae8fdd031ad8c1f9071dc6c8bbf3e740" target="_blank">`633fb212`</a>: use compose v2 via `docker compose` instead of `docker-compose` (#72)

## v3.1.6

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/f193a1e3ac7969d81c65e841ed78ebfb4271cce4" target="_blank">`f193a1e3`</a>: pip-licenses 4.2.0 is broken, exclude (#71)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/89abf922dadfdb0feb848583d79901e2fb5f91e4" target="_blank">`89abf922`</a>: change constraints for tomli dependency to avoid dependency problems with other dependencies
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/6ab6be0207572decec3a1232d1628a8f06e0c297" target="_blank">`6ab6be02`</a>: removed duplicated packaging dependency
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ad49973b05218f05229cac9c79b894881db1385f" target="_blank">`ad49973b`</a>: colima docker install needs buildx plugin
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c3a65880c81b6597dc0d128ab900b38224dda2de" target="_blank">`c3a65880`</a>: fixed mypy errors, due to updated mypy version
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/78e0b36b9940381a1829f7e5a9b11fda340e876c" target="_blank">`78e0b36b`</a>: adaptations for Biometria-se/grizzly#205.

## v3.1.5

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/08d9fb08de8ca8cd2307bf9fe4cc60fc302a2bc3" target="_blank">`08d9fb08`</a>: fixes Biometria-se/grizzly#186
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/7cbcc531685125d8ecb8c50f9aeb9e0c1fa81b44" target="_blank">`7cbcc531`</a>: Update README.md
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/81484ea7e727d83122193297d175bdb21210b9fb" target="_blank">`81484ea7`</a>: updated action versions to get rid of warnings (#66)

## v3.1.4

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0cfd9e9f408d5511030c114848ab9c3333e3b3b0" target="_blank">`0cfd9e9f`</a>: implemented support for `--csv*` arguments (#65)

## v3.1.3

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e211442c52ff77b464366e9384659c91b3a2454d" target="_blank">`e211442c`</a>: grizzly support templating for user weight

## v3.1.2

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/8c7b891533db4edeb16e1646569460108d8b85e9" target="_blank">`8c7b8915`</a>: annotate notices in feature metadata (comments) that grizzly-cli will show

## v3.1.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4df22703c1d5f36191d1a7748f0b96f986179e12" target="_blank">`4df22703`</a>: check for return code in command output
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/da9c0f0d8935bc093c8c7b89fd58b298ee80b8cf" target="_blank">`da9c0f0d`</a>: add `wheel` as dev dependency

## v3.1.0

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/f0a6b13b620bf10e8cbd2cd615ec572158ef4057" target="_blank">`f0a6b13b`</a>: fixed copy-paste error in publish package step
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/40b83dc578bc001b73bdb1c10c5e1d5b3c9bb0dc" target="_blank">`40b83dc5`</a>: fixed indentation for inputs in release workflow (#61)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/34235b596800758c1e2c38f2df14ae4bc551ebce" target="_blank">`34235b59`</a>: fixed warnings from coverage when running e2e tests (#60)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/771eb3e1470853de13132a6627e223d9feb177f1" target="_blank">`771eb3e1`</a>: implementation of issue #159
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/a9f6982c741cc32f01ab1abdda38a35b7855f7c1" target="_blank">`a9f6982c`</a>: ::set-output deprecated (#58)

## v3.0.11

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/159b313ad5bf51caea834e658e0e4d17ce4d6559" target="_blank">`159b313a`</a>: v3.0.10 comes after v3.0.1, so latest is v3.0.9 (lexicographic order) -- use `sort -r --version-sort` (#57)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/5404d7aea0782b5933ddb81be6365e475ec328ba" target="_blank">`5404d7ae`</a>: assume a remote branch if git cat-file fails

## v3.0.10

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/fb1f2608c9c8be877168195c8c89054458e06a30" target="_blank">`fb1f2608`</a>: set LANG and LC_ALL to C.UTF-8 instead of C
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/bdd0f7a3dd155f9d806164f9214b55e1bcc01091" target="_blank">`bdd0f7a3`</a>: improved grizzly version crawling
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/238a989cf72d5a42cdcc4148467f9bf3a2b2b659" target="_blank">`238a989c`</a>: oooh man... encoding on the windows-latest runner :S
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/924486e6e0f5999b91fe931533d35dd44c21a274" target="_blank">`924486e6`</a>: make End2EndFixture windows compatible
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/35b3b18bb05062688ab915ea4af53cebcbc2f883" target="_blank">`35b3b18b`</a>: remove user and group in grizzly image, if they exists
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/f2f20240d271c5afe8e03b2734564fa50344b90e" target="_blank">`f2f20240`</a>: update versions of github actions checkout and setup-python
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/bf4050e2cc16d5957b07eb93208355850ae050e3" target="_blank">`bf4050e2`</a>: e2e test of grizzly-cli init
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e96ee3496dbcce0789f20dc0bed76d6e8f76b8e5" target="_blank">`e96ee349`</a>: e2e test for grizzly init
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/9af2236c791a0454156648883cc075787609fd84" target="_blank">`9af2236c`</a>: restructure of tests and added e2e tests
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0032002cbc44cc7c99a8bb23c1d307f301674f31" target="_blank">`0032002c`</a>: fixed unit tests for windows
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/086df974b63e5679ac2a0c5a819176bcd62da549" target="_blank">`086df974`</a>: optimization of listing files for bash completion

## v3.0.9

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/d91ba37ed2c434cc7f41b701faf7b696a4833c23" target="_blank">`d91ba37e`</a>: workdir changed, correct entrypoint script path (#51)

## v3.0.8

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/61f8699ea9504da6310c6ec7e9882a0b80c7eb88" target="_blank">`61f8699e`</a>: simplified format_text for md-help (#50)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/13c3e80994cb74288afadd72950bd7a2bc896233" target="_blank">`13c3e809`</a>: set workdir to /srv/grizzly

## v3.0.7

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/84590d5cec7fd579c819ebaeee9e7792fd65b86a" target="_blank">`84590d5c`</a>: implementation of grizzly-cli dist clean (#48)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/05c628fbcdf69048792291124e63dad143b57c27" target="_blank">`05c628fb`</a>: changes for being able to use in E2E tests (#47)

## v3.0.6

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/215522a1982a60a3bfa12c4abf0177f104c09d1b" target="_blank">`215522a1`</a>: possibility to inject grizzly-cli command arguments via metadata specified in feature file
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/140f748413fa9f44c1d881d5a0c1e69eb64d906d" target="_blank">`140f7484`</a>: argument to increase how long the master will wait for worker report

## v3.0.5

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/7c3bb2f69d52e5ab8e482016f9bd62d908158d82" target="_blank">`7c3bb2f6`</a>: change version of IBM MQ Redist, and allow it to be overriden

## v3.0.4

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/75690e565cc42e014de53feb12e3250c014b5b02" target="_blank">`75690e56`</a>: pkg-resources is some kind of meta package, ignore in license table (#44)

## v3.0.3

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/6df5c857aa412f7f03e46203dd66769c46463bfc" target="_blank">`6df5c857`</a>: update script that generates licenses information (#43)

## v3.0.2

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/bb270a8202d3c1b2376191a18fb6afb5fb88d9c1" target="_blank">`bb270a82`</a>: changed `grizzly_cli.SCENARIOS` to a list to get guaranteed insertion order (#42)

## v3.0.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/df5a4902aeeb2a063ced089f9877a10f6186f915" target="_blank">`df5a4902`</a>: --yes argument to automagically answer yes on any questions
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0b57d7394f73edc2e34e817a5758c93ec303825c" target="_blank">`0b57d739`</a>: subcommand is not set for command `init`
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/43ccba6bac4b163ea4a1427893b393c333359572" target="_blank">`43ccba6b`</a>: do not sort scenarios by name, iterate in the order they are defined
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/dd4b5e7b2d3573079f7a69b031ae2066cad3ded5" target="_blank">`dd4b5e7b`</a>: adaptations for Biometria-se/grizzly#71

## v3.0.0

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/267ee8df5801abad7943b23fa7516bb8adc3efee" target="_blank">`267ee8df`</a>: restructuring commands
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c1170417042505c44b34cd80f4f2953f63766abf" target="_blank">`c1170417`</a>: step expression for number of users can be singular (#37)

## v2.1.6

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/803b3f3a073ea770e81d51b40d33f6cbda8118a1" target="_blank">`803b3f3a`</a>: users per scenario is calculated incorrectly (#36)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/065b1e5cfcf638b1eeda8890a34448241c32a227" target="_blank">`065b1e5c`</a>: documentation of IBM_MQ_LIB_HOST environment variable (#35)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e73174b2f86845e5aeb4455da41ea4e793790bd6" target="_blank">`e73174b2`</a>: make sure that it is possible to generate licenses documentation (#34)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/a574b5c4c70eb14aaa462f4a535fd1a95001e625" target="_blank">`a574b5c4`</a>: github action action-push-tag@v1 is broken (#33)

## v2.1.5

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e3eb90731fb55f8c99299ac4b2ac75a1df906e77" target="_blank">`e3eb9073`</a>: no smoothed timeline in test summary
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/2d706e0f77b51dbeb83489bf1ba26e171e9dd530" target="_blank">`2d706e0f`</a>: --add-host if overridden IBM_MQ_LIB_HOST is using host.docker.internal (#31)

## v2.1.4

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/220924f6a56ea7e4a948c3e8f6536fc8bc114c03" target="_blank">`220924f6`</a>: fix failing tests
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/45fc771858a3662e6a1257b14c1504ca1468e773" target="_blank">`45fc7718`</a>: possible to override host where IBM MQ lib redist package should be downloaded from
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/d34ebbf8460faf84d60bc2e30be769ab8ac04af6" target="_blank">`d34ebbf8`</a>: script for generating a license summary (#29)

## v2.1.3

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/49116c4245da77744ef6acd4f94101a1ddb29474" target="_blank">`49116c42`</a>: check if container image should be built with or without mq libraries
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/50ec4869759829399d9fa7eeed21c7eacba1575e" target="_blank">`50ec4869`</a>: check if grizzly-loadtester is installed with any extras
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/447f677e9bcff1b77b5f6918a1f5ca9f87bd27e4" target="_blank">`447f677e`</a>: Containerfile updated to have MQ libraries as optional
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/64b2131a1fe3440f6c76f12ba0056b70d6cd02df" target="_blank">`64b2131a`</a>: unable to use cache functionality of setup-python@v2
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/241bdb22c5c782fa27a072cd3d7db3fb948d4eec" target="_blank">`241bdb22`</a>: fix for Biometria-se/grizzly#73
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/340cfd18ef06f83d62c2d69d7a0acb1222538f5f" target="_blank">`340cfd18`</a>: only run code quality workflow on 3.10 on windows
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/e11c1b098f0d72e737048f5becaf933f80c32743" target="_blank">`e11c1b09`</a>: docker-compose v2 and v1 compatible in instructions
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/1db75e0bed7ce0f730ac8497d9930f1106d56b4b" target="_blank">`1db75e0b`</a>: base on 3.10-slim instead of alpine
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/a7dc5ce682d89d33c2857ebee59e2300dc235e54" target="_blank">`a7dc5ce6`</a>: add --no-tty argument to run dist
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/8d3d83ee7e7efaa93f0d52fb8143972b5c527d23" target="_blank">`8d3d83ee`</a>: more dynamic creation of sub command parsers
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/03652ae85876f4cc261a7091d5195c68ee1b4aad" target="_blank">`03652ae8`</a>: build package based on metadata only

## v2.1.2

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/344bc53b3f519c5f57166c1640bfcb908e58382d" target="_blank">`344bc53b`</a>: remove debug prints that fell through quality control

## v2.1.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/d41cde5c51e7a067825d689632621802bec9c132" target="_blank">`d41cde5c`</a>: added missing dependency (#24)

## v2.1.0

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4feb4ec5d72b4f095aca1bf4a6dca8af49601557" target="_blank">`4feb4ec5`</a>: build grizzly container image based on locust container image version that the project depends on
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4e3ec441f7864f8cc6a6b2ba3f0685055050fbe5" target="_blank">`4e3ec441`</a>: implementation of getting dependencies version
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4ffc453500330308ee88302e08cc25a1b2fa7989" target="_blank">`4ffc4535`</a>: WIP: get grizzly and locust version from project
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/64b5eaef2df1fd3a43f10d57a7ab69e08e64deee" target="_blank">`64b5eaef`</a>: Feature/spring 2022 cleanup (#22)

## v2.0.4

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c9c0c24bdc3309fa6ca550449c4904e781ad80ac" target="_blank">`c9c0c24b`</a>: fix tty size in container (#20)

## v2.0.3

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/234e9c05d076ed4e786064b045b3b606a4278886" target="_blank">`234e9c05`</a>: getattr retrives value of registry in args, but if it is None (#19)

## v2.0.2

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/37da0e7374da22af5d4f025381f38188e61f19b7" target="_blank">`37da0e73`</a>: Feature/build parser (#18)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/7c64bd0b2c5e26321860f75188465d9cf6d20874" target="_blank">`7c64bd0b`</a>: added arguments for controlling compose health checks (#17)

## v2.0.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/83b081123d0831e3c863e6ad6f0df3b1f37a7a7e" target="_blank">`83b08112`</a>: build broken for 2.0.0 (#16)

## v2.0.0

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/d624baa76ca7e02c02b0b049de484f800ddb5c10" target="_blank">`d624baa7`</a>: Feature/run windows 57 (#15)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/f20dccfa4996607483d872ecf6d4ef58a545433f" target="_blank">`f20dccfa`</a>: too greedy gitignore rule resulted in missing new workflow (#14)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/724bd70ee38a76e7e811c22b9349819f2189d6b2" target="_blank">`724bd70e`</a>: Feature/argument refactor (#13)
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/97ab0e45d2e20a3853639d16d39c346495d47b5c" target="_blank">`97ab0e45`</a>: refreshed devcontainer (#12)

## v1.1.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ae46c7a6c2c8dc0ec43b656bae40a5380f153381" target="_blank">`ae46c7a6`</a>: do not expose locust webui port (8089) in compose file (#11)

## v1.1.0

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/9be8b64b2d56ff9b2870cc0a208a09d26a5c6b97" target="_blank">`9be8b64b`</a>: Feature/validate iterations prompt (#10)

## v1.0.9

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/2cd470ae498e2130612a30527a6c4b88fe68790c" target="_blank">`2cd470ae`</a>: remove locust user (uid 1000) when building grizzly container image (#9)

## v1.0.8

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/cca24c6bfe0b65da14d4c61284eb6fc181362927" target="_blank">`cca24c6b`</a>: create a user with uid/gid matching user that executes grizzly-cli (#8)

## v1.0.7

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/4bee4c7b1b002da70024db086d0e572b3349f969" target="_blank">`4bee4c7b`</a>: get IBM MQ client logs when running distributed (#7)

## v1.0.6

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/746d72191cec220b1654d4636759f3dc43545a1c" target="_blank">`746d7219`</a>: expose internal environment variables in execution context (#6)

## v1.0.5

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/824f163d5d1f2b66a80998fff91a96b3e482eb52" target="_blank">`824f163d`</a>: lock locust image version to version of locust used by grizzly

## v1.0.4

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/f8fd5c9d439980ab054d9f9cb7be49d3fab9f8ea" target="_blank">`f8fd5c9d`</a>: ask for value expression can have more than one keyword (#5)

## v1.0.3

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/c0eacc1a0fe8ea96dd38e7c5f649f74df3c46ef7" target="_blank">`c0eacc1a`</a>: set MTU in docker-compose network to the same value as the default bridge has
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/964e699349ddd5826bfd7fa60df7cb5a0cea7b4f" target="_blank">`964e6993`</a>: updated pylint configuration

## v1.0.2

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/0d5ec1baa1d136ebbe2d51236e6a349be08a50d1" target="_blank">`0d5ec1ba`</a>: also get tags
- <a href="https://github.com/Biometria-se/grizzly-cli/commit/ffd521597350f9183a688fe4b1fa342d28fa87b8" target="_blank">`ffd52159`</a>: change development version to 0.0.0

## v1.0.1

- <a href="https://github.com/Biometria-se/grizzly-cli/commit/7fc1fa1b97e1a0d107358e6983a7d8c33bd4c6bd" target="_blank">`7fc1fa1b`</a>: set ulimit nofile to min recommended value for locust
{% endraw %}
