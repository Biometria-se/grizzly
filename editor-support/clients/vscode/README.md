# grizzly-vscode

The `grizzly-loadtester` Visual Studio Code extension that makes it easier to develop load test scenarios by providing
auto-complete of step expressions!

![Screen capture of diagnostics](https://github.com/Biometria-se/grizzly-lsp/raw/main/assets/images/grizzly-ls-diagnostics.gif)

![Screenshot of keyword auto-complete](https://github.com/Biometria-se/grizzly-lsp/raw/main/assets/images/screenshot-auto-complete-keywords.png)

![Screenshot of step expressions auto-complete](https://github.com/Biometria-se/grizzly-lsp/raw/main/assets/images/screenshot-auto-complete-step-expressions.png)

![Screenshot of hover help text](https://github.com/Biometria-se/grizzly-lsp/raw/main/assets/images/screenshot-hover-help.png)

Download the extension from [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=biometria-se.grizzly-loadtester-vscode).

For the extension to work, you have to install the language server `grizzly-loadtester-ls` which is published on [pypi.org](https://pypi.org/project/grizzly-loadtester-ls/).

Install it with:

```bash
python -m pip install grizzly-loadtester-ls
```

And make sure it's in a directory that is part of your `PATH` environment variable.
