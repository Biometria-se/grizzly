---
title: Editor support
---
# Editor support

It can be hard to remember all the step expressions by heart; that's why `grizzly-ls` exists! Which is a server implementation of LSP[^1], providing auto-complete of step expressions.

The server does not do much by itself, you have to install a client/extension that speaks LSP for your editor.

[^1]:
    The Language Server Protocol (LSP) defines the protocol used between an editor or IDE and a language server that provides language features like auto complete,
    go to definition, find all references etc. The goal of the Language Server Index Format (LSIF, pronounced like "else if") is to support rich code navigation in
    development tools or a Web UI without needing a local copy of the source code. [Read more](https://microsoft.github.io/language-server-protocol/)
