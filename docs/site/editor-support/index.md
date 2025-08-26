---
title: Language server
---
It can be hard to remember all the step expressions by heart; that's why `grizzly-ls` exists! Which is a server implementation of LSP, providing auto-complete of step expressions.

The server does not do much by itself, you have to install a client/extension that speaks LSP with your editor.

The Language Server Protocol (LSP) defines the protocol used between an editor or IDE and a language server that provides language 
features like auto complete, go to definition, find all references etc. The goal of the Language Server Index Format
(LSIF, pronounced like "else if") is to support rich code navigation in development tools or a Web UI without needing a local copy of
the source code, [read more](https://microsoft.github.io/language-server-protocol/).

The server has implemented the following features:

- Completion (auto-completion of gherkin keywords, step expressions etc.)
- Hover (show step expression help)
- Code actions (quick fixes for common problems)
- Definition (link to requests files in project, grizzly style, or any variable value with prefixed `file://`)
- Diagnostics (make sure that a feature file has correct syntax and is runnable)
