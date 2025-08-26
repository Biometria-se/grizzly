import * as vscode from 'vscode';
import * as path from 'path';
import { Utils } from 'vscode-uri';

import { ConsoleLogOutputChannel } from './log';

/**
 * Options for configuring Gherkin preview rendering.
 *
 * Specifies the source content for the preview, either from an existing
 * document or as raw content string. One of the properties must be provided.
 *
 * @property document - Optional TextDocument to preview. When provided, the document's content is used for rendering.
 * @property content - Optional raw string content to preview. Used when rendering content that doesn't come from a document.
 */
export interface GherkinPreviewOptions {
    document?: vscode.TextDocument;
    content?: string;
}

/**
 * Manages Gherkin feature file previews with syntax highlighting.
 *
 * Provides webview-based preview functionality for Gherkin feature files,
 * rendering them with syntax highlighting in a separate editor panel.
 * Supports both standard Gherkin and templated scenarios using Jinja2 syntax.
 * Automatically adapts syntax highlighting theme based on VS Code's active color theme.
 */
export class GherkinPreview {
    /**
     * Map of active webview panels keyed by document URI.
     * Tracks all currently open preview panels to prevent duplicates and enable proper cleanup.
     */
    public panels: Map<vscode.Uri, vscode.WebviewPanel>;

    /**
     * The highlight.js theme style to use based on the active VS Code color theme.
     * Set to 'github' for light themes and 'github-dark' for dark themes.
     */
    private style: string;

    /**
     * Configuration for the webview panel display positioning.
     * Opens the preview beside the active editor with focus preserved on the original editor.
     */
    private displayColumn = {
        viewColumn: vscode.ViewColumn.Beside,
        preserveFocus: true,
    };

    /**
     * Creates a new Gherkin preview manager.
     *
     * Initializes the preview manager with context and logger, sets up the panels map,
     * and configures the syntax highlighting style based on the active VS Code color theme.
     * Light themes (including high contrast light) use 'github' style, while dark themes
     * (including high contrast dark) use 'github-dark' style.
     *
     * @param context - The VS Code extension context, used for accessing extension resources like icons
     * @param logger - Console output channel for logging preview operations and errors
     */
    constructor(private readonly context: vscode.ExtensionContext, private readonly logger: ConsoleLogOutputChannel) {
        this.panels = new Map();

        const colorThemeKind = vscode.window.activeColorTheme.kind;

        switch (colorThemeKind) {
            case vscode.ColorThemeKind.HighContrastLight:
            case vscode.ColorThemeKind.Light:
                this.style = 'github';
                break;
            case vscode.ColorThemeKind.HighContrast:
            case vscode.ColorThemeKind.Dark:
                this.style = 'github-dark';
                break;
        }
    }

    /**
     * Creates a new webview panel for Gherkin preview.
     *
     * Initializes a webview panel with syntax highlighting capabilities, positioned beside
     * the active editor. The panel is configured with the Grizzly icon, disabled find widget,
     * enabled scripts for highlight.js, and retained context when hidden for better performance.
     *
     * @param uri - The URI of the file being previewed, used to extract the basename for the panel title
     * @returns A configured WebviewPanel instance ready to display Gherkin content
     */
    private create(uri: vscode.Uri) {
        const basename = path.basename(uri.path);

        const panel = vscode.window.createWebviewPanel('grizzly.gherkin.preview', `Preview: ${basename}`, this.displayColumn, {
            enableFindWidget: false,
            enableScripts: true,
            retainContextWhenHidden: true,
            localResourceRoots: [
                Utils.joinPath(this.context.extensionUri, 'images'),
            ]
        });

        panel.iconPath = Utils.joinPath(this.context.extensionUri, 'images', 'icon.png');

        return panel;
    }

    /**
     * Generate HTML content for the webview panel with syntax highlighting.
     *
     * Creates an HTML document that uses highlight.js for syntax highlighting of Gherkin or Python content.
     * The styling adapts to the VS Code theme (github style for light themes, github-dark for dark themes).
     *
     * @param content - The Gherkin or Python content to render
     * @param success - Whether the Gherkin rendering was successful (true: Gherkin syntax, false: Python syntax)
     * @returns HTML string with embedded highlight.js configuration and the content to be displayed
     */
    private generateHtml(content: string, success: boolean): string {
        const language = success ? 'gherkin' : 'python';

        return `<!doctype html>
<html class="no-js" lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">

  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/${this.style}.min.css">

  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/highlight.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/languages/${language}.min.js"></script>
  <script>hljs.highlightAll();</script>

  <style>
  body .hljs {
    background: var(--vscode-editor-background);
  }

  pre > code {
    font-size: var(--vscode-editor-font-size);
    font-family: var(--vscode-editor-font-family);
  }
  </style>

  <title>Gherkin Preview</title>
</head>

<body>
    <pre><code class="language-${language}">${content}</code></pre>
</body>

</html>`;
    }

    /**
     * Update the content of a webview panel by rendering the Gherkin feature file.
     *
     * @param textDocument - The text document to render
     * @param panel - Optional webview panel to update (if not provided, will be retrieved from panels map)
     * @param on_the_fly - Whether to render on-the-fly without validation (default: false)
     */
    public async update(textDocument: vscode.TextDocument, panel?: vscode.WebviewPanel, on_the_fly: boolean = false): Promise<void> {
        if (!panel) {
            panel = this.panels.get(textDocument.uri);
            if (!panel) return;
        }

        const [success, content]: [boolean, string | undefined] = await vscode.commands.executeCommand(
            'grizzly-ls/render-gherkin', {
                content: textDocument.getText(),
                path: textDocument.uri.path,
                on_the_fly,
            }
        );

        if (content) {
            panel.webview.html = this.generateHtml(content, success);
        }

        return;
    }

    /**
     * Close and dispose the webview panel for a given text document.
     *
     * @param textDocument - The text document whose preview panel should be closed
     * @returns true if the panel was found and closed, false otherwise
     */
    public close(textDocument: vscode.TextDocument): boolean {
        const panel = this.panels.get(textDocument.uri);

        if (panel) {
            panel.dispose();
            return this.panels.delete(textDocument.uri);
        }

        return false;
    }

    /**
     * Preview a Gherkin feature file in a webview panel, creating a new panel if needed.
     *
     * This method checks if the document contains scenarios requiring preview (using {% scenario marker),
     * creates or reveals the preview panel, and sets up event handlers for panel lifecycle.
     *
     * @param textDocument - The text document to preview
     * @param only_reveal - If true, only reveal existing panel without creating a new one (default: false)
     */
    public async preview(textDocument: vscode.TextDocument, only_reveal: boolean = false): Promise<void> {
        let panel = this.panels.get(textDocument.uri);

        if (!panel) {
            if (only_reveal) return;

            const content = textDocument.getText();
            if (!content.includes('{% scenario')) {
                const basename = path.basename(textDocument.uri.path);
                await vscode.window.showInformationMessage(`WYSIWYG: ${basename} does not need to be previewed`);
                return;
            }

            panel = this.create(textDocument.uri);

            if (!panel) {
                return;
            }

            this.panels.set(textDocument.uri, panel);

            panel.onDidChangeViewState(() => this.update(textDocument));
            panel.onDidDispose(() => {
                return this.close(textDocument), undefined, this.context.subscriptions;
            });
        } else {
            panel.reveal(this.displayColumn.viewColumn, this.displayColumn.preserveFocus);
        }

        this.update(textDocument, panel);
    }
}
