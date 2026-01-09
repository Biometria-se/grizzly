import { expect } from 'chai';
import * as vscode from 'vscode';
import { GherkinPreview, GherkinPreviewOptions } from '../../src/preview';
import { ConsoleLogOutputChannel } from '../../src/log';

describe('GherkinPreview', () => {
    let context: vscode.ExtensionContext;
    let logger: ConsoleLogOutputChannel;
    let preview: GherkinPreview;

    beforeEach(() => {
        context = {
            extensionUri: vscode.Uri.file('/test/extension'),
            subscriptions: []
        } as vscode.ExtensionContext;
        logger = new ConsoleLogOutputChannel('TestPreview', { log: true });
        preview = new GherkinPreview(context, logger);
    });

    afterEach(() => {
        if (logger) {
            logger.channel.dispose();
        }
    });

    describe('constructor', () => {
        it('should initialize with an empty panels map', () => {
            expect(preview.panels).to.be.instanceOf(Map);
            expect(preview.panels.size).to.equal(0);
        });

        it('should set style based on light theme', () => {
            const lightContext = { ...context };
            const savedTheme = vscode.window.activeColorTheme;
            (vscode.window.activeColorTheme as { kind: number }) = { kind: vscode.ColorThemeKind.Light };

            const lightPreview = new GherkinPreview(lightContext as vscode.ExtensionContext, logger);
            expect(lightPreview).to.exist;

            (vscode.window.activeColorTheme as { kind: number }) = savedTheme;
        });

        it('should set style based on dark theme', () => {
            const darkContext = { ...context };
            const savedTheme = vscode.window.activeColorTheme;
            (vscode.window.activeColorTheme as { kind: number }) = { kind: vscode.ColorThemeKind.Dark };

            const darkPreview = new GherkinPreview(darkContext as vscode.ExtensionContext, logger);
            expect(darkPreview).to.exist;

            (vscode.window.activeColorTheme as { kind: number }) = savedTheme;
        });
    });

    describe('close', () => {
        it('should return false when closing a non-existent panel', () => {
            const uri = vscode.Uri.file('/tmp/nonexistent.feature');
            const textDocument = { uri } as vscode.TextDocument;

            const result = preview.close(textDocument);

            expect(result).to.be.false;
        });

        it('should return true and remove panel when closing an existing panel', () => {
            const uri = vscode.Uri.file('/tmp/test.feature');
            const textDocument = { uri } as vscode.TextDocument;

            // Create a mock panel
            const mockPanel = vscode.window.createWebviewPanel(
                'grizzly.gherkin.preview',
                'Test Preview',
                vscode.ViewColumn.Beside
            );

            preview.panels.set(uri, mockPanel);
            expect(preview.panels.size).to.equal(1);

            const result = preview.close(textDocument);

            expect(result).to.be.true;
            expect(preview.panels.size).to.equal(0);
            expect(preview.panels.has(uri)).to.be.false;
        });
    });

    describe('private methods', () => {
        it('should have create method', () => {
            expect(preview['create']).to.be.a('function');
        });

        it('should have generateHtml method', () => {
            expect(preview['generateHtml']).to.be.a('function');
        });

        it('should generate HTML with gherkin language for success', () => {
            const html = preview['generateHtml']('Feature: Test', true);

            expect(html).to.be.a('string');
            expect(html).to.include('<!doctype html>');
            expect(html).to.include('Feature: Test');
            expect(html).to.include('language-gherkin');
            expect(html).to.include('highlight.js');
        });

        it('should generate HTML with python language for failure', () => {
            const html = preview['generateHtml']('def test():\n    pass', false);

            expect(html).to.be.a('string');
            expect(html).to.include('<!doctype html>');
            expect(html).to.include('def test():');
            expect(html).to.include('language-python');
        });

        it('should create a webview panel with correct properties', () => {
            const uri = vscode.Uri.file('/tmp/test.feature');
            const panel = preview['create'](uri);

            expect(panel).to.exist;
            expect(panel.webview).to.exist;
            expect(panel.title).to.include('test.feature');
        });
    });
});

describe('GherkinPreviewOptions', () => {
    it('should allow content property', () => {
        const options: GherkinPreviewOptions = { content: 'Feature: Test' };

        expect(options.content).to.equal('Feature: Test');
        expect(options.document).to.be.undefined;
    });

    it('should allow document property', () => {
        const mockDocument = { uri: vscode.Uri.file('/test.feature') } as vscode.TextDocument;
        const options: GherkinPreviewOptions = { document: mockDocument };

        expect(options.document).to.equal(mockDocument);
        expect(options.content).to.be.undefined;
    });

    it('should allow both properties', () => {
        const mockDocument = { uri: vscode.Uri.file('/test.feature') } as vscode.TextDocument;
        const options: GherkinPreviewOptions = {
            document: mockDocument,
            content: 'Feature: Test'
        };

        expect(options.document).to.equal(mockDocument);
        expect(options.content).to.equal('Feature: Test');
    });
});
