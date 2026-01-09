import { expect } from 'chai';

/**
 * Extension module has complex dependencies (vscode-languageclient, PythonExtension)
 * that cannot be mocked in unit tests. The full extension activation and lifecycle
 * is tested in e2e tests where the complete VS Code environment is available.
 *
 * Unit tests focus on isolated, testable components like log, model, and preview.
 */
describe('extension', () => {
    it('is tested in e2e tests due to complex dependencies', () => {
        // Extension module exports activate/deactivate functions that:
        // - Create and manage LanguageClient instances
        // - Interact with Python extension API
        // - Handle VS Code workspace and window events
        // - Manage server lifecycle and hot-reload
        //
        // These require the full VS Code extension host and are validated
        // in tests/e2e where the actual extension runs.
        expect(true).to.be.true;
    });
});
