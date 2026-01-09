import * as vscode from 'vscode';
import * as path from 'path';
import { expect } from 'chai';

export let doc: vscode.TextDocument | undefined = undefined;
export let editor: vscode.TextEditor | undefined = undefined;
export let documentEol: string;
export let platformEol: string;

export const testWorkspace: string = path.resolve(__dirname, '../../../../tests/project');

/**
 * Activates the biometria-se.vscode-grizzly extension
 */
export async function activate(docUri: vscode.Uri, content: string) {
    // The extensionId is `publisher.name` from package.json
    const ext = vscode.extensions.getExtension('biometria-se.grizzly-loadtester-vscode');
    await ext.activate();
    try {
        doc = await vscode.workspace.openTextDocument(docUri);
        editor = await vscode.window.showTextDocument(doc);

        await setTestContent(content);

        // if the extension for some reason won't start, it is nice to look at the output
        // to be able to understand why
        if (ext.exports === undefined) {
            console.error('extension did not start');
            await sleep(1000*60*60*24);
        }

        // wait until language server is done with everything
        while (!ext.exports.isActivated()) {
            await sleep(1000);
        }

        // show text document again after extensions has been activated, the output channel
        // can be come the active editor after the extension has started.
        if (editor.document.uri != vscode.window.activeTextEditor.document.uri) {
            editor = await vscode.window.showTextDocument(doc);
        }

        while (editor.document.uri != vscode.window.activeTextEditor.document.uri) {
            console.error(`${editor.document.uri} not open yet, active editor is ${vscode.window.activeTextEditor.document.uri}`);
            await sleep(1000);
        }
    } catch (e) {
        console.error(e);
    }
}

async function sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export const getDocPath = (p: string) => {
    return path.resolve(testWorkspace, p);
};
export const getDocUri = (p: string) => {
    return vscode.Uri.file(getDocPath(p));
};

async function setTestContent(content: string): Promise<boolean> {
    const all = new vscode.Range(doc.positionAt(0), doc.positionAt(doc.getText().length));
    return editor.edit((eb) => eb.replace(all, content));
}

export async function acceptAndAssertSuggestion(position: vscode.Position, expected: string): Promise<void> {
    // move cursor
    editor.selection = new vscode.Selection(position, position);

    const time: number = 125;

    vscode.commands.executeCommand('editor.action.triggerSuggest');
    await sleep(time);
    vscode.commands.executeCommand('acceptSelectedSuggestion');
    await sleep(time);
    // wrong texteditor active sometimes?!
    const buffer = vscode.window.activeTextEditor.document.getText();
    const actual = buffer.split(/\r?\n/)[position.line];

    expect(actual).to.be.equal(expected);
}
