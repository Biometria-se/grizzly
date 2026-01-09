/**
 * Completion Tests for Grizzly VS Code Extension
 *
 * This test suite validates the auto-completion functionality of the Grizzly language server
 * in VS Code. It covers three main areas of completion:
 *
 * 1. **Keyword Completion**: Tests auto-completion for Gherkin keywords (Feature, Scenario, Given, When, Then, etc.)
 *    - Ensures keywords are suggested at appropriate locations in the document hierarchy
 *    - Validates that used keywords are filtered out appropriately (e.g., Background appears only once)
 *    - Tests fuzzy matching for keyword suggestions
 *
 * 2. **Step Completion**: Tests auto-completion for Grizzly step definitions
 *    - Validates step suggestions based on keyword context (Given, When, Then)
 *    - Tests partial step matching and completion
 *    - Ensures proper snippet placeholders for step parameters
 *    - Tests completion of partially-typed steps with existing quoted values
 *
 * 3. **Variable Completion**: Tests auto-completion for template variables ({{ variable }})
 *    - Validates suggestion of variables defined earlier in the feature file
 *    - Tests partial variable name matching
 *    - Ensures proper handling of variable template syntax
 *
 * Each test uses the testCompletion helper to simulate VS Code's completion provider
 * and validates the returned completion items against expected values.
 */

import * as vscode from 'vscode';
import { expect } from 'chai';
import { activate, getDocUri, acceptAndAssertSuggestion } from './helper';
import { describe, it } from 'mocha';

describe('Should do completion on keywords', () => {
    it('Complete keywords, empty file, only suggest first-level keyword(s)', async () => {
        // empty document, only suggest "Feature"
        const position = new vscode.Position(0, 0);
        const actual = await testCompletion('', position);
        const expected = ['Ability', 'Business Need', 'Feature'];

        expect(actual.items.length).to.be.equal(3);
        expect(actual.items.map((item) => item.label)).to.deep.equal(expected);
        expect(actual.items.map((item) => item.kind)).to.deep.equal(new Array(expected.length).fill(vscode.CompletionItemKind.Keyword));

        await acceptAndAssertSuggestion(position, 'Ability: ');
    });

    it('Complete keywords, suggest second-level keywords', async () => {
        // only "Feature" present in document, suggest the two second-level keywords
        const position = new vscode.Position(1, 4);
        const actual = await testCompletion('Feature:\n\t', position);
        const expected = ['Background', 'Example', 'Scenario', 'Scenario Outline', 'Scenario Template'];

        expect(actual.items.map((value) => value.label)).to.deep.equal(expected);
        expect(actual.items.map((value) => value.kind)).to.deep.equal(new Array(expected.length).fill(vscode.CompletionItemKind.Keyword));
        await acceptAndAssertSuggestion(position, '\tBackground: ');
    });

    it('Complete keywords, only expect second-level keywords that has not been used', async () => {
        // "Background" present in document, which only occurs once, suggest only "Scenario"
        const content = `Feature:
    Background:
    `;

        const position = new vscode.Position(2, 4);
        const actual = await testCompletion(content, position);
        const expected = ['Example', 'Scenario', 'Scenario Outline', 'Scenario Template'];

        expect(actual.items.map((value) => value.label)).to.deep.equal(expected);
        expect(actual.items.map((value) => value.kind)).to.deep.equal(new Array(expected.length).fill(vscode.CompletionItemKind.Keyword));
        await acceptAndAssertSuggestion(position, '    Example: ');
    });

    it('Complete keywords, all other keywords', async () => {
        // "Background" and "Scenario" (at least once) present, suggest all the other keywords
        const content = `Feature:
    Background:
    Scenario:
        `;

        const position = new vscode.Position(3, 8);
        const actual = await testCompletion(content, position);
        const expected = [
            'And',
            'But',
            'Example',
            'Examples',
            'Given',
            'Scenario',
            'Scenario Outline',
            'Scenario Template',
            'Scenarios',
            'Then',
            'When',
        ];

        expect(actual.items.map((value) => value.label)).to.deep.equal(expected);
        expect(actual.items.map((value) => value.kind)).to.deep.equal(new Array(expected.length).fill(vscode.CompletionItemKind.Keyword));
        expect(actual.items.map((value) => {
            if (value.insertText instanceof vscode.SnippetString) {
                return value.insertText.value;
            } else {
                return value.insertText;
            }
        })).to.deep.equal(['And ', 'But ', 'Example: ', 'Examples: ', 'Given ', 'Scenario: ', 'Scenario Outline: ', 'Scenario Template: ', 'Scenarios: ', 'Then ', 'When ']);

        await acceptAndAssertSuggestion(position, '        And ');
    });

    it('Complete keywords, keywords containing `en` (fuzzy matching)', async () => {
        // Complete keywords containing "en"
        const content = `Feature:
    Background:
    Scenario:
        G`;

        const position = new vscode.Position(3, 9);
        const actual = await testCompletion(content, position);

        expect(actual.items.map((value) => value.label)).to.deep.equal(['Given']);
        expect(actual.items.map((value) => value.kind)).to.deep.equal(
            new Array(1).fill(vscode.CompletionItemKind.Keyword)
        );

        await acceptAndAssertSuggestion(position, '        Given ');
    });
});

/**
 * Tests auto-completion for Grizzly step definitions.
 * Steps should be suggested based on the current keyword context and support
 * partial matching and completion of steps with existing parameter values.
 */
describe('Should do completion on steps', () => {
    it('Complete steps, keyword `Given` step `variable`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Given variable`;

        const position = new vscode.Position(3, 22);
        const actual = await testCompletion(content, position);
        const expected = [
            'set context variable "" to ""',
            'ask for value of variable ""',
            'set global context variable "" to ""',
            'set alias "" for variable ""',
            'value for variable "" is ""',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
            expect(expected).to.contain(item.label);
        });

        await acceptAndAssertSuggestion(position, '        Given value for variable "" is ""');
    });

    it('Complete steps, keyword `Then` step `save`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Then save`;

        const position = new vscode.Position(3, 17);
        const actual = await testCompletion(content, position);
        const expected = [
            'save response metadata "" in variable ""',
            'save response payload "" in variable ""',
            'save response payload "" that matches "" in variable ""',
            'save response metadata "" that matches "" in variable ""',
            'save optional response metadata "" in variable "" with default value ""',
            'save optional response payload "" in variable "" with default value ""',
            'save optional response payload "" that matches "" in variable "" with default value ""',
            'save optional response metadata "" that matches "" in variable "" with default value ""',
            'get from "" with name "" and save response payload in ""',
            'get "" from keystore and save in variable ""',
            'get "" from keystore and save in variable "", with default value ""',
            'parse date "" and save in variable ""',
            'parse "" as "undefined" and save value of "" in variable ""',
            'parse "" as "plain" and save value of "" in variable ""',
            'parse "" as "xml" and save value of "" in variable ""',
            'parse "" as "json" and save value of "" in variable ""',
        ];

        const actualLabels = actual.items.map((item) => item.label);

        expected.forEach((e) => {
            expect(actualLabels).to.contain(e);
        });

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
        });

        const actualInsertText = actual.items.map((item) => {
            if (item.insertText instanceof vscode.SnippetString) {
                return item.insertText.value;
            } else {
                return item.insertText;
            }
        });

        expected.forEach((e) => {
            const parts: string[] = [];
            let index = 1;
            for (const p of e.split('""')) {
                if (p === undefined || p.length < 1) {
                    continue;
                }
                parts.push(p);
                parts.push(`"$${index++}"`);
            }
            e = parts.join('');
            expect(actualInsertText).to.contain(e);
        });

        await acceptAndAssertSuggestion(position, '        Then save optional response metadata "" in variable "" with default value ""');
    });

    it('Complete steps, keyword `Then` step `save response metadata "hello"`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Then save response metadata "hello"`;
        const position = new vscode.Position(3, 43);
        const actual = await testCompletion(content, position);
        const expected = [
            'save response metadata "hello" in variable ""',
            'save response metadata "hello" that matches "" in variable ""',
        ];

        const actualLabels = actual.items.map((item) => item.label);

        expected.forEach((e) => {
            expect(actualLabels).to.contain(e);
        });

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
        });

        await acceptAndAssertSuggestion(position, '        Then save response metadata "hello" in variable ""');
    });

    it('Complete steps, keyword `When` step `<null>`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        When `;
        const position = new vscode.Position(3, 13);
        const actual = await testCompletion(content, position);
        const expected = [
            'any task fail restart scenario',
            'any task fail restart iteration',
            'any task fail retry task',
            'any task fail stop user',
            'any task fail continue',
            'any task fail with "" restart scenario',
            'any task fail with "" restart iteration',
            'any task fail with "" retry task',
            'any task fail with "" stop user',
            'any task fail with "" continue',
            'the task fails restart scenario',
            'the task fails restart iteration',
            'the task fails retry task',
            'the task fails stop user',
            'the task fails continue',
            'the task fails with "" restart scenario',
            'the task fails with "" restart iteration',
            'the task fails with "" retry task',
            'the task fails with "" stop user',
            'the task fails with "" continue',
            'condition "" with name "" is true, execute these tasks',
            'fail ratio is greater than ""% fail scenario',
            'average response time is greater than "" milliseconds fail scenario',
            'response time percentile ""% is greater than "" milliseconds fail scenario',
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
            expect(expected).to.contain(item.label);
        });

        await acceptAndAssertSuggestion(position, '        When any task fail continue');
    });

    it('Complete steps, keyword `When` step `<null>`, no space', async () => {
        const content = `Feature:
    Background:
    Scenario:
        When`;
        const position = new vscode.Position(3, 12);
        const actual = await testCompletion(content, position);
        const expected = [
            'any task fail restart scenario',
            'any task fail restart iteration',
            'any task fail retry task',
            'any task fail stop user',
            'any task fail continue',
            'any task fail with "" restart scenario',
            'any task fail with "" restart iteration',
            'any task fail with "" retry task',
            'any task fail with "" stop user',
            'any task fail with "" continue',
            'the task fails restart scenario',
            'the task fails restart iteration',
            'the task fails retry task',
            'the task fails stop user',
            'the task fails continue',
            'the task fails with "" restart scenario',
            'the task fails with "" restart iteration',
            'the task fails with "" retry task',
            'the task fails with "" stop user',
            'the task fails with "" continue',
            'condition "" with name "" is true, execute these tasks',
            'fail ratio is greater than ""% fail scenario',
            'average response time is greater than "" milliseconds fail scenario',
            'response time percentile ""% is greater than "" milliseconds fail scenario',
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
            expect(expected).to.contain(item.label);
        });

        await acceptAndAssertSuggestion(position, '        When any task fail continue');
    });

    it('Complete steps, keyword `When` step `response `', async () => {
        const content = `Feature:
    Background:
    Scenario:
        When response `;
        const position = new vscode.Position(3, 22);
        const actual = await testCompletion(content, position);
        const expected = [
            'average response time is greater than "" milliseconds fail scenario',
            'response time percentile ""% is greater than "" milliseconds fail scenario',
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
            expect(expected).to.contain(item.label);
        });

        await acceptAndAssertSuggestion(position, '        When response metadata "" is "" fail request');
    });

    it('Complete steps, keyword `When` step `response fail request`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        When response fail request`;
        const position = new vscode.Position(3, 34);
        const actual = await testCompletion(content, position);
        const expected = [
            'response payload "" is not "" fail request',
            'response payload "" is "" fail request',
            'response metadata "" is not "" fail request',
            'response metadata "" is "" fail request',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
            expect(expected).to.contain(item.label);
        });

        await acceptAndAssertSuggestion(position, '        When response payload "" is "" fail request');
    });

    it('Complete steps, keyword `When` step `response payload "" is fail request`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        When response payload "" is fail request`;
        const position = new vscode.Position(3, 49);
        const actual = await testCompletion(content, position);
        const expected = ['response payload "" is not "" fail request', 'response payload "" is "" fail request'];

        const actualLabels = actual.items.map((item) => item.label);

        expected.forEach((e) => {
            expect(actualLabels).to.contain(e);
        });

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
        });

        await acceptAndAssertSuggestion(position, '        When response payload "" is "" fail request');
    });

    it('Complete steps, keyword `And` step `repeat for "" it`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Given a user of type "RestApi" load testing "https://www.example.org"
        And repeat for "1" it`;
        const position = new vscode.Position(4, 29);
        const actual = await testCompletion(content, position);

        const actualLabels = actual.items.map((item) => item.label);
        const actualInsertText = actual.items.map((item) => item.insertText);
        expect(actualInsertText).to.be.eql(['repeat for "1" iteration', 'repeat for "1" iterations']);
        expect(actualLabels).to.be.eql(['repeat for "1" iteration', 'repeat for "1" iterations']);

        await acceptAndAssertSuggestion(position, '        And repeat for "1" iteration');
    });

    it('Complete steps, keyword `And` step `repeat for "" `', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Given a user of type "RestApi" load testing "https://www.example.org"
        And repeat for "1" `;
        const position = new vscode.Position(4, 28);
        const actual = await testCompletion(content, position);

        const actualLabels = actual.items.map((item) => item.label);
        const actualInsertText = actual.items.map((item) => item.insertText);
        expect(actualInsertText).to.be.eql(['repeat for "1" iteration', 'repeat for "1" iterations']);
        expect(actualLabels).to.be.eql(['repeat for "1" iteration', 'repeat for "1" iterations']);

        await acceptAndAssertSuggestion(position, '        And repeat for "1" iteration');
    });

    it('Complete steps, keyword `And` step `repeat for ""`', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Given a user of type "RestApi" load testing "https://www.example.org"
        And repeat for "1"`;
        const position = new vscode.Position(4, 27);
        const actual = await testCompletion(content, position);

        const actualLabels = actual.items.map((item) => item.label);
        const actualInsertText = actual.items.map((item) => item.insertText);
        expect(actualInsertText).to.be.eql(['repeat for "1" iteration', 'repeat for "1" iterations']);
        expect(actualLabels).to.be.eql(['repeat for "1" iteration', 'repeat for "1" iterations']);

        await acceptAndAssertSuggestion(position, '        And repeat for "1" iteration');
    });

    it('Complete steps, complete incompleted step, no trailing space', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Given a user of type "RestApi"`;
        const position = new vscode.Position(3, 39);
        const actual = await testCompletion(content, position);

        const actualInsertText = actual.items.map((item) => {
            if (item.insertText instanceof vscode.SnippetString) {
                return item.insertText.value;
            } else {
                return item.insertText;
            }
        });

        expect(actual.items.length).to.be.equal(2);

        const expected = [
            'a user of type "RestApi" load testing "$1"',
            'a user of type "RestApi" with weight "$1" load testing "$2"',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
        });

        actualInsertText.forEach((insertText) => {
            expect(expected).to.contain(insertText);
        });

        await acceptAndAssertSuggestion(position, '        Given a user of type "RestApi" load testing ""');
    });

    it('Complete steps, complete incompleted step, trailing space', async () => {
        const content = `Feature:
    Background:
    Scenario:
        Given a user of type "RestApi" `;
        const position = new vscode.Position(3, 40);
        const actual = await testCompletion(content, position);

        const actualInsertText = actual.items.map((item) => {
            if (item.insertText instanceof vscode.SnippetString) {
                return item.insertText.value;
            } else {
                return item.insertText;
            }
        });

        expect(actual.items.length).to.be.equal(2);

        const expected = [
            'a user of type "RestApi" load testing "$1"',
            'a user of type "RestApi" with weight "$1" load testing "$2"',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Function);
        });

        actualInsertText.forEach((insertText) => {
            expect(expected, `does not contain '${insertText}'`).to.contain(insertText);
        });

        await acceptAndAssertSuggestion(position, '        Given a user of type "RestApi" load testing ""');
    });
});

/**
 * Tests auto-completion for variable references within template strings.
 * Variables defined earlier in the feature file (using steps like "value for variable")
 * should be suggested when typing {{ inside quoted strings.
 */
describe('Should do completion on variables', () => {
    it('Complete variable, not a complete step, no ending "', async () => {
        const content = `Feature:
    Background:
    Scenario:
        And value for variable "foo" is "none"
        And value for variable "bar" is "none"
        And ask for value for variable "world"
        Then log message "{{`;

        const position = new vscode.Position(6, 28);
        const actual = await testCompletion(content, position);
        const expected = [
            ' foo }}"',
            ' bar }}"',
            ' world }}"',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Variable);
            expect(expected).to.contain(item.insertText);
        });

        await acceptAndAssertSuggestion(position, '        Then log message "{{ bar }}"');
    });

    it('Complete variable, partial variable, not a complete step, no ending "', async () => {
        const content = `Feature:
    Background:
    Scenario:
        And value for variable "foo" is "none"
        And value for variable "bar" is "none"
        And value for variable "boo" is "none"
        And ask for value for variable "world"
        Then log message "{{ b`;

        const position = new vscode.Position(7, 31);
        const actual = await testCompletion(content, position);
        const expected = [
            'bar }}"',
            'boo }}"',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Variable);
            expect(expected).to.contain(item.insertText);
        });

        await acceptAndAssertSuggestion(position, '        Then log message "{{ bar }}"');
    });

    it('Complete variable, complete step, ending }}"', async () => {
        const content = `Feature:
    Background:
    Scenario:
        And value for variable "foo" is "none"
        And value for variable "bar" is "none"
        And ask for value for variable "world"
        Then log message "{{}}"`;

        const position = new vscode.Position(6, 28);
        const actual = await testCompletion(content, position);
        const expected = [
            ' foo ',
            ' bar ',
            ' world ',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Variable);
            expect(expected).to.contain(item.insertText);
        });

        await acceptAndAssertSuggestion(position, '        Then log message "{{ bar }}"');
    });

    it('Complete variable, partial variable, complete step, ending }}"', async () => {
        const content = `Feature:
    Background:
    Scenario:
        And value for variable "foo" is "none"
        And value for variable "bar" is "none"
        And ask for value for variable "boo"
        Then log message "{{ b}}"`;

        const position = new vscode.Position(6, 30);
        const actual = await testCompletion(content, position);
        const expected = [
            'bar ',
            'boo ',
        ];

        actual.items.forEach((item) => {
            expect(item.kind).to.be.equal(vscode.CompletionItemKind.Variable);
            expect(expected).to.contain(item.insertText);
        });

        await acceptAndAssertSuggestion(position, '        Then log message "{{ bar }}"');
    });
});

/**
 * Helper function to execute completion at a specific position in a test document.
 *
 * @param content - The feature file content to test against
 * @param position - The cursor position where completion should be triggered
 * @returns A promise resolving to the completion list provided by the language server
 */
async function testCompletion(content: string, position: vscode.Position): Promise<vscode.CompletionList> {
    const docUri = getDocUri('features/empty.feature');
    await activate(docUri, content);

    // Executing the command `vscode.executeCompletionItemProvider` to simulate triggering completion
    return (await vscode.commands.executeCommand(
        'vscode.executeCompletionItemProvider',
        docUri,
        position
    )) as vscode.CompletionList;
}
