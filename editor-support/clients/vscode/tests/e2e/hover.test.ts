import * as vscode from 'vscode';
import { expect } from 'chai';
import { getDocUri, activate } from './helper';
import { describe, it } from 'mocha';

describe('Should show help on hover step expression', () => {
    it('Hover in empty file', async () => {
        const actual = await testHover('', new vscode.Position(0, 0));
        expect(actual).to.equal(undefined);
    });

    it('Hover `Given a user...`', async () => {
        const content = `Feature:
    Scenario: test scenario
        Given a user of type "RestApi" with weight "1" load testing "$conf::template.host"
        When any task fail restart scenario
`;

        let end = 89;
        if (process.platform == 'win32') {
            end = 90;  // due to \r\n on win32?
        }

        const actual = await testHover(content, new vscode.Position(2, 35));

        expect(actual.range?.start.line).to.be.equal(2);
        expect(actual.range?.start.character).to.be.equal(8);
        expect(actual.range?.end.line).to.be.equal(2);
        expect(actual.range?.end.character).to.be.equal(end);
        const contents = actual.contents[0] as vscode.MarkdownString;
        expect(contents.value).to.be
            .equal(`Set which type of load user the scenario should use and which \`host\` is the target,
together with \`weight\` of the user (how many instances of this user should spawn relative to others).

Example:
\`\`\`gherkin
Given a user of type "RestApi" with weight "2" load testing "..."
Given a user of type "MessageQueue" with weight "1" load testing "..."
Given a user of type "ServiceBus" with weight "1" load testing "..."
Given a user of type "BlobStorage" with weight "4" load testing "..."
\`\`\`

Args:

* user_class_name \`str\`: name of an implementation of load user, with or without \`User\`-suffix
* weight_value \`int\`: weight value for the user, default is \`1\` (see [writing a locustfile](http://docs.locust.io/en/stable/writing-a-locustfile.html#weight-attribute))
* host \`str\`: an URL for the target host, format depends on which load user is specified
`);
    });

    it('Hover `When any task fail restart scenario`', async () => {
        const content = `Feature:
    Scenario: test scenario
        Given a user of type "RestApi" with weight "1" load testing "$conf::template.host"
        When any task fail restart scenario
`;

        let end = 42;
        if (process.platform == 'win32') {
            end = 43;  // due to \r\n on win32?
        }

        const actual = await testHover(content, new vscode.Position(3, 15));

        expect(actual.range?.start.line).to.be.equal(3);
        expect(actual.range?.start.character).to.be.equal(8);
        expect(actual.range?.end.line).to.be.equal(3);
        expect(actual.range?.end.character).to.be.equal(end);

        const contents = actual.contents[0] as vscode.MarkdownString;
        expect(contents.value).to.be
            .equal(`Set default behavior when latest task fail.

If no default behavior is set, the scenario will continue as nothing happened.

Example:
\`\`\`gherkin
When any task fail restart scenario
When any task fail stop user
\`\`\`

Args:

* failure_action \`FailureAction\`: default failure action when nothing specific is matched
`);
    });

});

async function testHover(content: string, position: vscode.Position): Promise<vscode.Hover> {
    const docUri = getDocUri('features/empty.feature');
    await activate(docUri, content);

    const [hover] = (await vscode.commands.executeCommand(
        'vscode.executeHoverProvider',
        docUri,
        position
    )) as vscode.Hover[];

    return hover;
}
