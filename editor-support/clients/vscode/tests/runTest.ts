import * as path from 'path';
import * as cp from 'child_process';
import * as fs from 'fs/promises';

import { homedir, tmpdir } from 'os';
import { runTests, downloadAndUnzipVSCode, resolveCliArgsFromVSCodeExecutablePath, runVSCodeCommand } from '@vscode/test-electron';

async function installExtension(vscodeExecutablePath: string, extensionId: string): Promise<string> {
    const home = homedir();
    let possibleExtensionPaths = [path.join(home, '.vscode-server'), path.join(home, '.vscode')];
    const [cli, ...args] = resolveCliArgsFromVSCodeExecutablePath(vscodeExecutablePath);

    // install dependency
    cp.spawnSync(cli, [...args, '--install-extension', extensionId], {
        encoding: 'utf-8',
        stdio: 'inherit',
    });

    for (const arg_value of args) {
        const [arg, value] = arg_value.split('=', 2);

        if (arg === '--extensions-dir') {
            possibleExtensionPaths = [path.dirname(value), ...possibleExtensionPaths];
            break;
        }
    }

    // copy installed extension

    for (const possibleExtensionPath of possibleExtensionPaths) {
        console.log(`?? trying ${possibleExtensionPath}`);
        try {
            await fs.access(possibleExtensionPath);
        } catch {  // does not exist try next one
            continue;
        }

        const extensionDir = path.join(possibleExtensionPath, 'extensions');
        try {
            await fs.access(extensionDir);
        } catch { // weird... but hey, let's cover it
            continue;
        }
        const extensions = (await fs.readdir(extensionDir, {withFileTypes: true})).filter(dir => dir.isDirectory()).map(dir => dir.name)
            .filter(name => name.startsWith(extensionId));

        if (extensions.length < 1) {
            continue;
        }

        const extension = extensions[0];

        console.log(`!! extensionDir=${extensionDir}, extension=${extension}`);

        const extensionDevelopmentPathExtra = path.join(tmpdir(), extension);

        await fs.cp(path.join(extensionDir, extension), extensionDevelopmentPathExtra, {recursive: true});

        console.log(`!! extensionDevelopmentPathExtra=${extensionDevelopmentPathExtra}`);

        return extensionDevelopmentPathExtra;
    }

    throw Error(`could not find extension ${extensionId}`);
}

async function main() {
    try {
        const extensionTestsPath = path.resolve(__dirname, './index');
        const testWorkspace: string = path.resolve(__dirname, '../../../../tests/project');
        console.log(`!! dirname=${__dirname}, extenstionTestsPath=${extensionTestsPath}, testWorkspace=${testWorkspace}`);
        console.log(`!! VIRTUAL_ENV=${process.env['VIRTUAL_ENV']}`);

        const vscodeExecutablePath = await downloadAndUnzipVSCode();

        await runVSCodeCommand(['--install-extension', 'ms-python.python', '--force']);

        const extensionDevelopmentPathExtra = await installExtension(vscodeExecutablePath, 'ms-python.python');

        // The folder containing the Extension Manifest package.json
        // Passed to `--extensionDevelopmentPath`
        const extensionDevelopmentPath = [path.resolve(__dirname, '../../'), extensionDevelopmentPathExtra];

        // The path to test runner
        // Passed to --extensionTestsPath

        const argv = process.argv.slice(2);

        process.env['TESTS'] = `${argv}`;

        // Download VS Code, unzip it and run the integration test
        await runTests({
            vscodeExecutablePath,
            extensionDevelopmentPath,
            extensionTestsPath,
            launchArgs: [testWorkspace],
        });
        process.exit(0);
    } catch (err) {
        console.error(err);
        console.error('Failed to run tests');
        process.exit(1);
    }
}

main();
