/**
 * Grizzly Language Server VS Code Extension
 *
 * This extension provides language server integration for Grizzly Gherkin feature files.
 * It manages the lifecycle of the language server, handles Python environment detection,
 * and provides preview capabilities for Gherkin features.
 */
'use strict';

import * as net from 'net';
import * as vscode from 'vscode';
import * as util from 'util';
import * as child_process from 'child_process';
import * as path from 'path';

import { LanguageClient, LanguageClientOptions, ServerOptions, State } from 'vscode-languageclient/node';
import { PythonExtension } from '@vscode/python-extension';

import { Settings, ExtensionStatus } from './model';
import { GherkinPreview, GherkinPreviewOptions } from './preview';
import { ConsoleLogOutputChannel } from './log';

const exec = util.promisify(child_process.exec);

/** Output channel for logging extension and language server messages */
let logger: ConsoleLogOutputChannel;

/** Active language client instance */
let client: LanguageClient;

/** Python extension API instance */
let python: PythonExtension;

/** URI of the document that triggered language server activation */
let documentUri: vscode.Uri;

/** URI of the language server source directory (for hot-reload) */
let serverUri: vscode.Uri;

/** Flag indicating if language server is currently starting */
let starting = false;

/** Flag indicating if extension is fully activated */
let activated = false;

/** Flag to prevent duplicate warning messages about server startup */
let notifiedAboutWaiting = false;

/** Extension status tracking object */
const status: ExtensionStatus = {
    isActivated: () => {
        return activated;
    },
    setActivated: (status: boolean = true) => {
        activated = status;
    }
};


/**
 * Creates a language server client using stdio (standard input/output) communication.
 *
 * This is the default communication method where the language server runs as a subprocess
 * and communicates via stdin/stdout.
 *
 * @param module - Python module name containing the language server (e.g., 'grizzly_ls')
 * @param args - Additional command-line arguments to pass to the language server
 * @param documentSelector - Array of language IDs this server should handle
 * @param initializationOptions - Extension settings to pass to the server on initialization
 * @returns Promise resolving to the configured LanguageClient instance
 */
async function createStdioLanguageServer(
    module: string,
    args: string[],
    documentSelector: string[],
    initializationOptions: Settings,
): Promise<LanguageClient> {
    const python = await getPythonPath();
    const command = `${python} -c "import ${module}; import inspect; print(inspect.getsourcefile(${module}));"`;

    try {
        const { stdout } = await exec(command);
        serverUri = vscode.Uri.file(path.dirname(stdout.trim()));
        logger.debug(`serverUri = "${serverUri}"`);
    } catch (error) {
        logger.error(command);
        logger.error(`Failed ^ to get module path for ${module}: ${error}`);
        logger.error('Hot-reload of language server will not work');
    }

    args = ['-m', module, '--embedded', ...args];

    if (process.env.VERBOSE) {
        if (!args.includes('--verbose')) {
            args = [...args, '--verbose'];
        }
        logger.warn('Starting language server in verbose mode');
    }

    const serverOptions: ServerOptions = {
        command: python,
        args,
    };

    const clientOptions: LanguageClientOptions = {
        documentSelector: documentSelector,
        markdown: {
            isTrusted: true,
        },
        outputChannel: logger.channel,
        initializationOptions,
    };

    return new LanguageClient(python, serverOptions, clientOptions);
}

/**
 * Creates a language server client using socket communication.
 *
 * This method connects to an already-running language server via TCP socket,
 * useful for debugging or when the server is running externally.
 *
 * @param host - Hostname or IP address of the language server (e.g., 'localhost')
 * @param port - Port number the language server is listening on
 * @param documentSelector - Array of language IDs this server should handle
 * @param initializationOptions - Extension settings to pass to the server on initialization
 * @returns Configured LanguageClient instance
 */
function createSocketLanguageServer(
    host: string,
    port: number,
    documentSelector: string[],
    initializationOptions: Settings,
): LanguageClient {
    const serverOptions: ServerOptions = () => {
        return new Promise((resolve) => {
            const client = new net.Socket();
            client.connect(port, host, () => {
                resolve({
                    reader: client,
                    writer: client,
                });
            });
        });
    };

    const clientOptions: LanguageClientOptions = {
        documentSelector: documentSelector,
        outputChannel: logger.channel,
        markdown: {
            isTrusted: true,
        },
        initializationOptions,
    };

    return new LanguageClient(`socket language server (${host}:${port})`, serverOptions, clientOptions);
}

/**
 * Creates a language server client based on user configuration.
 *
 * Reads the `grizzly.server.connection` setting to determine whether to use
 * stdio or socket communication mode.
 *
 * @returns Promise resolving to the configured LanguageClient instance
 * @throws Error if connection type is invalid
 */
async function createLanguageClient(): Promise<LanguageClient> {
    const configuration = vscode.workspace.getConfiguration('grizzly');
    const documentSelector = ['grizzly-gherkin'];
    let languageClient: LanguageClient;

    const connectionType = configuration.get<string>('server.connection');

    const settings = <Settings>(<unknown>configuration);

    switch (connectionType) {
        case 'stdio':
            languageClient = await createStdioLanguageServer(
                configuration.get<string>('stdio.module') || 'grizzly_ls',
                configuration.get<Array<string>>('stdio.args') || [],
                documentSelector,
                settings,
            );
            break;
        case 'socket':
            languageClient = createSocketLanguageServer(
                configuration.get<string>('socket.host') || 'localhost',
                configuration.get<number>('socket.port') || 4444,
                documentSelector,
                settings,
            );
            break;
        default:
            throw new Error(`${connectionType} is not a valid setting for grizzly.server.connection`);
    }

    return languageClient;
}

/**
 * Resolves the path to the Python interpreter to use for the language server.
 *
 * Prioritizes the active virtual environment if one exists, otherwise uses
 * the Python extension's active environment.
 *
 * @returns Promise resolving to the absolute path of the Python executable
 * @throws Error if unable to resolve the environment or find the executable
 */
async function getPythonPath(): Promise<string> {
    // make sure all environments are loaded
    await python.environments.refreshEnvironments();

    // use virtual env, if one is active
    const envPath = process.env['VIRTUAL_ENV'] || python.environments.getActiveEnvironmentPath().path;

    logger.debug(`Active environment path: ${envPath}`);

    const env = await python.environments.resolveEnvironment(envPath);

    if (!env) {
        throw new Error(`Unable to resolve environment: ${env}`);
    }

    const pythonUri = env.executable.uri;
    if (!pythonUri) {
        throw new Error('Python executable URI not found');
    }

    logger.info(`Using interpreter: ${pythonUri.fsPath}`);

    return pythonUri.fsPath;
}

/**
 * Loads and initializes the Python extension API.
 *
 * This must be called before attempting to use Python environment functionality.
 *
 * @returns Promise that resolves when the Python extension is loaded
 */
async function getPythonExtension(): Promise<void> {
    try {
        python = await PythonExtension.api();
    } catch (err) {
        logger.error(`Unable to load python extension: ${err}`);
    }
}

/**
 * Starts the language server.
 *
 * Creates a new language client, starts it, and sends the installation request
 * to ensure dependencies are set up. If a server is already running, it will
 * be stopped first.
 *
 * @returns Promise that resolves when the server is started and initialized
 */
async function startLanguageServer(): Promise<void> {
    if (starting) {
        return;
    }

    starting = true;
    if (client) {
        await stopLanguageServer();
    }

    try {
        client = await createLanguageClient();
        await client.start();
        logger.info(`Installing based on ${documentUri.path}`);
        await client.sendRequest('grizzly-ls/install', {});
        status.setActivated(true);
    } catch (error) {
        logger.error(`Unable to start language server: ${error}`);
    } finally {
        starting = false;
    }
}

/**
 * Stops the language server and cleans up resources.
 *
 * If the server is running, sends a stop request and disposes of the client instance.
 *
 * @returns Promise that resolves when the server is stopped
 */
async function stopLanguageServer(): Promise<void> {
    if (!client) {
        return;
    }

    if (client.state === State.Running) {
        await client.stop();
    }

    status.setActivated(false);

    client.dispose();
    client = undefined;
}

/**
 * Activates the Grizzly extension.
 *
 * This function is called by VS Code when the extension is activated. It:
 * - Initializes the logger and preview functionality
 * - Loads the Python extension
 * - Registers all commands and event handlers
 * - Sets up auto-start when Grizzly Gherkin files are opened
 * - Configures hot-reload for language server development
 *
 * @param context - VS Code extension context for registering subscriptions
 * @returns Promise resolving to the extension status object, or undefined if Python extension fails to load
 */
export async function activate(context: vscode.ExtensionContext): Promise<ExtensionStatus | undefined> {
    logger = new ConsoleLogOutputChannel('Grizzly Language Server', {log: true});

    const previewer = new GherkinPreview(context, logger);

    await getPythonExtension();
    if (!python) {
        return;
    }

    // <!-- register custom commands
    context.subscriptions.push(
        vscode.commands.registerCommand('grizzly.server.restart', async () => {
            const message = (status.isActivated()) ? 'Restarting language server' : 'Starting language server';
            logger.info(message);

            await startLanguageServer();
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('grizzly.server.inventory.rebuild', async () => {
            if (client) {
                await vscode.window.showInformationMessage('Saving all open files before rebuilding step inventory');
                vscode.workspace.textDocuments.forEach(async (textDocument: vscode.TextDocument) => {
                    await textDocument.save();
                });
                await vscode.commands.executeCommand('grizzly-ls/rebuild-inventory');
            }
        })
    );

    context.subscriptions.push(
        vscode.commands.registerCommand('grizzly.server.diagnostics.run', async () => {
            const textEditor = vscode.window.activeTextEditor;
            const textDocument = textEditor.document;

            if (textDocument.languageId === 'grizzly-gherkin') {
                await vscode.commands.executeCommand('grizzly-ls/run-diagnostics', textDocument);
            }
        })
    );
    // -->

    // when active texteditor is changed, run diagnostics on the new active document in the texteditor
    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor(async (textEditor: vscode.TextEditor | undefined) => {
            if (textEditor === undefined || !client || client.state !== State.Running) {
                return;
            }

            const textDocument = textEditor.document;
            if (textDocument.languageId === 'grizzly-gherkin') {
                await vscode.commands.executeCommand('grizzly-ls/run-diagnostics', textDocument);
                previewer.preview(textDocument, true);
            }
        })
    );

    // restart if any changes to the python environment was made
    context.subscriptions.push(
        python.environments.onDidChangeActiveEnvironmentPath(async () => {
            if (client) {
                logger.info('Python environment modified, restarting server');
                await startLanguageServer();
            }
        })
    );

    // restart if any related configuration changes has been made
    context.subscriptions.push(
        vscode.workspace.onDidChangeConfiguration(async (event) => {
            if (event.affectsConfiguration('grizzly') && status.isActivated()) {
                logger.info('Settings changed, restarting server');
                await startLanguageServer();
            }
        })
    );

    // hot reload if a change to its source was made
    context.subscriptions.push(
        vscode.workspace.onDidSaveTextDocument(async (textDocument: vscode.TextDocument) => {
            if (serverUri !== undefined && textDocument.uri.path.startsWith(serverUri.path)) {
                logger.info(`Hot-reloading server: ${textDocument.uri.toString()} modified`);
                await startLanguageServer();
            }

            await previewer.update(textDocument);
        })
    );

    // start if it's not already started, and a `grizzly-gherkin` document is opened
    context.subscriptions.push(
        vscode.workspace.onDidOpenTextDocument(async (textDocument: vscode.TextDocument) => {
            if (!client && textDocument.languageId === 'grizzly-gherkin') {
                documentUri = textDocument.uri;
                await startLanguageServer();
            }
        })
    );

    // close preview if file closes
    context.subscriptions.push(
        vscode.workspace.onDidCloseTextDocument(async (textDocument: vscode.TextDocument) => {
            previewer.close(textDocument);
        })
    );

    // update preview if text document changes
    context.subscriptions.push(
        vscode.workspace.onDidChangeTextDocument(async (event: vscode.TextDocumentChangeEvent) => {
            const textDocument = event.document;
            await previewer.update(textDocument, undefined, true);
        })
    );

    // start if there are any open `grizzly-gherkin` files open
    vscode.workspace.textDocuments.forEach(async (textDocument: vscode.TextDocument) => {
        if (!client && textDocument.languageId === 'grizzly-gherkin') {
            documentUri = textDocument.uri;
            await startLanguageServer();
        }
    });

    // disable vscode builtin handler of `file://` url's, since it interferse with grizzly-ls definitions
    // https://github.com/microsoft/vscode/blob/f1f645f4ccbee9d56d091b819a81d34af31be17f/src/vs/editor/contrib/links/links.ts#L310-L330
    const configuration = vscode.workspace.getConfiguration('', {languageId: 'grizzly-gherkin'});
    configuration.update('editor.links', false, false, true);

    // add preview capabilities
    context.subscriptions.push(
        vscode.commands.registerCommand('grizzly.gherkin.preview.beside', (options: GherkinPreviewOptions) => {
            if (!client) {
                if (!notifiedAboutWaiting) {
                    vscode.window.showWarningMessage('Wait until language server has started').then(() => notifiedAboutWaiting = true);
                }
                return;
            }

            if (!options.content
                && !options.document
                && vscode.window.activeTextEditor?.document
                && vscode.window.activeTextEditor?.document.languageId === 'grizzly-gherkin'
            ) {
                options.document = vscode.window.activeTextEditor.document;
            }

            const execute = (opts: GherkinPreviewOptions) => {
                previewer.preview(opts.document).then(() => {
                    logger.debug(`preview panel created for ${opts.document?.uri}`);
                });
            };

            execute(options);
        })
    );

    return status;
}

/**
 * Deactivates the Grizzly extension.
 *
 * This function is called by VS Code when the extension is deactivated.
 * It ensures the language server is properly stopped.
 *
 * @returns Promise that resolves when deactivation is complete
 */
export function deactivate(): Thenable<void> {
    return stopLanguageServer();
}
