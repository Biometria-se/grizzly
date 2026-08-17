import * as core from '@actions/core';
import * as exec from '@actions/exec';

const CLEANUP_SCRIPT = [
    'xvfb_pids=$(ps aux | grep "tmp/xvfb-run" | grep -v grep | awk \'{print $2}\')',
    'if [ -n "$xvfb_pids" ]; then',
    '  echo "Killing the following xvfb processes: $xvfb_pids"',
    '  sudo kill $xvfb_pids',
    'else',
    '  echo "No xvfb processes to kill"',
    'fi'
].join('\n');

/**
 * Install Xvfb and its X11 utilities (Linux only).
 * @param {Function} execute - Command executor (defaults to `@actions/exec`)
 * @returns {Promise<void>}
 */
export async function installXvfb(execute = exec.exec) {
    await execute('sudo apt-get update');
    await execute('sudo apt-get install -y xvfb x11-xserver-utils');
}

/**
 * Kill any lingering `xvfb-run` processes started by this action.
 * @param {Function} execute - Command executor (defaults to `@actions/exec`)
 * @returns {Promise<void>}
 */
export async function cleanupXvfb(execute = exec.exec) {
    try {
        await execute('bash', ['-c', CLEANUP_SCRIPT]);
    } catch {
        // best-effort cleanup, ignore failures
    }
}

/**
 * Run a single command, optionally in a given working directory.
 * @param {string} command - Command to run
 * @param {object} options - Configuration options
 * @param {string} [options.directory] - Working directory
 * @param {Function} [options.execute] - Command executor (defaults to `@actions/exec`)
 * @returns {Promise<void>}
 */
export async function runCommand(command, options = {}) {
    const { directory, execute = exec.exec } = options;
    await execute(command, [], directory ? { cwd: directory } : undefined);
}

/**
 * Run a single command wrapped with `xvfb-run`, cleaning up afterwards.
 * @param {string} command - Command to run
 * @param {object} options - Configuration options
 * @param {string} [options.directory] - Working directory
 * @param {string} [options.serverOptions] - Xvfb server options
 * @param {Function} [options.execute] - Command executor (defaults to `@actions/exec`)
 * @returns {Promise<void>}
 */
export async function runCommandWithXvfb(command, options = {}) {
    const { directory, serverOptions, execute = exec.exec } = options;
    const optionsArgument = serverOptions ? `-s "${serverOptions}" ` : '';
    const wrappedCommand = `xvfb-run --auto-servernum ${optionsArgument}${command}`;

    try {
        await runCommand(wrappedCommand, { directory, execute });
    } finally {
        await cleanupXvfb(execute);
    }
}

/**
 * Run each command, wrapped with Xvfb on Linux.
 * @param {object} options - Configuration options
 * @param {string} options.platform - `process.platform` value (defaults to `process.platform`)
 * @param {string[]} options.commands - Commands to run
 * @param {string} [options.directory] - Working directory
 * @param {string} [options.serverOptions] - Xvfb server options
 * @param {Function} [options.execute] - Command executor (defaults to `@actions/exec`)
 * @param {object} [options.logger] - Logger with an `info` method (defaults to `@actions/core`)
 * @returns {Promise<void>}
 */
export async function setupXvfb(options = {}) {
    const {
        platform = process.platform,
        commands,
        directory,
        serverOptions,
        execute = exec.exec,
        logger = core
    } = options;

    if (platform === 'linux') {
        await installXvfb(execute);
    }

    for (const command of commands) {
        if (!command) {
            continue;
        }

        if (platform === 'linux') {
            logger.info(`Command: ${command}`);
            await runCommandWithXvfb(command, { directory, serverOptions, execute });
        } else {
            await runCommand(command, { directory, execute });
        }
    }
}

/**
 * Run the action in GitHub Actions mode
 * @param {object} dependencies - Dependency injection object
 * @param {object} dependencies.core - GitHub Actions core module
 * @param {Function} dependencies.execute - Command executor
 * @param {string} dependencies.platform - `process.platform` value
 * @returns {Promise<void>}
 */
export async function run(dependencies = {}) {
    const {
        core: coreModule = core,
        execute = exec.exec,
        platform = process.platform
    } = dependencies;

    try {
        const commands = coreModule.getInput('run', { required: true }).split('\n');
        const directory = coreModule.getInput('working-directory') || undefined;
        const serverOptions = coreModule.getInput('options') || undefined;

        await setupXvfb({
            platform,
            commands,
            directory,
            serverOptions,
            execute,
            logger: coreModule
        });
    } catch (error) {
        coreModule.setFailed(error.message);
    }
}

if (process.env.GITHUB_ACTIONS) {
    run();
}
