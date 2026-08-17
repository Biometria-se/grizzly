import * as core from '@actions/core';
import * as exec from '@actions/exec';

/**
 * Set the runner's timezone based on its operating system.
 * @param {object} options - Configuration options
 * @param {string} options.platform - `process.platform` value (defaults to `process.platform`)
 * @param {string} options.timezoneLinux - Timezone to set on Linux
 * @param {string} options.timezoneWindows - Timezone to set on Windows
 * @param {string} options.timezoneMacos - Timezone to set on macOS
 * @param {Function} options.execute - Command executor (defaults to `@actions/exec`)
 * @returns {Promise<void>}
 */
export async function setTimezone(options = {}) {
    const {
        platform = process.platform,
        timezoneLinux,
        timezoneWindows,
        timezoneMacos,
        execute = exec.exec
    } = options;

    switch (platform) {
        case 'linux':
            await execute('sudo', ['timedatectl', 'set-timezone', timezoneLinux]);
            break;
        case 'darwin':
            await execute('sudo', ['systemsetup', '-settimezone', timezoneMacos]);
            break;
        case 'win32':
            await execute('tzutil', ['/s', timezoneWindows]);
            break;
        default:
            throw new Error(`Platform ${platform} not supported; only linux, darwin or win32 are supported`);
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
        await setTimezone({
            platform,
            timezoneLinux: coreModule.getInput('timezoneLinux'),
            timezoneWindows: coreModule.getInput('timezoneWindows'),
            timezoneMacos: coreModule.getInput('timezoneMacos'),
            execute
        });
    } catch (error) {
        coreModule.setFailed(error.message);
    }
}

if (process.env.GITHUB_ACTIONS) {
    run();
}
