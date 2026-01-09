import * as core from '@actions/core';
import os from 'os';
import path from 'path';
import fs from 'fs';

/**
 * Setup environment variables and PATH for GitHub Actions workflow
 * @param {object} options - Configuration options
 * @param {string[]|null} options.envVars - Environment variables to add (key=value format)
 * @param {string[]|null} options.paths - Paths to add to PATH variable
 * @param {object} options.env - Environment variables object (defaults to process.env)
 * @param {object} options.logger - Logger object with info method (defaults to core)
 * @returns {Promise<{envVars: string[], paths: string[]}>}
 */
export async function setupEnvironment(options = {}) {
    const {
        envVars = null,
        paths = null,
        env = process.env,
        logger = core
    } = options;

    let finalEnvVars = envVars;
    let finalPaths = paths;

    // If no env vars or paths provided, use defaults
    if (finalEnvVars === null && finalPaths === null) {
        const workspace = env.GITHUB_WORKSPACE;
        if (!workspace) {
            throw new Error('GITHUB_WORKSPACE environment variable is not set');
        }

        const virtualEnv = path.join(workspace, '.venv');
        const isWindows = process.platform === 'win32';
        const virtualEnvPath = path.join(virtualEnv, isWindows ? 'Scripts' : 'bin');
        const tmpDir = os.tmpdir();
        const grizzlyTmpLogfile = path.join(tmpDir, 'grizzly.log');

        if (finalEnvVars === null) {
            finalEnvVars = [
                `VIRTUAL_ENV=${virtualEnv}`,
                `GRIZZLY_TMP_DIR=${tmpDir}`,
                `GRIZZLY_TMP_LOGFILE=${grizzlyTmpLogfile}`
            ];
        }

        if (finalPaths === null) {
            finalPaths = [virtualEnvPath];
        }
    }

    // Add paths to PATH
    if (finalPaths !== null && finalPaths.length > 0) {
        const githubPath = env.GITHUB_PATH;
        if (!githubPath) {
            throw new Error('GITHUB_PATH environment variable is not set');
        }

        for (const pathItem of finalPaths) {
            fs.appendFileSync(githubPath, `${pathItem}\n`);
        }

        logger.info(`Added paths to PATH variable:\n${JSON.stringify(finalPaths, null, 2)}`);
    }

    // Add environment variables
    if (finalEnvVars !== null && finalEnvVars.length > 0) {
        const githubEnv = env.GITHUB_ENV;
        if (!githubEnv) {
            throw new Error('GITHUB_ENV environment variable is not set');
        }

        for (const envVar of finalEnvVars) {
            const [key, ...valueParts] = envVar.split('=');
            let value = valueParts.join('=');

            // Special handling for LD_LIBRARY_PATH - append to existing value
            if (key === 'LD_LIBRARY_PATH') {
                const currentValue = env[key];
                if (currentValue) {
                    const pathSeparator = process.platform === 'win32' ? ';' : ':';
                    value = `${value}${pathSeparator}${currentValue}`;
                }
            }

            fs.appendFileSync(githubEnv, `${key}=${value}\n`);
        }

        logger.info(`Added environment variables:\n${JSON.stringify(finalEnvVars, null, 2)}`);
    }

    return {
        envVars: finalEnvVars || [],
        paths: finalPaths || []
    };
}

/**
 * Run the action in GitHub Actions mode
 * @param {object} dependencies - Dependency injection object
 * @param {object} dependencies.core - GitHub Actions core module
 * @param {object} dependencies.env - Environment variables object (defaults to process.env)
 * @returns {Promise<void>}
 */
export async function run(dependencies = {}) {
    const {
        core: coreModule = core,
        env = process.env
    } = dependencies;

    try {
        const addEnvInput = coreModule.getInput('add-env');
        const addPathInput = coreModule.getInput('add-path');

        // Parse comma-separated inputs
        const envVars = addEnvInput ? addEnvInput.split(',').map(s => s.trim()).filter(s => s) : null;
        const paths = addPathInput ? addPathInput.split(',').map(s => s.trim()).filter(s => s) : null;

        coreModule.info('Setting up environment...');

        await setupEnvironment({
            envVars,
            paths,
            env,
            logger: coreModule
        });

        coreModule.info('Environment setup completed successfully');
    } catch (error) {
        coreModule.setFailed(error.message);
    }
}

// Only run if this is the main module being executed
const isMainModule = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(process.argv[1]);

if (isMainModule && process.env.GITHUB_ACTIONS) {
    run();
}
