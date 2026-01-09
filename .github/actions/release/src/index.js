import * as core from '@actions/core';
import * as exec from '@actions/exec';
import * as github from '@actions/github';
import { readFileSync, existsSync } from 'fs';
import { join, resolve } from 'path';
import * as semver from 'semver';
import { parse as parseToml } from '@iarna/toml';
import { fileURLToPath } from 'url';

/**
 * Get the next release tag based on project configuration
 * @param {string} projectPath - Path to the project directory
 * @param {string} bump - Version bump type (major, minor, patch)
 * @param {object} logger - Logger object with info, warning, error methods (defaults to core)
 * @param {object} execModule - Exec module for running commands (defaults to exec)
 * @returns {Promise<{nextVersion: string, nextTag: string}>}
 */
export async function getNextReleaseTag(projectPath, bump, logger = core, execModule = exec) {
    let tagPattern;

    // Check for pyproject.toml first
    const pyprojectPath = join(projectPath, 'pyproject.toml');
    if (existsSync(pyprojectPath)) {
        const pyprojectContent = readFileSync(pyprojectPath, 'utf8');
        const pyproject = parseToml(pyprojectContent);

        const describeCommand = pyproject?.tool?.hatch?.version?.['raw-options']?.scm?.git?.describe_command;
        if (!describeCommand) {
            throw new Error('no git.scm.describe_command found in pyproject.toml');
        }

        const regex = /git.*--match ['"]([^'"]+)['"]/;
        const match = describeCommand.match(regex);

        if (!match) {
            throw new Error('no tag pattern found in git.scm.describe_command');
        }

        tagPattern = match[1];
    } else {
        // Check for package.local.json
        const packageJsonPath = join(projectPath, 'package.local.json');
        if (!existsSync(packageJsonPath)) {
            throw new Error('no recognized project file found in the specified directory');
        }

        const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
        tagPattern = packageJson.tag?.pattern;

        if (!tagPattern) {
            throw new Error('no version pattern found in package.local.json');
        }
    }

    logger.info(`Tag pattern: ${tagPattern}`);

    // Get existing tags
    const gitTagArgs = [
        'tag',
        '-l',
        tagPattern,
        '--sort=-version:refname',
        '--format=%(refname:lstrip=2)'
    ];

    // Log command for visibility (especially in CLI mode)
    if (logger.info) {
        logger.info(`[command]git ${gitTagArgs.join(' ')}`);
    }

    let stdout = '';
    await execModule.exec('git', gitTagArgs, {
        silent: false,
        listeners: {
            stdout: (data) => {
                stdout += data.toString();
            }
        }
    });

    const tags = stdout.trim().split('\n').filter(t => t);
    const previousTag = tags.length > 0 ? tags[0] : tagPattern.replace('v*[0-9]*', 'v0.0.1');

    logger.info(`Previous tag    : ${previousTag}`);

    // Split tag into prefix and version
    const [tagPrefix, previousTagVersion] = previousTag.split('@');
    const previousVersion = previousTagVersion.replace(/^v/, '');

    logger.info(`Tag prefix      : ${tagPrefix}`);
    logger.info(`Previous version: ${previousVersion}`);

    // Parse and bump version
    const currentVersion = semver.parse(previousVersion);
    if (!currentVersion) {
        throw new Error(`Invalid semver version: ${previousVersion}`);
    }

    const nextVersion = semver.inc(currentVersion, bump);
    if (!nextVersion) {
        throw new Error(`Invalid bump type: ${bump}`);
    }

    const nextTag = `${tagPrefix}@v${nextVersion}`;

    logger.info(`Next version    : ${nextVersion}`);
    logger.info(`Next tag        : ${nextTag}`);

    return { nextVersion, nextTag };
}

async function run() {
    try {
        const project = core.getInput('project', { required: true });
        const versionBump = core.getInput('version-bump', { required: true });
        const dryRun = core.getInput('dry-run') === 'true'; const token = core.getInput('github-token', { required: true });
        const jobName = core.getInput('name', { required: true });

        // Store token and job name in state for cleanup phase
        core.saveState('github-token', token);
        core.saveState('job-name', jobName);
        core.info(`Starting release with version bump: ${versionBump}`);
        core.info(`Dry run mode: ${dryRun}`);

        // Get next release version and tag
        const { nextVersion, nextTag } = await getNextReleaseTag(project, versionBump);

        // Set outputs
        core.setOutput('next-release-version', nextVersion);
        core.setOutput('next-release-tag', nextTag);

        // Save state for post-job cleanup
        core.saveState('next-release-tag', nextTag);
        core.saveState('dry-run', dryRun.toString());

        // Configure git
        await exec.exec('git', ['config', 'user.name', process.env.GITHUB_ACTOR]);
        await exec.exec('git', ['config', 'user.email', `${process.env.GITHUB_ACTOR}@users.noreply.github.com`]);

        // Create tag
        await exec.exec('git', ['tag', '-a', nextTag, '-m', `Automatic release ${nextVersion}`]);

        core.info('Release setup completed successfully');
    } catch (error) {
        core.setFailed(error.message);
    }
}

/**
 * Cleanup function to push or delete tags based on job status
 * @param {object} dependencies - Dependency injection object
 * @param {object} dependencies.core - GitHub Actions core module
 * @param {object} dependencies.exec - GitHub Actions exec module
 * @param {object} dependencies.github - GitHub Actions github module
 * @param {object} dependencies.env - Environment variables object (defaults to process.env)
 * @param {number} dependencies.maxWaitTime - Maximum time to wait for steps to complete in milliseconds (defaults to 60000)
 * @param {number} dependencies.baseDelayMs - Base delay for exponential backoff in milliseconds (defaults to 1000)
 * @param {number} dependencies.maxDelayMs - Maximum delay cap for backoff in milliseconds (defaults to 5000)
 * @returns {Promise<void>}
 */
export async function cleanup(dependencies = {}) {
    const {
        core: coreModule = core,
        exec: execModule = exec,
        github: githubModule = github,
        env = process.env,
        maxWaitTime = 60000,
        baseDelayMs = 1000,
        maxDelayMs = 5000,
    } = dependencies;

    try {
        const nextTag = coreModule.getState('next-release-tag');
        const dryRun = coreModule.getState('dry-run') === 'true';

        if (!nextTag) {
            throw new Error('No next-release-tag found in state for cleanup');
        }

        coreModule.info('Running post-job cleanup...');

        let shouldPushTag = false;

        // Always check job status
        const token = coreModule.getState('github-token');
        const jobName = coreModule.getState('job-name');
        const runId = env.GITHUB_RUN_ID;
        const repository = env.GITHUB_REPOSITORY;

        if (!token || !runId || !repository || !jobName) {
            throw new Error('Missing required environment variables or state (GITHUB_TOKEN, GITHUB_RUN_ID, GITHUB_REPOSITORY, or job-name)');
        }

        const [owner, repo] = repository.split('/');
        const octokit = githubModule.getOctokit(token);

        coreModule.info(`Checking job status for run ${runId}...`);

        // Poll until the first Post* step is in_progress and all steps before it are completed
        const startTime = Date.now();
        let attempt = 0;
        let currentJob = null;
        let postStepReady = false;

        while (!postStepReady && (Date.now() - startTime) < maxWaitTime) {
            attempt++;

            // Get jobs for this workflow run
            const { data: { jobs } } = await octokit.rest.actions.listJobsForWorkflowRun({
                owner,
                repo,
                run_id: parseInt(runId, 10),
            });

            if (attempt === 1) {
                // Log available jobs for debugging on first attempt
                coreModule.info(`Found ${jobs.length} jobs in workflow run`);
                jobs.forEach(job => {
                    coreModule.info(`  Job: name="${job.name}", id=${job.id}, status=${job.status}, conclusion=${job.conclusion || 'none'}`);
                });
                coreModule.info(`Looking for job with name: ${jobName}`);
            }

            // Find the current job by name
            currentJob = jobs.find(job => job.name === jobName);

            if (!currentJob) {
                throw new Error(`Could not find current job with name '${jobName}' in workflow run`);
            }

            const steps = currentJob.steps || [];

            // Find our cleanup step: first step starting with "Post" that is currently in_progress
            // All steps before it must be completed before we can validate their conclusions
            const postStepIndex = steps.findIndex(step => step.name.startsWith('Post') && step.status === 'in_progress');

            // Early exit conditions:
            // 1. No steps at all - can't find Post step
            // 2. Post step is first (index 0) - no steps to validate before it
            // 3. Job is completed with non-success conclusion and no Post step - won't appear
            if (steps.length === 0 || postStepIndex === 0) {
                break; // Exit polling loop immediately
            }

            if (postStepIndex > 0) {
                const stepsBeforePost = steps.slice(0, postStepIndex);
                postStepReady = stepsBeforePost.every(step => step.status === 'completed');
            }

            if (!postStepReady) {
                coreModule.info(`Attempt ${attempt}: Cleanup step (Post*) not ready or previous steps still running, waiting...`);

                // Calculate exponential backoff with jitter: base * 2^(attempt-1) + random jitter up to 500ms
                const baseDelay = Math.min(baseDelayMs * Math.pow(2, attempt - 1), maxDelayMs);
                const jitter = Math.random() * 500;
                const delay = baseDelay + jitter;

                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                // Log all steps on successful final attempt
                coreModule.info(`Attempt ${attempt}: Found ${steps.length} steps:`);
                steps.forEach((step, index) => {
                    coreModule.info(`  Step ${index + 1}: name="${step.name}", status=${step.status}, conclusion=${step.conclusion || 'none'}`);
                });
            }
        }

        if (!postStepReady) {
            throw new Error(`Timeout after ${(maxWaitTime / 1000).toFixed(2)}s: Cleanup step (Post*) not found with in_progress status or previous steps not completed`);
        }

        coreModule.info(`Job status: ${currentJob.status}, conclusion: ${currentJob.conclusion || 'none'}`);

        // Check if all steps before first Post step succeeded
        const steps = currentJob.steps || [];

        // Find the cleanup step (guaranteed to exist since we waited for it in the polling loop)
        const postStepIndex = steps.findIndex(step => step.name.startsWith('Post') && step.status === 'in_progress');

        if (postStepIndex === -1) {
            throw new Error('Cleanup step (Post*) unexpectedly not found - this should not happen');
        }

        // Get all steps before first Post step
        const stepsToCheck = steps.slice(0, postStepIndex);
        const allStepsSucceeded = stepsToCheck.every(step => step.conclusion === 'success');

        if (allStepsSucceeded && stepsToCheck.length > 0) {
            // All steps before first Post step succeeded
            shouldPushTag = !dryRun;
        } else {
            // Some steps failed, were cancelled, skipped, or no steps to validate
            if (stepsToCheck.length === 0) {
                coreModule.error(`No steps found before cleanup step - cannot validate`);
            } else {
                const failedSteps = stepsToCheck.filter(step => step.conclusion !== 'success');
                coreModule.error(`Not all steps succeeded (${failedSteps.length}/${stepsToCheck.length} failed)`);
            }
            shouldPushTag = false;
        }

        if (shouldPushTag) {
            coreModule.info(`Pushing tag ${nextTag} to remote`);
            coreModule.info(`[command]git push origin ${nextTag}`);
            await execModule.exec('git', ['push', 'origin', nextTag], { silent: false });
        } else {
            const reason = dryRun ? 'dry-run mode' : 'job failed or was cancelled';
            coreModule.info(`Deleting tag ${nextTag} (${reason})`);
            coreModule.info(`[command]git tag -d ${nextTag}`);
            await execModule.exec('git', ['tag', '-d', nextTag], {
                silent: false,
                ignoreReturnCode: true
            });
        }
    } catch (error) {
        coreModule.setFailed(`Cleanup failed: ${error.message}`);
    }
}

// Only run CLI or GitHub Actions code if this is the main module being executed
// When imported as a module, none of this code should run
const isMainModule = process.argv[1] && resolve(fileURLToPath(import.meta.url)) === resolve(process.argv[1]);

if (isMainModule) {
    // CLI mode for testing
    // Check if we're NOT running in GitHub Actions (GITHUB_ACTIONS is always set to 'true' in GitHub Actions)
    // See: https://docs.github.com/en/actions/reference/workflows-and-actions/variables
    const isCliMode = !process.env.GITHUB_ACTIONS;

    if (isCliMode) {
        const args = process.argv.slice(2);

        if (args.length < 2 || args[0] === '--help' || args[0] === '-h') {
            console.log('Usage: node index.js <project-path> <bump-type>');
            console.log('');
            console.log('Arguments:');
            console.log('  project-path   Path to the project directory');
            console.log('  bump-type      Version bump type (major, minor, patch)');
            console.log('');
            console.log('Example:');
            console.log('  node index.js ./framework patch');
            process.exit(args.length < 2 ? 1 : 0);
        }

        const [projectPath, bumpType] = args;

        // Create a simple logger for CLI mode
        const cliLogger = {
            info: (msg) => console.log(`[INFO] ${msg}`),
            warning: (msg) => console.warn(`[WARN] ${msg}`),
            error: (msg) => console.error(`[ERROR] ${msg}`)
        };

        // Call getNextReleaseTag with CLI logger
        getNextReleaseTag(projectPath, bumpType, cliLogger)
            .then(({ nextVersion, nextTag }) => {
                console.log('');
                console.log('Results:');
                console.log(`  Next Version: ${nextVersion}`);
                console.log(`  Next Tag    : ${nextTag}`);
                process.exit(0);
            })
            .catch((error) => {
                console.error('');
                console.error(`Error: ${error.message}`);
                process.exit(1);
            });
    } else {
        // GitHub Actions mode
        // Check if we're in post-job cleanup phase
        if (core.getState('isPost') === 'true') {
            cleanup();
        } else {
            core.saveState('isPost', 'true');
            run();
        }
    }
}
