import * as core from '@actions/core';
import * as github from '@actions/github';
import { resolve } from 'path';
import { fileURLToPath } from 'url';

/**
 * Check pull request for version bump labels
 * @param {object} context - GitHub context
 * @param {object} octokit - GitHub API client
 * @param {string|null} prNumber - PR number from manual trigger (null for automatic)
 * @param {object} logger - Logger object with info, warning, error methods (defaults to core)
 * @returns {Promise<{shouldRelease: boolean, versionBump: string|null, prNumber: number, commitSha: string, baseCommitSha: string}>}
 */
export async function checkPullRequest(context, octokit, prNumber = null, logger = core) {
    let pr;
    let commitSha;

    if (prNumber !== null) {
        // Manual trigger: get PR by number
        logger.info(`manual trigger for PR #${prNumber}`);

        const prResponse = await octokit.rest.pulls.get({
            owner: context.repo.owner,
            repo: context.repo.repo,
            pull_number: prNumber
        });

        pr = prResponse.data;

        // Verify PR is merged
        if (!pr.merged) {
            throw new Error(`PR #${prNumber} is not merged`);
        }

        commitSha = pr.merge_commit_sha;
        logger.info(`PR #${prNumber} merged at commit: ${commitSha}`);
    } else {
        // Automatic trigger: find PR associated with the commit
        commitSha = context.sha;
        logger.info(`finding PR associated with commit: ${commitSha}`);

        const { data: prs } = await octokit.rest.repos.listPullRequestsAssociatedWithCommit({
            owner: context.repo.owner,
            repo: context.repo.repo,
            commit_sha: commitSha
        });

        if (!prs || prs.length === 0) {
            logger.info(`no PR found associated with commit ${commitSha}, skipping`);
            return {
                shouldRelease: false,
                versionBump: null,
                prNumber: null,
                commitSha,
                baseCommitSha: null
            };
        }

        // Get the first (most recent) PR
        pr = prs[0];
        logger.info(`found PR #${pr.number} associated with commit`);

        // Verify PR is merged
        if (!pr.merged_at) {
            logger.info(`PR #${pr.number} is not merged, skipping`);
            return {
                shouldRelease: false,
                versionBump: null,
                prNumber: pr.number,
                commitSha,
                baseCommitSha: pr.base.sha
            };
        }
    }

    const labels = pr.labels.map(label => label.name);
    logger.info(`PR #${pr.number} labels: ${labels.join(', ')}`);

    // Get the base commit (the commit that the PR was merged onto)
    const baseCommitSha = pr.base.sha;
    logger.info(`PR base commit: ${baseCommitSha}`);

    // Check for version bump labels
    const versionLabels = ['major', 'minor', 'patch'];
    const versionLabel = versionLabels.find(label => labels.includes(label));

    if (versionLabel) {
        logger.info(`found version label: ${versionLabel}`);
        return {
            shouldRelease: true,
            versionBump: versionLabel,
            prNumber: pr.number,
            commitSha,
            baseCommitSha
        };
    } else {
        if (prNumber !== null) {
            // Manual trigger: fail if no version label
            logger.info('no version release label (major/minor/patch) found');
            throw new Error(`no version release label found on PR #${pr.number}`);
        } else {
            // Automatic trigger: skip if no version label
            logger.info('no version release label (major/minor/patch) found, skipping');
            return {
                shouldRelease: false,
                versionBump: null,
                prNumber: pr.number,
                commitSha,
                baseCommitSha
            };
        }
    }
}

/**
 * Run the action in GitHub Actions mode
 * @param {object} dependencies - Dependency injection object
 * @param {object} dependencies.core - GitHub Actions core module
 * @param {object} dependencies.github - GitHub Actions github module
 * @param {object} dependencies.env - Environment variables object (defaults to process.env)
 * @returns {Promise<void>}
 */
export async function run(dependencies = {}) {
    const {
        core: coreModule = core,
        github: githubModule = github,
    } = dependencies;

    try {
        const prNumberInput = coreModule.getInput('pr-number');
        const token = coreModule.getInput('github-token', { required: true });

        const octokit = githubModule.getOctokit(token);
        const context = githubModule.context;

        // Determine if this is a manual or automatic trigger
        const prNumber = prNumberInput ? parseInt(prNumberInput, 10) : null;

        coreModule.info('Checking pull request for version bump labels...');

        const result = await checkPullRequest(context, octokit, prNumber, coreModule);

        // Set outputs
        coreModule.setOutput('should-release', result.shouldRelease.toString());
        coreModule.setOutput('version-bump', result.versionBump || '');
        coreModule.setOutput('pr-number', result.prNumber ? result.prNumber.toString() : '');
        coreModule.setOutput('commit-sha', result.commitSha);
        coreModule.setOutput('base-commit-sha', result.baseCommitSha || '');

        coreModule.info('Pull request check completed successfully');
    } catch (error) {
        coreModule.setOutput('should-release', 'false');
        coreModule.setFailed(error.message);
    }
}

// Only run if this is the main module being executed
const isMainModule = process.argv[1] && resolve(fileURLToPath(import.meta.url)) === resolve(process.argv[1]);

if (isMainModule) {
    // Check if we're NOT running in GitHub Actions
    const isCliMode = !process.env.GITHUB_ACTIONS;

    if (isCliMode) {
        const args = process.argv.slice(2);

        if (args.length < 1 || args[0] === '--help' || args[0] === '-h') {
            console.log('Usage: node index.js <pr-number>');
            console.log('');
            console.log('Arguments:');
            console.log('  pr-number   Pull request number to check');
            console.log('');
            console.log('Example:');
            console.log('  node index.js 123');
            process.exit(args.length < 1 ? 1 : 0);
        }

        const [prNumberArg] = args;
        const prNumber = parseInt(prNumberArg, 10);

        if (isNaN(prNumber)) {
            console.error(`Error: Invalid PR number: ${prNumberArg}`);
            process.exit(1);
        }

        // Create a simple logger for CLI mode
        const cliLogger = {
            info: (msg) => console.log(`[INFO] ${msg}`),
            warning: (msg) => console.warn(`[WARN] ${msg}`),
            error: (msg) => console.error(`[ERROR] ${msg}`)
        };

        // For CLI mode, we need a GitHub token
        const token = process.env.GITHUB_TOKEN;
        if (!token) {
            console.error('Error: GITHUB_TOKEN environment variable is required');
            process.exit(1);
        }

        const octokit = github.getOctokit(token);
        const context = github.context;

        // Call checkPullRequest with CLI logger
        checkPullRequest(context, octokit, prNumber, cliLogger)
            .then((result) => {
                console.log('');
                console.log('Results:');
                console.log(`  Should Release  : ${result.shouldRelease}`);
                console.log(`  Version Bump    : ${result.versionBump}`);
                console.log(`  PR Number       : ${result.prNumber}`);
                console.log(`  Commit SHA      : ${result.commitSha}`);
                console.log(`  Base Commit SHA : ${result.baseCommitSha}`);
                process.exit(0);
            })
            .catch((error) => {
                console.error('');
                console.error(`Error: ${error.message}`);
                process.exit(1);
            });
    } else {
        // GitHub Actions mode
        run();
    }
}
