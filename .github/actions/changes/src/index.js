import * as core from '@actions/core';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import yaml from 'js-yaml';
import toml from '@iarna/toml';

/**
 * @typedef {Object} ChangeE2eTests
 * @property {string} local - Path to local e2e tests (run mode: local)
 * @property {string} dist - Path to distributed e2e tests (run mode: distributed)
 */

/**
 * @typedef {Object} ChangeTests
 * @property {string} unit - Path to unit tests directory
 * @property {ChangeE2eTests} e2e - Configuration for end-to-end tests
 */

/**
 * @typedef {Object} Change
 * @property {string} directory - Relative path to package directory
 * @property {string} package - Package name (e.g., 'grizzly-loadtester')
 * @property {ChangeTests} tests - Test configuration for this package
 */

/**
 * Create a Change object for a Python package by detecting its test structure
 * @param {string} directory - Absolute path to the package directory
 * @param {string} relativeDirectory - Relative path to the directory from workspace root
 * @param {string} packageName - Name of the Python package
 * @returns {Change} Change object with populated test configuration
 */
function createPythonChange(directory, relativeDirectory, packageName) {
    const directoryPath = path.resolve(directory);
    const testsPath = path.join(directoryPath, 'tests');

    // Find first test_* directory
    let testDirectory = null;
    if (fs.existsSync(testsPath)) {
        const testDirs = fs.readdirSync(testsPath)
            .filter(name => name.startsWith('test_'))
            .map(name => path.join(testsPath, name))
            .filter(p => fs.statSync(p).isDirectory());

        if (testDirs.length > 0) {
            testDirectory = testDirs[0];
        }
    }

    // No tests directory found
    if (!testDirectory) {
        return {
            directory: relativeDirectory,
            package: packageName,
            tests: {
                unit: '',
                e2e: { local: '', dist: '' }
            }
        };
    }

    const testUnitDirectory = path.join(testDirectory, 'unit');
    const testE2eDirectory = path.join(testDirectory, 'e2e');

    let argsUnit = '';
    let argsE2e = '';
    let argsE2eDist = '';

    if (fs.existsSync(testUnitDirectory) && fs.existsSync(testE2eDirectory)) {
        argsUnit = path.relative(directoryPath, testUnitDirectory).replace(/\\/g, '/');
        argsE2e = path.relative(directoryPath, testE2eDirectory).replace(/\\/g, '/');

        if (packageName === 'grizzly-loadtester') {
            argsE2eDist = argsE2e;
        }
    } else {
        argsUnit = path.relative(directoryPath, testDirectory).replace(/\\/g, '/');
    }

    return {
        directory: relativeDirectory,
        package: packageName,
        tests: {
            unit: argsUnit,
            e2e: { local: argsE2e, dist: argsE2eDist }
        }
    };
}

/**
 * Detect changes in a Python package and its reverse dependencies
 * @param {string} directory - Absolute path to the directory to analyze
 * @param {string} relativeDirectory - Relative path to the directory from workspace root
 * @param {Array} uvLockPackages - List of packages from uv.lock file
 * @param {string} workspaceRoot - Root directory of the workspace
 * @param {boolean} release - If true, only include packages with release configuration
 * @returns {Set<Change>} Set of Change objects for the package and its reverse dependencies
 */
function pythonPackage(directory, relativeDirectory, uvLockPackages, workspaceRoot, release = false) {
    const changes = new Set();
    const pyprojectFile = path.join(directory, 'pyproject.toml');

    if (!fs.existsSync(pyprojectFile)) {
        return changes;
    }

    const pyprojectContent = fs.readFileSync(pyprojectFile, 'utf8');
    const pyproject = toml.parse(pyprojectContent);
    const project = pyproject.project || {};

    // Check if package has release configuration
    if (release) {
        const hasReleaseConfig = pyproject.tool?.hatch?.version?.['raw-options']?.scm?.git?.describe_command;
        if (!hasReleaseConfig) {
            return changes;
        }
    }

    const packageName = project.name;
    if (!packageName) {
        return changes;
    }

    changes.add(createPythonChange(directory, relativeDirectory, packageName));

    // Find workspace packages that depend on this package (reverse dependencies)
    for (const pkg of uvLockPackages) {
        const pkgName = pkg.name || '';
        const dependencies = pkg.dependencies || [];

        if (pkgName.startsWith('grizzly-') && dependencies.some(dep => dep.name === packageName)) {
            const reversePackage = pkgName;
            const reverseRelativeDirectory = pkg.source?.editable;

            if (reverseRelativeDirectory) {
                const reverseFullPath = path.join(workspaceRoot, reverseRelativeDirectory);
                changes.add(createPythonChange(reverseFullPath, reverseRelativeDirectory, reversePackage));
            }
        }
    }

    return changes;
}

/**
 * Detect changes in a Node.js/npm package
 * @param {string} directory - Absolute path to the directory to analyze
 * @param {string} relativeDirectory - Relative path to the directory from workspace root
 * @param {boolean} release - If true, only include packages with release configuration
 * @returns {Set<Change>} Set containing a Change object if the package exists and meets criteria
 */
function nodePackage(directory, relativeDirectory, release = false) {
    const changes = new Set();
    const packageJsonFile = path.join(directory, 'package.json');

    if (!fs.existsSync(packageJsonFile)) {
        return changes;
    }

    // Check for release configuration
    if (release) {
        const packageLocalJsonFile = path.join(directory, 'package.local.json');
        if (!fs.existsSync(packageLocalJsonFile)) {
            return changes;
        }

        const packageLocalJson = JSON.parse(fs.readFileSync(packageLocalJsonFile, 'utf8'));
        const hasReleaseConfig = packageLocalJson.tag?.pattern;
        if (!hasReleaseConfig) {
            return changes;
        }
    }

    const packageJson = JSON.parse(fs.readFileSync(packageJsonFile, 'utf8'));
    const packageScripts = packageJson.scripts || {};

    const argsUnit = 'test' in packageScripts ? 'test' : '';
    const argsE2e = 'test:e2e' in packageScripts ? 'test:e2e' : '';

    changes.add({
        directory: relativeDirectory,
        package: packageJson.name,
        tests: {
            unit: argsUnit,
            e2e: { local: argsE2e, dist: '' }
        }
    });

    return changes;
}

/**
 * Map changed directories to package changes and their test configurations
 * @param {Object} options - Configuration options
 * @param {string} options.changes - JSON string of list of directories that had changes
 * @param {boolean} options.force - Force run on all packages
 * @param {boolean} options.release - Indicates if this is a release run
 * @param {boolean} options.manual - Indicates if this is a manual release (workflow_dispatch)
 * @param {string} options.workspaceRoot - Root directory of the workspace
 * @param {Object} options.logger - Logger object (defaults to console)
 * @returns {Promise<Object>} Object with changes_uv, changes_npm, and changes_actions arrays
 */
export async function mapChanges(options = {}) {
    const {
        changes,
        force = false,
        release = false,
        manual = false,
        workspaceRoot = process.cwd(),
        logger = console
    } = options;

    let workflowInput;

    try {
        workflowInput = JSON.parse(changes);
    } catch {
        throw new Error(`Invalid JSON in changes: "${changes}"`);
    }

    if (!release && (force || (workflowInput !== undefined && workflowInput.includes('uv')))) {
        // Load all packages from changes-filter.yaml
        const changeFiltersFile = path.join(workspaceRoot, '.github', 'changes-filter.yaml');
        const changeFiltersContent = fs.readFileSync(changeFiltersFile, 'utf8');
        const changeFilters = yaml.load(changeFiltersContent);
        workflowInput = Object.keys(changeFilters);
    }

    // Fail if workflow files were modified during release
    if (release && workflowInput && workflowInput.some(dir => dir.includes('workflows'))) {
        if (manual) {
            // Manual release: throw error
            throw new Error('Workflow files cannot be part of a release');
        } else {
            // Automatic release: warn and skip
            const warningMsg = 'Workflow files cannot be part of a release - skipping change detection';
            const warnMethod = logger.warning || logger.warn;
            if (warnMethod) {
                warnMethod.call(logger, warningMsg);
            }
            return {
                changes_uv: [],
                changes_npm: []
            };
        }
    }

    const changesMap = {
        uv: new Map(),
        npm: new Map()
    };

    // Load uv.lock file
    const uvLockFile = path.join(workspaceRoot, 'uv.lock');
    const uvLockContent = fs.readFileSync(uvLockFile, 'utf8');
    const uvLock = toml.parse(uvLockContent);
    const uvLockPackages = uvLock.package || [];

    // Process each directory
    for (const directory of workflowInput) {
        const fullPath = path.join(workspaceRoot, directory);

        // Python packages
        const pythonChanges = pythonPackage(fullPath, directory, uvLockPackages, workspaceRoot, release);
        for (const change of pythonChanges) {
            changesMap.uv.set(change.directory, change);
        }

        // Node packages
        const npmChanges = nodePackage(fullPath, directory, release);
        for (const change of npmChanges) {
            changesMap.npm.set(change.directory, change);
        }
    }

    // Convert maps to sorted arrays
    const changesUv = Array.from(changesMap.uv.values()).sort((a, b) => a.package.localeCompare(b.package));
    const changesNpm = Array.from(changesMap.npm.values()).sort((a, b) => a.package.localeCompare(b.package));

    const result = {
        changes_uv: changesUv,
        changes_npm: changesNpm
    };

    const logMessage = `Detected changes:\nuv=${JSON.stringify(changesUv)}\nnpm=${JSON.stringify(changesNpm)}`;
    logger.info(logMessage);

    return result;
}

/**
 * Run the action in GitHub Actions mode
 * @param {Object} dependencies - Dependency injection object
 * @param {Object} dependencies.core - GitHub Actions core module
 * @param {Object} dependencies.env - Environment variables object (defaults to process.env)
 * @returns {Promise<void>}
 */
export async function run(dependencies = {}) {
    const {
        core: coreModule = core,
        env = process.env
    } = dependencies;

    try {
        const changesInput = coreModule.getInput('changes', { required: true });
        const forceInput = coreModule.getInput('force', { required: true });
        const releaseInput = coreModule.getInput('release') === 'true';
        const manualInput = coreModule.getInput('manual') === 'true';

        const force = forceInput === 'true';

        // Get workspace root (parent of .github directory)
        const workspaceRoot = env.GITHUB_WORKSPACE || process.cwd();

        coreModule.info('Mapping changes to packages...');

        const result = await mapChanges({
            changes: changesInput,
            force,
            release: releaseInput,
            manual: manualInput,
            workspaceRoot,
            logger: coreModule
        });

        // Set outputs as JSON strings
        coreModule.setOutput('changes_uv', JSON.stringify(result.changes_uv));
        coreModule.setOutput('changes_npm', JSON.stringify(result.changes_npm));

        coreModule.info('Changes mapped successfully');
    } catch (error) {
        coreModule.setFailed(error.message);
    }
}

// CLI mode support
const __filename = fileURLToPath(import.meta.url);
const isMainModule = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(__filename);

if (isMainModule && !process.env.GITHUB_ACTIONS) {
    // CLI mode
    const args = process.argv.slice(2);

    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        console.log('Usage: node index.js --changes <json> --force <true|false> [--release]');
        console.log('');
        console.log('Options:');
        console.log('  --changes <json>  JSON string of directories that changed');
        console.log('  --force <bool>    Force run on all packages (true/false)');
        console.log('  --release         Indicates if this is a release run');
        console.log('');
        console.log('Example:');
        console.log('  node index.js --changes \'["framework"]\' --force false');
        console.log('  node index.js --changes \'[]\' --force true --release');
        process.exit(args.length === 0 ? 1 : 0);
    }

    // Parse CLI arguments
    const changesIdx = args.indexOf('--changes');
    const forceIdx = args.indexOf('--force');
    const releaseIdx = args.indexOf('--release');

    if (changesIdx === -1 || forceIdx === -1) {
        console.error('Error: --changes and --force are required');
        process.exit(1);
    }

    const changesArg = args[changesIdx + 1];
    const forceArg = args[forceIdx + 1] === 'true';
    const releaseArg = releaseIdx !== -1;

    // Find workspace root (go up until we find .github directory)
    let workspaceRoot = process.cwd();
    while (!fs.existsSync(path.join(workspaceRoot, '.github')) && workspaceRoot !== '/') {
        workspaceRoot = path.dirname(workspaceRoot);
    }

    if (!fs.existsSync(path.join(workspaceRoot, '.github'))) {
        console.error('Error: Could not find workspace root (no .github directory found)');
        process.exit(1);
    }

    mapChanges({
        changes: changesArg,
        force: forceArg,
        release: releaseArg,
        workspaceRoot,
        logger: console
    })
        .then((result) => {
            console.log('');
            console.log('Results:');
            console.log(`  UV Changes  : ${result.changes_uv.length}`);
            console.log(`  NPM Changes : ${result.changes_npm.length}`);
            process.exit(0);
        })
        .catch((error) => {
            console.error('');
            console.error(`Error: ${error.message}`);
            process.exit(1);
        });
} else if (isMainModule && process.env.GITHUB_ACTIONS) {
    // GitHub Actions mode
    run();
}
