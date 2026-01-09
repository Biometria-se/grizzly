import { expect } from 'chai';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { fileURLToPath } from 'url';
import { dirname, resolve } from 'path';

const execFileAsync = promisify(execFile);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('CLI mode', function () {
    const indexPath = resolve(__dirname, '../src/index.js');
    const workspaceRoot = resolve(__dirname, '../../../..');
    const projectPath = 'framework';

    // Set longer timeout for CLI tests since they spawn processes
    this.timeout(5000);

    describe('help output', () => {
        it('should display help when --help is passed', async () => {
            try {
                await execFileAsync('node', [indexPath, '--help']);
                expect.fail('Should have exited with code 1');
            } catch (error) {
                // Exit code 1 for help
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Usage:');
                expect(error.stdout).to.include('project-path');
                expect(error.stdout).to.include('bump-type');
            }
        });

        it('should display help when -h is passed', async () => {
            try {
                await execFileAsync('node', [indexPath, '-h']);
                expect.fail('Should have exited with code 1');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Usage:');
            }
        });

        it('should display help when no arguments are provided', async () => {
            try {
                await execFileAsync('node', [indexPath], { cwd: workspaceRoot });
                expect.fail('Should have exited with code 1');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Usage:');
            }
        });

        it('should display help when only one argument is provided', async () => {
            try {
                await execFileAsync('node', [indexPath, projectPath], { cwd: workspaceRoot });
                expect.fail('Should have exited with code 1');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Usage:');
            }
        });
    });

    describe('version calculation', () => {
        it('should calculate and display patch version', async () => {
            const { stdout } = await execFileAsync('node', [indexPath, projectPath, 'patch'], { cwd: workspaceRoot });

            expect(stdout).to.include('Results:');
            expect(stdout).to.include('Next Version:');
            expect(stdout).to.include('Next Tag');
            expect(stdout).to.match(/Next Version:\s+\d+\.\d+\.\d+/);
            expect(stdout).to.match(/Next Tag\s+:\s+framework@v\d+\.\d+\.\d+/);
        });

        it('should calculate minor version with patch reset to 0', async () => {
            const { stdout } = await execFileAsync('node', [indexPath, projectPath, 'minor'], { cwd: workspaceRoot });

            expect(stdout).to.include('Next Version:');
            expect(stdout).to.match(/Next Version:\s+\d+\.\d+\.0/);
        });

        it('should calculate major version with minor and patch reset to 0', async () => {
            const { stdout } = await execFileAsync('node', [indexPath, projectPath, 'major'], { cwd: workspaceRoot });

            expect(stdout).to.include('Next Version:');
            expect(stdout).to.match(/Next Version:\s+\d+\.0\.0/);
        });

        it('should exit with error code for invalid project path', async () => {
            try {
                await execFileAsync('node', [indexPath, '/non/existent/path', 'patch']);
                expect.fail('Should have exited with error code');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stderr).to.include('Error:');
            }
        });
    });

    describe('github actions detection', () => {
        it('should not run in CLI mode when GITHUB_ACTIONS is set', async () => {
            try {
                await execFileAsync('node', [indexPath, projectPath, 'patch'], {
                    env: { ...process.env, GITHUB_ACTIONS: 'true' },
                    cwd: workspaceRoot
                });
                expect.fail('Should have exited with error code');
            } catch (error) {
                // When GITHUB_ACTIONS is set, it tries to run as GitHub Action
                // and fails because required inputs are not set
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Input required and not supplied');
            }
        });

        it('should run in CLI mode when GITHUB_ACTIONS is not set', async () => {
            // Remove GITHUB_ACTIONS from environment
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            const { stdout } = await execFileAsync('node', [indexPath, projectPath, 'patch'], {
                env: cleanEnv,
                cwd: workspaceRoot
            });

            expect(stdout).to.include('Results:');
            expect(stdout).to.include('Next Version:');
        });
    });

    describe('output format', () => {
        it('should include info messages in output', async () => {
            const { stdout } = await execFileAsync('node', [indexPath, projectPath, 'patch'], { cwd: workspaceRoot });

            // Should have structured output with [INFO] messages and Results section
            expect(stdout).to.include('[INFO]');
            expect(stdout).to.include('Results:');
            expect(stdout).to.include('Next Version:');
            expect(stdout).to.include('Next Tag');
            expect(stdout).to.include('Previous tag');
            expect(stdout).to.include('Previous version');
        });

        it('should show git commands in output', async () => {
            const { stdout } = await execFileAsync('node', [indexPath, projectPath, 'patch'], { cwd: workspaceRoot });

            // Git commands are logged to stdout by @actions/exec in CLI mode
            expect(stdout).to.include('[command]');
            expect(stdout).to.include('git tag');
        });
    });
});
