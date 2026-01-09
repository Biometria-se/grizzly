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

    // Set longer timeout for CLI tests since they spawn processes
    this.timeout(5000);

    describe('help output', () => {
        it('should display help when --help is passed', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            // --help exits with code 0, so it resolves successfully
            const { stdout } = await execFileAsync('node', [indexPath, '--help'], { env: cleanEnv });
            expect(stdout).to.include('Usage:');
            expect(stdout).to.include('--changes');
            expect(stdout).to.include('--force');
        });

        it('should display help when -h is passed', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            // -h exits with code 0, so it resolves successfully
            const { stdout } = await execFileAsync('node', [indexPath, '-h'], { env: cleanEnv });
            expect(stdout).to.include('Usage:');
        });

        it('should display help when no arguments are provided', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            try {
                await execFileAsync('node', [indexPath], { env: cleanEnv, cwd: workspaceRoot });
                expect.fail('Should have exited with code 1');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Usage:');
            }
        });
    });

    describe('argument validation', () => {
        it('should exit with error when --changes is missing', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            try {
                await execFileAsync('node', [indexPath, '--force', 'false'], { env: cleanEnv, cwd: workspaceRoot });
                expect.fail('Should have exited with error code');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stderr).to.include('--changes and --force are required');
            }
        });

        it('should exit with error when --force is missing', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            try {
                await execFileAsync('node', [indexPath, '--changes', '[]'], { env: cleanEnv, cwd: workspaceRoot });
                expect.fail('Should have exited with error code');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stderr).to.include('--changes and --force are required');
            }
        });
    });

    describe('changes processing', () => {
        it('should process empty changes with force mode', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            const { stdout } = await execFileAsync('node',
                [indexPath, '--changes', '[]', '--force', 'true'],
                { env: cleanEnv, cwd: workspaceRoot }
            );

            expect(stdout).to.include('Results:');
            expect(stdout).to.include('UV Changes');
            expect(stdout).to.include('NPM Changes');
        });

        it('should process specific changes', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            const { stdout } = await execFileAsync('node',
                [indexPath, '--changes', '["framework"]', '--force', 'false'],
                { env: cleanEnv, cwd: workspaceRoot }
            );

            expect(stdout).to.include('Results:');
            expect(stdout).to.include('UV Changes');
        });

        it('should handle release flag', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            const { stdout } = await execFileAsync('node',
                [indexPath, '--changes', '["framework"]', '--force', 'false', '--release'],
                { env: cleanEnv, cwd: workspaceRoot }
            );

            expect(stdout).to.include('Results:');
        });
    });

    describe('github actions detection', () => {
        it('should not run in CLI mode when GITHUB_ACTIONS is set', async () => {
            try {
                await execFileAsync('node', [indexPath, '--changes', '[]', '--force', 'false'], {
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
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            const { stdout } = await execFileAsync('node',
                [indexPath, '--changes', '[]', '--force', 'true'],
                { env: cleanEnv, cwd: workspaceRoot }
            );

            expect(stdout).to.include('Results:');
            expect(stdout).to.include('UV Changes');
        });
    });
});
