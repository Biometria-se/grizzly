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

    // Set longer timeout for CLI tests since they spawn processes
    this.timeout(5000);

    describe('help output', () => {
        it('should display help when --help is passed', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            // --help exits with code 0, so it resolves successfully
            const { stdout } = await execFileAsync('node', [indexPath, '--help'], { env: cleanEnv });
            expect(stdout).to.include('Usage:');
            expect(stdout).to.include('pr-number');
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
                await execFileAsync('node', [indexPath], { env: cleanEnv });
                expect.fail('Should have exited with code 1');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Usage:');
            }
        });
    });

    describe('error handling', () => {
        it('should exit with error for invalid PR number', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;

            try {
                await execFileAsync('node', [indexPath, 'invalid'], { env: cleanEnv });
                expect.fail('Should have exited with error code');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stderr).to.include('Invalid PR number');
            }
        });

        it('should exit with error when GITHUB_TOKEN is missing', async () => {
            const cleanEnv = { ...process.env };
            delete cleanEnv.GITHUB_ACTIONS;
            delete cleanEnv.GITHUB_TOKEN;

            try {
                await execFileAsync('node', [indexPath, '123'], { env: cleanEnv });
                expect.fail('Should have exited with error code');
            } catch (error) {
                expect(error.code).to.equal(1);
                expect(error.stderr).to.include('GITHUB_TOKEN environment variable is required');
            }
        });
    });

    describe('github actions detection', () => {
        it('should not run in CLI mode when GITHUB_ACTIONS is set', async () => {
            try {
                await execFileAsync('node', [indexPath, '123'], {
                    env: { ...process.env, GITHUB_ACTIONS: 'true' }
                });
                expect.fail('Should have exited with error code');
            } catch (error) {
                // When GITHUB_ACTIONS is set, it tries to run as GitHub Action
                // and fails because required inputs are not set
                expect(error.code).to.equal(1);
                expect(error.stdout).to.include('Input required and not supplied');
            }
        });
    });
});
