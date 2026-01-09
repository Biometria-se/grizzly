import { expect } from 'chai';
import sinon from 'sinon';
import { setupEnvironment, run } from '../src/index.js';
import fs from 'fs';
import os from 'os';

describe('setupEnvironment', () => {
    // Create stub logger to suppress output during tests
    let mockLogger;

    let appendFileSyncStub;
    let mockEnv;
    let originalPlatform;
    let tmpdirStub;

    beforeEach(() => {
        // Create fresh logger for each test
        mockLogger = {
            info: sinon.stub(),
            warning: sinon.stub(),
            error: sinon.stub(),
            debug: sinon.stub()
        };

        // Stub fs.appendFileSync
        appendFileSyncStub = sinon.stub(fs, 'appendFileSync');
        tmpdirStub = sinon.stub(os, 'tmpdir');

        // Save original platform
        originalPlatform = process.platform;

        // Create mock environment
        mockEnv = {
            GITHUB_WORKSPACE: '/workspace',
            GITHUB_PATH: '/tmp/github_path',
            GITHUB_ENV: '/tmp/github_env'
        };
    });

    afterEach(() => {
        sinon.restore();
        // Restore platform
        Object.defineProperty(process, 'platform', {
            value: originalPlatform
        });
    });

    describe('with default parameters', () => {
        it('should setup default environment variables and paths on Linux', async () => {
            // Mock process.platform
            Object.defineProperty(process, 'platform', {
                value: 'linux'
            });
            tmpdirStub.returns('/tmp');

            const result = await setupEnvironment({
                envVars: null,
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            // Verify paths were added
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/workspace/.venv/bin\n');

            // Verify environment variables were added
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'VIRTUAL_ENV=/workspace/.venv\n');
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'GRIZZLY_TMP_DIR=/tmp\n');
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'GRIZZLY_TMP_LOGFILE=/tmp/grizzly.log\n');

            expect(result.paths).to.deep.equal(['/workspace/.venv/bin']);
            expect(result.envVars).to.have.lengthOf(3);
            expect(result.envVars[0]).to.equal('VIRTUAL_ENV=/workspace/.venv');
            expect(result.envVars[1]).to.equal('GRIZZLY_TMP_DIR=/tmp');
            expect(result.envVars[2]).to.equal('GRIZZLY_TMP_LOGFILE=/tmp/grizzly.log');
        });

        it('should setup default environment variables and paths on Windows', async () => {
            // Mock process.platform
            Object.defineProperty(process, 'platform', {
                value: 'win32'
            });
            tmpdirStub.returns('C:\\Temp');

            const result = await setupEnvironment({
                envVars: null,
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            // Verify paths were added (Windows uses Scripts instead of bin)
            // Note: path.join will still use forward slashes on Linux, even with platform set to win32
            const expectedVenvPath = '/workspace/.venv/Scripts';
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', `${expectedVenvPath}\n`);

            // Verify environment variables were added
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'VIRTUAL_ENV=/workspace/.venv\n');
            // path.join normalizes the path separator based on the actual OS, not the mocked platform
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', sinon.match(/GRIZZLY_TMP_DIR=C:\\Temp/));
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', sinon.match(/GRIZZLY_TMP_LOGFILE=C:\\Temp/));

            expect(result.paths).to.deep.equal([expectedVenvPath]);
        });
    });

    describe('with custom parameters', () => {
        it('should add custom environment variables', async () => {
            const result = await setupEnvironment({
                envVars: ['FOO=bar', 'BAZ=qux'],
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'FOO=bar\n');
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'BAZ=qux\n');

            expect(result.envVars).to.deep.equal(['FOO=bar', 'BAZ=qux']);
            expect(result.paths).to.deep.equal([]);
        });

        it('should add custom paths', async () => {
            const result = await setupEnvironment({
                envVars: null,
                paths: ['/custom/path1', '/custom/path2'],
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/custom/path1\n');
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/custom/path2\n');

            expect(result.paths).to.deep.equal(['/custom/path1', '/custom/path2']);
            expect(result.envVars).to.deep.equal([]);
        });

        it('should add both custom environment variables and paths', async () => {
            const result = await setupEnvironment({
                envVars: ['TEST=value'],
                paths: ['/test/path'],
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/test/path\n');
            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'TEST=value\n');

            expect(result.paths).to.deep.equal(['/test/path']);
            expect(result.envVars).to.deep.equal(['TEST=value']);
        });

        it('should handle environment variable with equals sign in value', async () => {
            const result = await setupEnvironment({
                envVars: ['CONNECTION_STRING=Server=localhost;Database=test'],
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'CONNECTION_STRING=Server=localhost;Database=test\n');

            expect(result.envVars).to.deep.equal(['CONNECTION_STRING=Server=localhost;Database=test']);
        });
    });

    describe('LD_LIBRARY_PATH handling', () => {
        it('should append to existing LD_LIBRARY_PATH on Linux', async () => {
            Object.defineProperty(process, 'platform', {
                value: 'linux'
            });
            mockEnv.LD_LIBRARY_PATH = '/existing/path';

            const result = await setupEnvironment({
                envVars: ['LD_LIBRARY_PATH=/new/path'],
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'LD_LIBRARY_PATH=/new/path:/existing/path\n');

            expect(result.envVars).to.deep.equal(['LD_LIBRARY_PATH=/new/path']);
        });

        it('should use only new value if LD_LIBRARY_PATH not set', async () => {
            Object.defineProperty(process, 'platform', {
                value: 'linux'
            });

            await setupEnvironment({
                envVars: ['LD_LIBRARY_PATH=/new/path'],
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'LD_LIBRARY_PATH=/new/path\n');
        });

        it('should use semicolon separator on Windows', async () => {
            Object.defineProperty(process, 'platform', {
                value: 'win32'
            });
            mockEnv.LD_LIBRARY_PATH = 'C:\\existing\\path';

            await setupEnvironment({
                envVars: ['LD_LIBRARY_PATH=C:\\new\\path'],
                paths: null,
                env: mockEnv,
                logger: mockLogger
            });

            sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'LD_LIBRARY_PATH=C:\\new\\path;C:\\existing\\path\n');
        });
    });

    describe('error handling', () => {
        it('should throw error if GITHUB_WORKSPACE is not set', async () => {
            const invalidEnv = {
                GITHUB_PATH: '/tmp/github_path',
                GITHUB_ENV: '/tmp/github_env'
            };

            try {
                await setupEnvironment({
                    envVars: null,
                    paths: null,
                    env: invalidEnv,
                    logger: mockLogger
                });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.equal('GITHUB_WORKSPACE environment variable is not set');
            }
        });

        it('should throw error if GITHUB_PATH is not set when adding paths', async () => {
            const invalidEnv = {
                GITHUB_WORKSPACE: '/workspace',
                GITHUB_ENV: '/tmp/github_env'
            };

            try {
                await setupEnvironment({
                    envVars: null,
                    paths: ['/test/path'],
                    env: invalidEnv,
                    logger: mockLogger
                });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.equal('GITHUB_PATH environment variable is not set');
            }
        });

        it('should throw error if GITHUB_ENV is not set when adding env vars', async () => {
            const invalidEnv = {
                GITHUB_WORKSPACE: '/workspace',
                GITHUB_PATH: '/tmp/github_path'
            };

            try {
                await setupEnvironment({
                    envVars: ['TEST=value'],
                    paths: null,
                    env: invalidEnv,
                    logger: mockLogger
                });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.equal('GITHUB_ENV environment variable is not set');
            }
        });
    });

    describe('empty inputs', () => {
        it('should handle empty arrays', async () => {
            const result = await setupEnvironment({
                envVars: [],
                paths: [],
                env: mockEnv,
                logger: mockLogger
            });

            expect(result.envVars).to.deep.equal([]);
            expect(result.paths).to.deep.equal([]);
        });
    });
});

describe('run', () => {
    let mockCore;
    let mockEnv;
    let appendFileSyncStub;
    let tmpdirStub;
    let originalPlatform;

    beforeEach(() => {
        appendFileSyncStub = sinon.stub(fs, 'appendFileSync');
        tmpdirStub = sinon.stub(os, 'tmpdir');
        originalPlatform = process.platform;

        mockCore = {
            getInput: sinon.stub(),
            info: sinon.stub(),
            setFailed: sinon.stub()
        };

        mockEnv = {
            GITHUB_WORKSPACE: '/workspace',
            GITHUB_PATH: '/tmp/github_path',
            GITHUB_ENV: '/tmp/github_env'
        };
    });

    afterEach(() => {
        sinon.restore();
        Object.defineProperty(process, 'platform', {
            value: originalPlatform
        });
    });

    it('should parse comma-separated inputs', async () => {
        mockCore.getInput.withArgs('add-env').returns('FOO=bar,BAZ=qux');
        mockCore.getInput.withArgs('add-path').returns('/path1,/path2');

        Object.defineProperty(process, 'platform', {
            value: 'linux'
        });

        await run({
            core: mockCore,
            env: mockEnv
        });

        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'FOO=bar\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'BAZ=qux\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/path1\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/path2\n');

        sinon.assert.notCalled(mockCore.setFailed);
    });

    it('should use defaults when no inputs provided', async () => {
        mockCore.getInput.withArgs('add-env').returns('');
        mockCore.getInput.withArgs('add-path').returns('');

        Object.defineProperty(process, 'platform', {
            value: 'linux'
        });
        tmpdirStub.returns('/tmp');

        await run({
            core: mockCore,
            env: mockEnv
        });

        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/workspace/.venv/bin\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'VIRTUAL_ENV=/workspace/.venv\n');

        sinon.assert.notCalled(mockCore.setFailed);
    });

    it('should handle errors gracefully', async () => {
        mockCore.getInput.withArgs('add-env').returns('');
        mockCore.getInput.withArgs('add-path').returns('');

        // Remove GITHUB_WORKSPACE to trigger error
        const invalidEnv = {
            GITHUB_PATH: '/tmp/github_path',
            GITHUB_ENV: '/tmp/github_env'
        };

        await run({
            core: mockCore,
            env: invalidEnv
        });

        sinon.assert.calledOnce(mockCore.setFailed);
        sinon.assert.calledWith(mockCore.setFailed, 'GITHUB_WORKSPACE environment variable is not set');
    });

    it('should trim whitespace from inputs', async () => {
        mockCore.getInput.withArgs('add-env').returns('  FOO=bar  ,  BAZ=qux  ');
        mockCore.getInput.withArgs('add-path').returns('  /path1  ,  /path2  ');

        Object.defineProperty(process, 'platform', {
            value: 'linux'
        });

        await run({
            core: mockCore,
            env: mockEnv
        });

        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'FOO=bar\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_env', 'BAZ=qux\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/path1\n');
        sinon.assert.calledWith(appendFileSyncStub, '/tmp/github_path', '/path2\n');
    });
});
