import { expect } from 'chai';
import sinon from 'sinon';
import { setupXvfb, run, installXvfb, cleanupXvfb, runCommand, runCommandWithXvfb } from '../src/index.js';

describe('setupXvfb', () => {
    let executeStub;
    let mockLogger;

    beforeEach(() => {
        executeStub = sinon.stub().resolves();
        mockLogger = { info: sinon.stub() };
    });

    it('should install xvfb and wrap each command on Linux', async () => {
        await setupXvfb({
            platform: 'linux',
            commands: ['npm run test:e2e'],
            execute: executeStub,
            logger: mockLogger
        });

        sinon.assert.calledWith(executeStub, 'sudo apt-get update');
        sinon.assert.calledWith(executeStub, 'sudo apt-get install -y xvfb x11-xserver-utils');
        sinon.assert.calledWith(executeStub, 'xvfb-run --auto-servernum npm run test:e2e', []);
    });

    it('should include server options when provided', async () => {
        await setupXvfb({
            platform: 'linux',
            commands: ['npm run test:e2e'],
            serverOptions: '-screen 0 1024x768x24',
            execute: executeStub,
            logger: mockLogger
        });

        sinon.assert.calledWith(executeStub, 'xvfb-run --auto-servernum -s "-screen 0 1024x768x24" npm run test:e2e', []);
    });

    it('should run commands in the given working directory', async () => {
        await setupXvfb({
            platform: 'linux',
            commands: ['npm run test:e2e'],
            directory: '/path/to/project',
            execute: executeStub,
            logger: mockLogger
        });

        sinon.assert.calledWith(executeStub, 'xvfb-run --auto-servernum npm run test:e2e', [], { cwd: '/path/to/project' });
    });

    it('should run commands directly on non-Linux platforms without installing xvfb', async () => {
        await setupXvfb({
            platform: 'win32',
            commands: ['npm run test:e2e'],
            execute: executeStub,
            logger: mockLogger
        });

        sinon.assert.neverCalledWith(executeStub, 'sudo apt-get update');
        sinon.assert.calledWith(executeStub, 'npm run test:e2e', []);
    });

    it('should skip empty commands', async () => {
        await setupXvfb({
            platform: 'linux',
            commands: ['npm run test:e2e', ''],
            execute: executeStub,
            logger: mockLogger
        });

        sinon.assert.calledOnce(mockLogger.info);
    });

    it('should clean up xvfb processes even if the command fails', async () => {
        executeStub.withArgs(sinon.match(/^xvfb-run/)).rejects(new Error('boom'));

        try {
            await setupXvfb({
                platform: 'linux',
                commands: ['npm run test:e2e'],
                execute: executeStub,
                logger: mockLogger
            });
            expect.fail('should have thrown');
        } catch (error) {
            expect(error.message).to.equal('boom');
        }

        sinon.assert.calledWith(executeStub, 'bash', ['-c', sinon.match(/xvfb_pids/)]);
    });
});

describe('installXvfb', () => {
    it('should update apt and install xvfb with x11-xserver-utils', async () => {
        const executeStub = sinon.stub().resolves();
        await installXvfb(executeStub);

        sinon.assert.calledWith(executeStub, 'sudo apt-get update');
        sinon.assert.calledWith(executeStub, 'sudo apt-get install -y xvfb x11-xserver-utils');
    });
});

describe('cleanupXvfb', () => {
    it('should run the cleanup script', async () => {
        const executeStub = sinon.stub().resolves();
        await cleanupXvfb(executeStub);

        sinon.assert.calledWith(executeStub, 'bash', ['-c', sinon.match(/xvfb_pids/)]);
    });

    it('should swallow errors from the cleanup script', async () => {
        const executeStub = sinon.stub().rejects(new Error('cleanup failed'));

        await cleanupXvfb(executeStub);
    });
});

describe('runCommand', () => {
    it('should run without cwd when no directory is given', async () => {
        const executeStub = sinon.stub().resolves();
        await runCommand('echo hi', { execute: executeStub });
        sinon.assert.calledWith(executeStub, 'echo hi', [], undefined);
    });

    it('should run with cwd when a directory is given', async () => {
        const executeStub = sinon.stub().resolves();
        await runCommand('echo hi', { directory: '/path/to/project', execute: executeStub });
        sinon.assert.calledWith(executeStub, 'echo hi', [], { cwd: '/path/to/project' });
    });
});

describe('runCommandWithXvfb', () => {
    it('should wrap the command with xvfb-run and clean up on success', async () => {
        const executeStub = sinon.stub().resolves();

        await runCommandWithXvfb('npm run test:e2e', { execute: executeStub });

        sinon.assert.calledWith(executeStub, 'xvfb-run --auto-servernum npm run test:e2e', []);
        sinon.assert.calledWith(executeStub, 'bash', sinon.match.array);
    });

    it('should always attempt cleanup, even when cleanup itself fails', async () => {
        const executeStub = sinon.stub();
        executeStub.withArgs(sinon.match(/^xvfb-run/)).resolves();
        executeStub.withArgs('bash', sinon.match.array).rejects(new Error('cleanup failed'));

        await runCommandWithXvfb('npm run test:e2e', { execute: executeStub });

        sinon.assert.calledWith(executeStub, 'bash', sinon.match.array);
    });
});

describe('run', () => {
    let mockCore;
    let executeStub;

    beforeEach(() => {
        executeStub = sinon.stub().resolves();
        mockCore = {
            getInput: sinon.stub(),
            setFailed: sinon.stub(),
            info: sinon.stub()
        };
    });

    it('should read inputs and run commands', async () => {
        mockCore.getInput.withArgs('run', { required: true }).returns('npm run test:e2e');
        mockCore.getInput.withArgs('working-directory').returns('');
        mockCore.getInput.withArgs('options').returns('');

        await run({ core: mockCore, execute: executeStub, platform: 'win32' });

        sinon.assert.calledWith(executeStub, 'npm run test:e2e', []);
        sinon.assert.notCalled(mockCore.setFailed);
    });

    it('should call setFailed on error', async () => {
        mockCore.getInput.withArgs('run', { required: true }).throws(new Error('missing input'));

        await run({ core: mockCore, execute: executeStub, platform: 'win32' });

        sinon.assert.calledOnce(mockCore.setFailed);
    });
});
