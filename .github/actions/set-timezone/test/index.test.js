import { expect } from 'chai';
import sinon from 'sinon';
import { setTimezone, run } from '../src/index.js';

describe('setTimezone', () => {
    let executeStub;

    beforeEach(() => {
        executeStub = sinon.stub().resolves();
    });

    it('should set timezone on Linux using timedatectl', async () => {
        await setTimezone({
            platform: 'linux',
            timezoneLinux: 'Europe/Stockholm',
            execute: executeStub
        });

        sinon.assert.calledWith(executeStub, 'sudo', ['timedatectl', 'set-timezone', 'Europe/Stockholm']);
    });

    it('should set timezone on macOS using systemsetup', async () => {
        await setTimezone({
            platform: 'darwin',
            timezoneMacos: 'Europe/Stockholm',
            execute: executeStub
        });

        sinon.assert.calledWith(executeStub, 'sudo', ['systemsetup', '-settimezone', 'Europe/Stockholm']);
    });

    it('should set timezone on Windows using tzutil', async () => {
        await setTimezone({
            platform: 'win32',
            timezoneWindows: 'W. Europe Standard Time',
            execute: executeStub
        });

        sinon.assert.calledWith(executeStub, 'tzutil', ['/s', 'W. Europe Standard Time']);
    });

    it('should throw for an unsupported platform', async () => {
        try {
            await setTimezone({ platform: 'freebsd', execute: executeStub });
            expect.fail('should have thrown');
        } catch (error) {
            expect(error.message).to.match(/not supported/);
        }
    });
});

describe('run', () => {
    let mockCore;
    let executeStub;

    beforeEach(() => {
        executeStub = sinon.stub().resolves();
        mockCore = {
            getInput: sinon.stub(),
            setFailed: sinon.stub()
        };
    });

    it('should read inputs and set the timezone', async () => {
        mockCore.getInput.withArgs('timezoneLinux').returns('Europe/Stockholm');

        await run({ core: mockCore, execute: executeStub, platform: 'linux' });

        sinon.assert.calledWith(executeStub, 'sudo', ['timedatectl', 'set-timezone', 'Europe/Stockholm']);
        sinon.assert.notCalled(mockCore.setFailed);
    });

    it('should call setFailed on error', async () => {
        await run({ core: mockCore, execute: executeStub, platform: 'freebsd' });

        sinon.assert.calledOnce(mockCore.setFailed);
    });
});
