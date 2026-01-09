import { expect } from 'chai';
import sinon from 'sinon';
import { mapChanges, run } from '../src/index.js';
import fs from 'fs';

describe('mapChanges', () => {
    let readFileSyncStub;
    let existsSyncStub;
    let readdirSyncStub;
    let statSyncStub;
    let mockLogger;

    beforeEach(() => {
        readFileSyncStub = sinon.stub(fs, 'readFileSync');
        existsSyncStub = sinon.stub(fs, 'existsSync');
        readdirSyncStub = sinon.stub(fs, 'readdirSync');
        statSyncStub = sinon.stub(fs, 'statSync');
        sinon.stub(console, 'log'); // Suppress console.log output in tests

        mockLogger = {
            log: sinon.stub(),
            info: sinon.stub(),
            error: sinon.stub()
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('basic functionality', () => {
        it('should parse JSON changes input', async () => {
            const mockUvLock = 'package = []';

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            existsSyncStub.returns(false);

            const result = await mapChanges({
                changes: '["framework"]',
                force: false,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result).to.have.property('changes_uv');
            expect(result).to.have.property('changes_npm');
        });

        it('should use force mode to load all packages from changes-filter.yaml', async () => {
            const mockUvLock = 'package = []';
            const mockChangeFilters = 'workflows: ".github/workflows/**"\nframework: "framework/**"';

            readFileSyncStub.withArgs(sinon.match(/changes-filter\.yaml/)).returns(mockChangeFilters);
            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            existsSyncStub.returns(false);

            const result = await mapChanges({
                changes: '[]',
                force: true,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result).to.have.property('changes_uv');
            sinon.assert.calledWith(readFileSyncStub, sinon.match(/changes-filter\.yaml/));
        });

        it('should throw error on invalid JSON in changes', async () => {
            try {
                await mapChanges({
                    changes: 'invalid json',
                    force: false,
                    release: false,
                    workspaceRoot: '/workspace',
                    logger: mockLogger
                });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('Invalid JSON');
            }
        });

        it('should throw error if workflows modified during release', async () => {
            const mockUvLock = 'package = []';
            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);

            try {
                await mapChanges({
                    changes: '["workflows"]',
                    force: false,
                    release: true,
                    workspaceRoot: '/workspace',
                    logger: mockLogger
                });
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.include('Workflow files cannot be part of a release');
            }
        });
    });

    describe('Python package detection', () => {
        it('should detect Python package with unit and e2e tests', async () => {
            const mockUvLock = 'package = []';
            const mockPyproject = `
[project]
name = "grizzly-loadtester"
`;

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/pyproject\.toml/)).returns(mockPyproject);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/tests$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/test_framework$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/unit$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/e2e$/)).returns(true);
            existsSyncStub.callThrough();

            readdirSyncStub.withArgs(sinon.match(/tests$/)).returns(['test_framework']);
            statSyncStub.returns({ isDirectory: () => true });

            const result = await mapChanges({
                changes: '["framework"]',
                force: false,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_uv).to.have.lengthOf(1);
            expect(result.changes_uv[0].package).to.equal('grizzly-loadtester');
            expect(result.changes_uv[0].tests.unit).to.include('unit');
            expect(result.changes_uv[0].tests.e2e.local).to.include('e2e');
        });

        it('should detect Python package with only unit tests', async () => {
            const mockUvLock = 'package = []';
            const mockPyproject = `
[project]
name = "test-package"
`;

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/pyproject\.toml/)).returns(mockPyproject);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/tests$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/test_package$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/unit$/)).returns(false);
            existsSyncStub.withArgs(sinon.match(/e2e$/)).returns(false);
            existsSyncStub.callThrough();

            readdirSyncStub.withArgs(sinon.match(/tests$/)).returns(['test_package']);
            statSyncStub.returns({ isDirectory: () => true });

            const result = await mapChanges({
                changes: '["framework"]',
                force: false,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_uv).to.have.lengthOf(1);
            expect(result.changes_uv[0].package).to.equal('test-package');
            expect(result.changes_uv[0].tests.unit).to.include('test_package');
            expect(result.changes_uv[0].tests.e2e.local).to.equal('');
        });

        it('should skip Python package without release config in release mode', async () => {
            const mockUvLock = 'package = []';
            const mockPyproject = `
[project]
name = "test-package"
`;

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/pyproject\.toml/)).returns(mockPyproject);
            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(true);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["framework"]',
                force: false,
                release: true,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_uv).to.have.lengthOf(0);
        });

        it('should include Python package with release config in release mode', async () => {
            const mockUvLock = 'package = []';
            const mockPyproject = `
[project]
name = "test-package"

[tool.hatch.version]
source = "vcs"

[tool.hatch.version.raw-options]
scm = { git = { describe_command = "git describe --tags" } }
`;

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/pyproject\.toml/)).returns(mockPyproject);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/tests$/)).returns(false);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["framework"]',
                force: false,
                release: true,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_uv).to.have.lengthOf(1);
            expect(result.changes_uv[0].package).to.equal('test-package');
        });

        it('should include reverse dependencies', async () => {
            const mockUvLock = `
[[package]]
name = "grizzly-loadtester-common"
source = { editable = "common" }

[[package]]
name = "grizzly-loadtester"
source = { editable = "framework" }
dependencies = [
    { name = "grizzly-loadtester-common" }
]
`;
            const mockPyproject = `
[project]
name = "grizzly-loadtester-common"
`;

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/pyproject\.toml/)).returns(mockPyproject);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/tests$/)).returns(false);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["common"]',
                force: false,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_uv).to.have.lengthOf(2);
            const packages = result.changes_uv.map(c => c.package).sort();
            expect(packages).to.deep.equal(['grizzly-loadtester', 'grizzly-loadtester-common']);
        });
    });

    describe('Node package detection', () => {
        it('should detect Node package with test scripts', async () => {
            const mockUvLock = 'package = []';
            const mockPackageJson = JSON.stringify({
                name: 'test-extension',
                scripts: {
                    test: 'mocha',
                    'test:e2e': 'mocha e2e'
                }
            });

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/package\.json$/)).returns(mockPackageJson);
            readdirSyncStub.returns(['package.json', 'src', 'test']);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(false);
            existsSyncStub.withArgs(sinon.match(/package\.json$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/clients\/vscode$/)).returns(true);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["editor-support/clients/vscode"]',
                force: false,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_npm).to.have.lengthOf(1);
            expect(result.changes_npm[0].package).to.equal('test-extension');
            expect(result.changes_npm[0].tests.unit).to.equal('test');
            expect(result.changes_npm[0].tests.e2e.local).to.equal('test:e2e');
        });

        it('should skip Node package without release config in release mode', async () => {
            const mockUvLock = 'package = []';
            const mockPackageJson = JSON.stringify({
                name: 'test-extension'
            });

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/package\.json$/)).returns(mockPackageJson);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(false);
            existsSyncStub.withArgs(sinon.match(/package\.json$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/package\.local\.json$/)).returns(false);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["editor-support/clients/vscode"]',
                force: false,
                release: true,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_npm).to.have.lengthOf(0);
        });

        it('should include Node package with release config in release mode', async () => {
            const mockUvLock = 'package = []';
            const mockPackageJson = JSON.stringify({
                name: 'test-extension',
                scripts: { test: 'mocha' }
            });
            const mockPackageLocalJson = JSON.stringify({
                tag: { pattern: 'v*' }
            });

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/package\.json$/)).returns(mockPackageJson);
            readFileSyncStub.withArgs(sinon.match(/package\.local\.json$/)).returns(mockPackageLocalJson);
            readdirSyncStub.returns(['package.json', 'package.local.json', 'src', 'test']);

            existsSyncStub.withArgs(sinon.match(/pyproject\.toml$/)).returns(false);
            existsSyncStub.withArgs(sinon.match(/package\.json$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/package\.local\.json$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/clients\/vscode$/)).returns(true);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["editor-support/clients/vscode"]',
                force: false,
                release: true,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_npm).to.have.lengthOf(1);
            expect(result.changes_npm[0].package).to.equal('test-extension');
        });
    });

    describe('sorting and output', () => {
        it('should sort changes by package name', async () => {
            const mockUvLock = 'package = []';
            const mockPyproject1 = '[project]\nname = "zebra-package"';
            const mockPyproject2 = '[project]\nname = "alpha-package"';

            readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
            readFileSyncStub.withArgs(sinon.match(/dir1.*pyproject\.toml/)).returns(mockPyproject1);
            readFileSyncStub.withArgs(sinon.match(/dir2.*pyproject\.toml/)).returns(mockPyproject2);

            existsSyncStub.withArgs(sinon.match(/dir1.*pyproject\.toml$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/dir2.*pyproject\.toml$/)).returns(true);
            existsSyncStub.withArgs(sinon.match(/tests$/)).returns(false);
            existsSyncStub.callThrough();

            const result = await mapChanges({
                changes: '["dir1", "dir2"]',
                force: false,
                release: false,
                workspaceRoot: '/workspace',
                logger: mockLogger
            });

            expect(result.changes_uv).to.have.lengthOf(2);
            expect(result.changes_uv[0].package).to.equal('alpha-package');
            expect(result.changes_uv[1].package).to.equal('zebra-package');
        });
    });
});

describe('run', () => {
    let mockCore;
    let mockEnv;
    let readFileSyncStub;
    let existsSyncStub;

    beforeEach(() => {
        readFileSyncStub = sinon.stub(fs, 'readFileSync');
        existsSyncStub = sinon.stub(fs, 'existsSync');
        sinon.stub(console, 'log'); // Suppress console.log output in tests

        mockCore = {
            getInput: sinon.stub(),
            setOutput: sinon.stub(),
            setFailed: sinon.stub(),
            info: sinon.stub()
        };

        mockEnv = {
            GITHUB_WORKSPACE: '/workspace'
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should process inputs and set outputs', async () => {
        mockCore.getInput.withArgs('changes', sinon.match.any).returns('["framework"]');
        mockCore.getInput.withArgs('force', sinon.match.any).returns('false');
        mockCore.getInput.withArgs('release').returns('false');

        const mockUvLock = 'package = []';
        readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
        existsSyncStub.returns(false);

        await run({
            core: mockCore,
            env: mockEnv
        });

        sinon.assert.calledWith(mockCore.setOutput, 'changes_uv', sinon.match.string);
        sinon.assert.calledWith(mockCore.setOutput, 'changes_npm', sinon.match.string);
        sinon.assert.notCalled(mockCore.setFailed);
    });

    it('should handle errors gracefully', async () => {
        mockCore.getInput.withArgs('changes', sinon.match.any).returns('invalid json');
        mockCore.getInput.withArgs('force', sinon.match.any).returns('false');
        mockCore.getInput.withArgs('release').returns('false');

        await run({
            core: mockCore,
            env: mockEnv
        });

        sinon.assert.calledOnce(mockCore.setFailed);
        sinon.assert.calledWith(mockCore.setFailed, sinon.match(/Invalid JSON/));
    });

    it('should handle release mode', async () => {
        mockCore.getInput.withArgs('changes', sinon.match.any).returns('["framework"]');
        mockCore.getInput.withArgs('force', sinon.match.any).returns('false');
        mockCore.getInput.withArgs('release').returns('true');

        const mockUvLock = 'package = []';
        readFileSyncStub.withArgs(sinon.match(/uv\.lock/)).returns(mockUvLock);
        existsSyncStub.returns(false);

        await run({
            core: mockCore,
            env: mockEnv
        });

        sinon.assert.notCalled(mockCore.setFailed);
    });
});
