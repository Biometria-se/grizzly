import { expect } from 'chai';
import sinon from 'sinon';
import { checkPullRequest, run } from '../src/index.js';

// Note: The run() function uses process.env.GITHUB_TOKEN
// which is automatically available in GitHub Actions.
// Tests focus on checkPullRequest() which accepts octokit via dependency injection.

describe('checkPullRequest', () => {
    // Create stub logger to suppress output during tests
    const mockLogger = {
        info: sinon.stub(),
        warning: sinon.stub(),
        error: sinon.stub(),
        debug: sinon.stub()
    };

    let mockContext;
    let mockOctokit;

    beforeEach(() => {
        // Reset all logger stubs before each test
        sinon.reset();

        // Create mock context
        mockContext = {
            repo: {
                owner: 'test-owner',
                repo: 'test-repo'
            },
            payload: {
                pull_request: null
            }
        };

        // Create mock octokit
        mockOctokit = {
            rest: {
                pulls: {
                    get: sinon.stub()
                },
                repos: {
                    listPullRequestsAssociatedWithCommit: sinon.stub()
                }
            }
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('manual trigger (workflow_dispatch)', () => {
        it('should process merged PR with patch label', async () => {
            const mockPR = {
                number: 123,
                merged: true,
                merge_commit_sha: 'abc123',
                base: { sha: 'base123' },
                labels: [{ name: 'patch' }]
            };

            mockOctokit.rest.pulls.get.resolves({ data: mockPR });

            const result = await checkPullRequest(mockContext, mockOctokit, 123, mockLogger);

            expect(result).to.deep.equal({
                shouldRelease: true,
                versionBump: 'patch',
                prNumber: 123,
                commitSha: 'abc123',
                baseCommitSha: 'base123'
            });

            sinon.assert.calledOnce(mockOctokit.rest.pulls.get);
            sinon.assert.calledWith(mockOctokit.rest.pulls.get, {
                owner: 'test-owner',
                repo: 'test-repo',
                pull_number: 123
            });
        });

        it('should process merged PR with minor label', async () => {
            const mockPR = {
                number: 456,
                merged: true,
                merge_commit_sha: 'def456',
                base: { sha: 'base456' },
                labels: [{ name: 'minor' }, { name: 'documentation' }]
            };

            mockOctokit.rest.pulls.get.resolves({ data: mockPR });

            const result = await checkPullRequest(mockContext, mockOctokit, 456, mockLogger);

            expect(result.versionBump).to.equal('minor');
            expect(result.shouldRelease).to.be.true;
        });

        it('should process merged PR with major label', async () => {
            const mockPR = {
                number: 789,
                merged: true,
                merge_commit_sha: 'ghi789',
                base: { sha: 'base789' },
                labels: [{ name: 'major' }]
            };

            mockOctokit.rest.pulls.get.resolves({ data: mockPR });

            const result = await checkPullRequest(mockContext, mockOctokit, 789, mockLogger);

            expect(result.versionBump).to.equal('major');
            expect(result.shouldRelease).to.be.true;
        });

        it('should throw error for unmerged PR', async () => {
            const mockPR = {
                number: 999,
                merged: false,
                merge_commit_sha: null,
                base: { sha: 'base999' },
                labels: [{ name: 'patch' }]
            };

            mockOctokit.rest.pulls.get.resolves({ data: mockPR });

            try {
                await checkPullRequest(mockContext, mockOctokit, 999, mockLogger);
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.equal('PR #999 is not merged');
            }
        });

        it('should throw error when no version label found', async () => {
            const mockPR = {
                number: 111,
                merged: true,
                merge_commit_sha: 'jkl111',
                base: { sha: 'base111' },
                labels: [{ name: 'documentation' }, { name: 'bug' }]
            };

            mockOctokit.rest.pulls.get.resolves({ data: mockPR });

            try {
                await checkPullRequest(mockContext, mockOctokit, 111, mockLogger);
                expect.fail('Should have thrown an error');
            } catch (error) {
                expect(error.message).to.equal('no version release label found on PR #111');
            }
        });
    });

    describe('automatic trigger (push event)', () => {
        beforeEach(() => {
            // Set the commit SHA in context for push events
            mockContext.sha = 'abc123commit';
        });

        it('should find and process PR associated with commit', async () => {
            const mockPR = {
                number: 222,
                merged_at: '2024-01-01T00:00:00Z',
                merge_commit_sha: 'abc123commit',
                base: { sha: 'base222' },
                labels: [{ name: 'patch' }]
            };

            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            const result = await checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            expect(result).to.deep.equal({
                shouldRelease: true,
                versionBump: 'patch',
                prNumber: 222,
                commitSha: 'abc123commit',
                baseCommitSha: 'base222'
            });

            sinon.assert.calledOnce(mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit);
            sinon.assert.calledWith(mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit, {
                owner: 'test-owner',
                repo: 'test-repo',
                commit_sha: 'abc123commit'
            });
            sinon.assert.notCalled(mockOctokit.rest.pulls.get);
        });

        it('should handle multiple PRs and select the first one', async () => {
            const mockPRs = [
                {
                    number: 333,
                    merged_at: '2024-01-02T00:00:00Z',
                    merge_commit_sha: 'def456commit',
                    base: { sha: 'base333' },
                    labels: [{ name: 'minor' }]
                },
                {
                    number: 444,
                    merged_at: '2024-01-01T00:00:00Z',
                    merge_commit_sha: 'def456commit',
                    base: { sha: 'base444' },
                    labels: [{ name: 'patch' }]
                }
            ];

            mockContext.sha = 'def456commit';
            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: mockPRs
            });

            const result = await checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            expect(result.prNumber).to.equal(333);
            expect(result.versionBump).to.equal('minor');
        });

        it('should skip when no PR found for commit', async () => {
            mockContext.sha = 'nopr123';
            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: []
            });

            const clock = sinon.useFakeTimers();

            const resultPromise = checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            // Fast-forward through all retry delays (1s, 2s, 4s, 8s)
            await clock.tickAsync(1000 + 2000 + 4000 + 8000);

            const result = await resultPromise;

            expect(result).to.deep.equal({
                shouldRelease: false,
                versionBump: null,
                prNumber: null,
                commitSha: 'nopr123',
                baseCommitSha: null
            });

            clock.restore();
        });

        it('should retry when PR not found initially (eventual consistency)', async () => {
            const mockPR = {
                number: 888,
                merged_at: '2024-01-05T00:00:00Z',
                merge_commit_sha: 'retry123',
                base: { sha: 'base888' },
                labels: [{ name: 'patch' }]
            };

            mockContext.sha = 'retry123';

            // First call returns empty, second call returns the PR (simulating eventual consistency)
            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit
                .onFirstCall().resolves({ data: [] })
                .onSecondCall().resolves({ data: [mockPR] });

            const clock = sinon.useFakeTimers();

            const resultPromise = checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            // Fast-forward through the retry delay
            await clock.tickAsync(1000);

            const result = await resultPromise;

            expect(result).to.deep.equal({
                shouldRelease: true,
                versionBump: 'patch',
                prNumber: 888,
                commitSha: 'retry123',
                baseCommitSha: 'base888'
            });

            // Verify it called the API twice
            sinon.assert.calledTwice(mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit);

            clock.restore();
        });

        it('should give up after max retries when PR never found', async () => {
            mockContext.sha = 'neverFound123';

            // Always return empty
            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: []
            });

            const clock = sinon.useFakeTimers();

            const resultPromise = checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            // Fast-forward through all retry delays (1s, 2s, 4s, 8s)
            await clock.tickAsync(1000 + 2000 + 4000 + 8000);

            const result = await resultPromise;

            expect(result).to.deep.equal({
                shouldRelease: false,
                versionBump: null,
                prNumber: null,
                commitSha: 'neverFound123',
                baseCommitSha: null
            });

            // Verify it called the API 5 times (initial + 4 retries)
            expect(mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.callCount).to.equal(5);

            clock.restore();
        });

        it('should skip when PR is not merged', async () => {
            const mockPR = {
                number: 555,
                merged_at: null,
                merge_commit_sha: null,
                base: { sha: 'base555' },
                labels: [{ name: 'patch' }]
            };

            mockContext.sha = 'unmerged123';
            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            const result = await checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            expect(result).to.deep.equal({
                shouldRelease: false,
                versionBump: null,
                prNumber: 555,
                commitSha: 'unmerged123',
                baseCommitSha: 'base555'
            });
        });

        it('should prioritize first matching version label', async () => {
            mockContext.sha = 'multilabel123';
            const mockPR = {
                number: 666,
                merged_at: '2024-01-03T00:00:00Z',
                merge_commit_sha: 'multilabel123',
                base: { sha: 'base666' },
                labels: [
                    { name: 'major' },
                    { name: 'minor' },
                    { name: 'patch' }
                ]
            };

            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            const result = await checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            expect(result.versionBump).to.equal('major');
        });

        it('should skip when no version label in automatic trigger', async () => {
            mockContext.sha = 'nolabel123';
            const mockPR = {
                number: 777,
                merged_at: '2024-01-04T00:00:00Z',
                merge_commit_sha: 'nolabel123',
                base: { sha: 'base777' },
                labels: []
            };

            mockOctokit.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            const result = await checkPullRequest(mockContext, mockOctokit, null, mockLogger);

            expect(result).to.deep.equal({
                shouldRelease: false,
                versionBump: null,
                prNumber: 777,
                commitSha: 'nolabel123',
                baseCommitSha: 'base777'
            });
        });
    });

    describe('label parsing', () => {
        it('should extract labels correctly', async () => {
            const mockPR = {
                number: 555,
                merged: true,
                merge_commit_sha: 'vwx555',
                base: { sha: 'base555' },
                labels: [
                    { name: 'bug' },
                    { name: 'patch' },
                    { name: 'documentation' }
                ]
            };

            mockOctokit.rest.pulls.get.resolves({ data: mockPR });

            const result = await checkPullRequest(mockContext, mockOctokit, 555, mockLogger);

            expect(result.versionBump).to.equal('patch');
            sinon.assert.calledWith(mockLogger.info, sinon.match(/bug, patch, documentation/));
        });
    });
});

describe('run', () => {
    let coreStub;
    let githubStub;
    let octokitStub;

    beforeEach(() => {
        // Setup core stub
        coreStub = {
            getInput: sinon.stub(),
            setOutput: sinon.stub(),
            info: sinon.stub(),
            setFailed: sinon.stub(),
        };

        // Setup octokit mock
        octokitStub = {
            rest: {
                pulls: {
                    get: sinon.stub(),
                },
                repos: {
                    listPullRequestsAssociatedWithCommit: sinon.stub(),
                },
            },
        };

        // Setup github stub
        githubStub = {
            getOctokit: sinon.stub().returns(octokitStub),
            context: {
                repo: {
                    owner: 'test-owner',
                    repo: 'test-repo',
                },
                payload: {
                    pull_request: null,
                },
            },
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    describe('environment variable handling', () => {
        it('should fail when github-token is missing', async () => {
            coreStub.getInput.withArgs('pr-number').returns('');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('');

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.setFailed.called).to.be.true;
            expect(coreStub.setOutput.calledWith('should-release', 'false')).to.be.true;
        });

        it('should use github-token from input', async () => {
            coreStub.getInput.withArgs('pr-number').returns('');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token-123');

            githubStub.context.payload.pull_request = {
                number: 123,
                merged: true,
                merge_commit_sha: 'abc123',
                base: { sha: 'base123' },
                labels: [{ name: 'patch' }],
            };

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(githubStub.getOctokit.calledOnce).to.be.true;
            expect(githubStub.getOctokit.calledWith('test-token-123')).to.be.true;
        });
    });

    describe('automatic trigger', () => {
        it('should set all outputs correctly for automatic trigger', async () => {
            coreStub.getInput.withArgs('pr-number').returns('');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            githubStub.context.sha = 'def456';

            const mockPR = {
                number: 456,
                merged_at: '2024-01-01T00:00:00Z',
                merge_commit_sha: 'def456',
                base: { sha: 'base456' },
                labels: [{ name: 'minor' }],
            };

            octokitStub.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.setOutput.calledWith('should-release', 'true')).to.be.true;
            expect(coreStub.setOutput.calledWith('version-bump', 'minor')).to.be.true;
            expect(coreStub.setOutput.calledWith('pr-number', '456')).to.be.true;
            expect(coreStub.setOutput.calledWith('commit-sha', 'def456')).to.be.true;
            expect(coreStub.setOutput.calledWith('base-commit-sha', 'base456')).to.be.true;
            expect(coreStub.setFailed.called).to.be.false;
        });

        it('should log info messages', async () => {
            coreStub.getInput.withArgs('pr-number').returns('');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            githubStub.context.sha = 'ghi789';

            const mockPR = {
                number: 789,
                merged_at: '2024-01-02T00:00:00Z',
                merge_commit_sha: 'ghi789',
                base: { sha: 'base789' },
                labels: [{ name: 'major' }],
            };

            octokitStub.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.info.calledWith('Checking pull request for version bump labels...')).to.be.true;
            expect(coreStub.info.calledWith('Pull request check completed successfully')).to.be.true;
        });
    });

    describe('manual trigger', () => {
        it('should set all outputs correctly for manual trigger', async () => {
            coreStub.getInput.withArgs('pr-number').returns('999');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            const mockPR = {
                number: 999,
                merged: true,
                merge_commit_sha: 'jkl999',
                base: { sha: 'base999' },
                labels: [{ name: 'patch' }],
            };

            octokitStub.rest.pulls.get.resolves({ data: mockPR });

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.setOutput.calledWith('should-release', 'true')).to.be.true;
            expect(coreStub.setOutput.calledWith('version-bump', 'patch')).to.be.true;
            expect(coreStub.setOutput.calledWith('pr-number', '999')).to.be.true;
            expect(coreStub.setOutput.calledWith('commit-sha', 'jkl999')).to.be.true;
            expect(coreStub.setOutput.calledWith('base-commit-sha', 'base999')).to.be.true;
            expect(coreStub.setFailed.called).to.be.false;
        });

        it('should call GitHub API with correct PR number', async () => {
            coreStub.getInput.withArgs('pr-number').returns('111');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            const mockPR = {
                number: 111,
                merged: true,
                merge_commit_sha: 'mno111',
                base: { sha: 'base111' },
                labels: [{ name: 'minor' }],
            };

            octokitStub.rest.pulls.get.resolves({ data: mockPR });

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(octokitStub.rest.pulls.get.calledOnce).to.be.true;
            expect(octokitStub.rest.pulls.get.calledWith({
                owner: 'test-owner',
                repo: 'test-repo',
                pull_number: 111,
            })).to.be.true;
        });
    });

    describe('error handling', () => {
        it('should set should-release to false when no version label (automatic trigger)', async () => {
            coreStub.getInput.withArgs('pr-number').returns('');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            githubStub.context.sha = 'pqr222';

            const mockPR = {
                number: 222,
                merged_at: '2024-01-03T00:00:00Z',
                merge_commit_sha: 'pqr222',
                base: { sha: 'base222' },
                labels: [], // No version label
            };

            octokitStub.rest.repos.listPullRequestsAssociatedWithCommit.resolves({
                data: [mockPR]
            });

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.setOutput.calledWith('should-release', 'false')).to.be.true;
            expect(coreStub.setOutput.calledWith('version-bump', '')).to.be.true;
            expect(coreStub.setOutput.calledWith('pr-number', '222')).to.be.true;
            expect(coreStub.setFailed.called).to.be.false;
        });

        it('should handle unmerged PR error', async () => {
            coreStub.getInput.withArgs('pr-number').returns('333');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            const mockPR = {
                number: 333,
                merged: false,
                merge_commit_sha: null,
                base: { sha: 'base333' },
                labels: [{ name: 'patch' }],
            };

            octokitStub.rest.pulls.get.resolves({ data: mockPR });

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.setOutput.calledWith('should-release', 'false')).to.be.true;
            expect(coreStub.setFailed.calledOnce).to.be.true;
            expect(coreStub.setFailed.firstCall.args[0]).to.equal('PR #333 is not merged');
        });

        it('should handle API errors', async () => {
            coreStub.getInput.withArgs('pr-number').returns('444');
            coreStub.getInput.withArgs('github-token', { required: true }).returns('test-token');

            octokitStub.rest.pulls.get.rejects(new Error('API rate limit exceeded'));

            await run({
                core: coreStub,
                github: githubStub,
            });

            expect(coreStub.setOutput.calledWith('should-release', 'false')).to.be.true;
            expect(coreStub.setFailed.calledOnce).to.be.true;
            expect(coreStub.setFailed.firstCall.args[0]).to.equal('API rate limit exceeded');
        });
    });
});
