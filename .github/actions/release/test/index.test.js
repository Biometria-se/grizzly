import { expect } from 'chai';
import sinon from 'sinon';
import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';
import { getNextReleaseTag, cleanup } from '../src/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

describe('getNextReleaseTag', () => {
  const frameworkPath = resolve(__dirname, '../../../../framework');
  let execStub;

  // Create stub logger to suppress output during tests
  const mockLogger = {
    info: sinon.stub(),
    warning: sinon.stub(),
    error: sinon.stub(),
    debug: sinon.stub()
  };

  beforeEach(() => {
    // Create exec stub to suppress command output during tests
    execStub = {
      exec: sinon.stub().callsFake(async (command, args, options) => {
        // Simulate git tag command output
        if (command === 'git' && args[0] === 'tag') {
          if (options?.listeners?.stdout) {
            options.listeners.stdout(Buffer.from('framework@v3.2.5\n'));
          }
          return 0;
        }
        return 0;
      })
    };
  });

  afterEach(() => {
    // Reset all stubs after each test
    sinon.reset();
  });

  describe('version bumping', () => {
    it('should calculate next patch version', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'patch', mockLogger, execStub);

      expect(result).to.have.property('nextVersion');
      expect(result).to.have.property('nextTag');
      expect(result.nextVersion).to.match(/^\d+\.\d+\.\d+$/);
      expect(result.nextTag).to.include(result.nextVersion);
    });

    it('should calculate next minor version', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'minor', mockLogger, execStub);

      expect(result).to.have.property('nextVersion');
      expect(result).to.have.property('nextTag');
      expect(result.nextVersion).to.match(/^\d+\.\d+\.0$/);
      expect(result.nextTag).to.include(result.nextVersion);
    });

    it('should calculate next major version', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'major', mockLogger, execStub);

      expect(result).to.have.property('nextVersion');
      expect(result).to.have.property('nextTag');
      expect(result.nextVersion).to.match(/^\d+\.0\.0$/);
      expect(result.nextTag).to.include(result.nextVersion);
    });

    it('should increment version correctly for patch', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'patch', mockLogger, execStub);
      const [, , patch] = result.nextVersion.split('.').map(Number);

      expect(patch).to.be.greaterThan(0);
    });

    it('should reset patch to 0 for minor bump', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'minor', mockLogger, execStub);
      const [, , patch] = result.nextVersion.split('.').map(Number);

      expect(patch).to.equal(0);
      const minor = result.nextVersion.split('.')[1];
      expect(Number(minor)).to.be.greaterThan(0);
    });

    it('should reset minor and patch to 0 for major bump', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'major', mockLogger, execStub);
      const [major, minor, patch] = result.nextVersion.split('.').map(Number);
      expect(minor).to.equal(0);
      expect(patch).to.equal(0);
      expect(major).to.be.greaterThan(0);
    });
  });

  describe('tag format', () => {
    it('should include project prefix in tag', async () => {
      const result = await getNextReleaseTag(frameworkPath, 'patch', mockLogger, execStub);

      expect(result.nextTag).to.match(/^framework@v\d+\.\d+\.\d+$/);
    });
  });

  describe('error handling', () => {
    it('should throw error for non-existent path', async () => {
      try {
        await getNextReleaseTag('/non/existent/path', 'patch', mockLogger, execStub);
        expect.fail('Should have thrown an error');
      } catch (error) {
        expect(error).to.exist;
      }
    });
  });
});

describe('cleanup', () => {
  let coreStub;
  let execStub;
  let octokitStub;
  let githubStub;

  beforeEach(() => {
    // Setup core stub
    coreStub = {
      getState: sinon.stub(),
      info: sinon.stub(),
      warning: sinon.stub(),
      error: sinon.stub(),
      setFailed: sinon.stub(),
    };

    // Setup exec stub
    execStub = {
      exec: sinon.stub().resolves(),
    };

    // Setup octokit mock
    octokitStub = {
      rest: {
        actions: {
          listJobsForWorkflowRun: sinon.stub(),
        },
      },
    };

    // Setup github stub
    githubStub = {
      getOctokit: sinon.stub().returns(octokitStub),
    };
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('job status checking', () => {
    it('should push tag when all steps succeeded and not dry-run', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.true;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'])).to.be.false;
    });

    it('should delete tag when a step failed', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'failure',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'failure' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should delete tag when job has no steps', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'failure',
              steps: [],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
        maxWaitTime: 200, // Short timeout for test
      });

      expect(coreStub.setFailed.calledWith(sinon.match(/Cleanup step \(Post\*\) not found with in_progress status or previous steps not completed/))).to.be.true;
    });

    it('should delete tag when a step was cancelled', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'cancelled',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'cancelled' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should delete tag in dry-run mode even if all steps succeeded', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('true');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should check job status even in dry-run mode', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('true');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(octokitStub.rest.actions.listJobsForWorkflowRun.calledOnce).to.be.true;
      expect(octokitStub.rest.actions.listJobsForWorkflowRun.calledWith({
        owner: 'owner',
        repo: 'repo',
        run_id: 12345,
      })).to.be.true;
    });

    it('should parse GITHUB_REPOSITORY correctly', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 99999,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Test', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '99999',
        GITHUB_REPOSITORY: 'my-org/my-repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(octokitStub.rest.actions.listJobsForWorkflowRun.calledWith({
        owner: 'my-org',
        repo: 'my-repo',
        run_id: 99999,
      })).to.be.true;
    });
  });

  describe('error handling', () => {
    it('should fail when next-release-tag is missing', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.setFailed.called).to.be.true;
      expect(coreStub.setFailed.firstCall.args[0]).to.include('No next-release-tag found');
    });

    it('should fail when job-name is missing from state', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns(''); // Empty job name

      const env = {
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.setFailed.called).to.be.true;
      expect(coreStub.setFailed.firstCall.args[0]).to.include('Missing required environment variables or state');
    });

    it('should fail when GITHUB_RUN_ID is missing', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.setFailed.called).to.be.true;
      expect(coreStub.setFailed.firstCall.args[0]).to.include('Missing required environment variables');
    });

    it('should fail when GITHUB_REPOSITORY is missing', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.setFailed.called).to.be.true;
      expect(coreStub.setFailed.firstCall.args[0]).to.include('Missing required environment variables');
    });

    it('should fail when job name is not found', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 99999,
              name: 'other-job',
              status: 'completed',
              conclusion: 'success',
              steps: [],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.setFailed.called).to.be.true;
      expect(coreStub.setFailed.firstCall.args[0]).to.include('Could not find current job');
    });

    it('should log all jobs for debugging', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('job2');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 123,
              name: 'job1',
              status: 'completed',
              conclusion: 'success',
              steps: [],
            },
            {
              id: 456,
              name: 'job2',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Step1', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.info.calledWith('Found 2 jobs in workflow run')).to.be.true;
      expect(coreStub.info.calledWith('  Job: name="job1", id=123, status=completed, conclusion=success')).to.be.true;
      expect(coreStub.info.calledWith('  Job: name="job2", id=456, status=completed, conclusion=success')).to.be.true;
      expect(coreStub.info.calledWith('Looking for job with name: job2')).to.be.true;
    });

    it('should fail when github api call fails', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.rejects(new Error('API error'));

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.setFailed.called).to.be.true;
      expect(coreStub.setFailed.firstCall.args[0]).to.include('API error');
    });
  });

  describe('logging', () => {
    it('should log job and step status information', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.info.calledWith('Running post-job cleanup...')).to.be.true;
      expect(coreStub.info.calledWith('Checking job status for run 12345...')).to.be.true;
      expect(coreStub.info.calledWith('Job status: completed, conclusion: success')).to.be.true;
      expect(coreStub.info.calledWith(sinon.match(/Found 3 steps/))).to.be.true;
      expect(coreStub.info.calledWith('Pushing tag framework@v1.2.3 to remote')).to.be.true;
    });

    it('should log error when step fails', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'failure',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'failure' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(coreStub.info.calledWith('Deleting tag framework@v1.2.3 (job failed or was cancelled)')).to.be.true;
    });

    it('should log dry-run reason when deleting tag', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('true');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Test', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.info.calledWith('Deleting tag framework@v1.2.3 (dry-run mode)')).to.be.true;
    });

    it('should timeout if steps are still in progress after timeout', async function() {
      this.timeout(5000); // Set timeout to 5 seconds to allow for the 1 second wait

      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      // Never return "Post Setup release" step - simulating timeout waiting for it
      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'in_progress',
              conclusion: null,
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
        maxWaitTime: 200, // 200ms for testing
        baseDelayMs: 10,  // 10ms base delay for fast testing
        maxDelayMs: 50,   // 50ms max delay for fast testing
      });

      expect(coreStub.setFailed.calledWith(sinon.match(/Cleanup step \(Post\*\) not found with in_progress status or previous steps not completed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
    });

    it('should wait and retry when steps are initially in progress', async function() {
      this.timeout(5000); // Allow time for retries

      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      let callCount = 0;
      octokitStub.rest.actions.listJobsForWorkflowRun.callsFake(() => {
        callCount++;
        // First call: "Post Setup release" step not present yet
        if (callCount === 1) {
          return Promise.resolve({
            data: {
              jobs: [
                {
                  id: 12345,
                  name: 'test-job',
                  status: 'in_progress',
                  conclusion: null,
                  steps: [
                    { name: 'Checkout', status: 'completed', conclusion: 'success' },
                    { name: 'Build', status: 'in_progress', conclusion: null },
                  ],
                },
              ],
            },
          });
        }
        // Second call: "Post Setup release" appears, all steps before it completed
        return Promise.resolve({
          data: {
            jobs: [
              {
                id: 12345,
                name: 'test-job',
                status: 'completed',
                conclusion: 'success',
                steps: [
                  { name: 'Checkout', status: 'completed', conclusion: 'success' },
                  { name: 'Build', status: 'completed', conclusion: 'success' },
                  { name: 'Post Setup release', status: 'in_progress', conclusion: null },
                ],
              },
            ],
          },
        });
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
        baseDelayMs: 10,  // 10ms base delay for fast testing
        maxDelayMs: 50,   // 50ms max delay for fast testing
      });

      // Should have retried at least twice
      expect(octokitStub.rest.actions.listJobsForWorkflowRun.callCount).to.be.at.least(2);
      expect(coreStub.info.calledWith(sinon.match(/Cleanup step \(Post\*\) not ready or previous steps still running, waiting/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.true;
    });
  });

  describe('last step validation', () => {
    it('should push tag when last step is in_progress and all other steps succeeded', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'in_progress',
              conclusion: null,
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.true;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'])).to.be.false;
    });

    it('should push tag when last step has conclusion success', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.true;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'])).to.be.false;
    });

    it('should fail when last step has failed status', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'failure',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'failure' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should fail when last step has cancelled status', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'cancelled',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'cancelled' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should fail when last step has skipped status', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'skipped' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should delete tag when last step is valid but other steps failed', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'in_progress',
              conclusion: null,
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'failure' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should log error when last step status is invalid', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'failure' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should fail when last step has completed status but null conclusion', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: null,
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: null },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });

    it('should fail when last step has in_progress status but failure conclusion', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'in_progress',
              conclusion: null,
              steps: [
                { name: 'Checkout', status: 'completed', conclusion: 'success' },
                { name: 'Build', status: 'completed', conclusion: 'failure' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.error.calledWith(sinon.match(/Not all steps succeeded.*failed/))).to.be.true;
      expect(execStub.exec.calledWith('git', ['push', 'origin', 'framework@v1.2.3'], { silent: false })).to.be.false;
      expect(execStub.exec.calledWith('git', ['tag', '-d', 'framework@v1.2.3'], { silent: false, ignoreReturnCode: true })).to.be.true;
    });
  });

  describe('step logging', () => {
    it('should log steps on every polling attempt', async function() {
      this.timeout(5000);

      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      let callCount = 0;
      octokitStub.rest.actions.listJobsForWorkflowRun.callsFake(() => {
        callCount++;
        // First two calls: "Post Setup release" not present yet
        if (callCount <= 2) {
          return Promise.resolve({
            data: {
              jobs: [
                {
                  id: 12345,
                  name: 'test-job',
                  status: 'in_progress',
                  conclusion: null,
                  steps: [
                    { name: 'Checkout', status: 'completed', conclusion: 'success' },
                    { name: 'Build', status: 'in_progress', conclusion: null },
                  ],
                },
              ],
            },
          });
        }
        // Third call: "Post Setup release" appears, all steps before it completed
        return Promise.resolve({
          data: {
            jobs: [
              {
                id: 12345,
                name: 'test-job',
                status: 'completed',
                conclusion: 'success',
                steps: [
                  { name: 'Checkout', status: 'completed', conclusion: 'success' },
                  { name: 'Build', status: 'completed', conclusion: 'success' },
                  { name: 'Post Setup release', status: 'in_progress', conclusion: null },
                ],
              },
            ],
          },
        });
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
        baseDelayMs: 10,
        maxDelayMs: 50,
      });

      // Should have retried at least twice
      expect(octokitStub.rest.actions.listJobsForWorkflowRun.callCount).to.be.at.least(2);

      // Should only log steps on the final successful attempt (attempt 3)
      expect(coreStub.info.calledWith(sinon.match(/Attempt 3: Found 3 steps:/))).to.be.true;

      // Should log individual step details only on final attempt
      expect(coreStub.info.calledWith(sinon.match(/Step 1: name="Checkout"/))).to.be.true;
      expect(coreStub.info.calledWith(sinon.match(/Step 2: name="Build"/))).to.be.true;
      expect(coreStub.info.calledWith(sinon.match(/Step 3: name="Post Setup release"/))).to.be.true;
    });

    it('should log step status and conclusion', async () => {
      coreStub.getState.withArgs('next-release-tag').returns('framework@v1.2.3');
      coreStub.getState.withArgs('dry-run').returns('false');
      coreStub.getState.withArgs('github-token').returns('test-token');
      coreStub.getState.withArgs('job-name').returns('test-job');

      octokitStub.rest.actions.listJobsForWorkflowRun.resolves({
        data: {
          jobs: [
            {
              id: 12345,
              name: 'test-job',
              status: 'completed',
              conclusion: 'success',
              steps: [
                { name: 'Step1', status: 'completed', conclusion: 'success' },
                { name: 'Step2', status: 'completed', conclusion: 'success' },
                { name: 'Post Setup release', status: 'in_progress', conclusion: null },
              ],
            },
          ],
        },
      });

      const env = {
        GITHUB_TOKEN: 'test-token',
        GITHUB_RUN_ID: '12345',
        GITHUB_REPOSITORY: 'owner/repo',
      };

      await cleanup({
        core: coreStub,
        exec: execStub,
        github: githubStub,
        env,
      });

      expect(coreStub.info.calledWith('  Step 1: name="Step1", status=completed, conclusion=success')).to.be.true;
      expect(coreStub.info.calledWith('  Step 2: name="Step2", status=completed, conclusion=success')).to.be.true;
      expect(coreStub.info.calledWith('  Step 3: name="Post Setup release", status=in_progress, conclusion=none')).to.be.true;
    });
  });
});
