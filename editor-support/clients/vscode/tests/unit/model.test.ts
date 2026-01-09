import { expect } from 'chai';
import { Settings, ExtensionStatus } from '../../src/model';

describe('model', () => {
    it('should export ExtensionStatus interface', () => {
        // Test that we can use ExtensionStatus type
        const status: ExtensionStatus = {
            isActivated: () => true,
            setActivated: () => { /* no-op */ }
        };
        expect(status.isActivated()).to.be.true;
        expect(() => status.setActivated(false)).to.not.throw();
    });

    it('should export Settings interface', () => {
        // Test that we can use Settings type
        const settings: Settings = {
            server: { connection: 'stdio' },
            stdio: { executable: 'python', args: [] },
            socket: { host: 'localhost', port: 4444 },
            variable_pattern: [],
            pip_extra_index_url: '',
            use_virtual_environment: true,
            diagnostics_on_save_only: false
        };
        expect(settings.server.connection).to.equal('stdio');
        expect(settings.stdio.executable).to.equal('python');
        expect(settings.socket.host).to.equal('localhost');
        expect(settings.socket.port).to.equal(4444);
    });

    it('should allow stdio connection type', () => {
        const settings: Settings = {
            server: { connection: 'stdio' },
            stdio: { executable: '/usr/bin/python3', args: ['--verbose'] },
            socket: { host: 'localhost', port: 4444 },
            variable_pattern: ['pattern1'],
            pip_extra_index_url: 'https://example.com',
            use_virtual_environment: false,
            diagnostics_on_save_only: true
        };
        expect(settings.server.connection).to.equal('stdio');
    });

    it('should allow socket connection type', () => {
        const settings: Settings = {
            server: { connection: 'socket' },
            stdio: { executable: 'python', args: [] },
            socket: { host: '127.0.0.1', port: 8080 },
            variable_pattern: [],
            pip_extra_index_url: '',
            use_virtual_environment: true,
            diagnostics_on_save_only: false
        };
        expect(settings.server.connection).to.equal('socket');
    });
});
