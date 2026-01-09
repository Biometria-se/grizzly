import { expect } from 'chai';
import { ConsoleLogOutputChannel } from '../../src/log';
import { mockChannels } from './setup';

describe('ConsoleLogOutputChannel', () => {
    let logger: ConsoleLogOutputChannel;
    let consoleStubs: {
        log: typeof console.log;
        trace: typeof console.trace;
        debug: typeof console.debug;
        info: typeof console.info;
        warn: typeof console.warn;
        error: typeof console.error;
    };

    beforeEach(() => {
        mockChannels.clear();

        // Stub console methods to prevent output during tests
        consoleStubs = {
            log: console.log,
            trace: console.trace,
            debug: console.debug,
            info: console.info,
            warn: console.warn,
            error: console.error
        };

        console.log = () => {};
        console.trace = () => {};
        console.debug = () => {};
        console.info = () => {};
        console.warn = () => {};
        console.error = () => {};
    });

    afterEach(() => {
        if (logger) {
            logger.channel.dispose();
        }

        // Restore console methods
        console.log = consoleStubs.log;
        console.trace = consoleStubs.trace;
        console.debug = consoleStubs.debug;
        console.info = consoleStubs.info;
        console.warn = consoleStubs.warn;
        console.error = consoleStubs.error;
    });

    it('should create an output channel with the given name', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(logger.channel).to.exist;
        expect(logger.channel.name).to.equal('TestChannel');
    });

    it('should show the channel on creation', () => {
        logger = new ConsoleLogOutputChannel('TestChannel2', { log: true });

        expect(logger.channel).to.exist;
    });

    it('should have trace method', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(() => logger.trace('trace message')).to.not.throw();
        expect(() => logger.trace('trace with args', 'arg1', 'arg2')).to.not.throw();
    });

    it('should have debug method', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(() => logger.debug('debug message')).to.not.throw();
        expect(() => logger.debug('debug with args', { key: 'value' })).to.not.throw();
    });

    it('should have info method', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(() => logger.info('info message')).to.not.throw();
        expect(() => logger.info('info with args', 123, true)).to.not.throw();
    });

    it('should have warn method', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(() => logger.warn('warning message')).to.not.throw();
        expect(() => logger.warn('warning with args', ['array'])).to.not.throw();
    });

    it('should have error method that accepts strings', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(() => logger.error('error message')).to.not.throw();
        expect(() => logger.error('error with args', 'extra')).to.not.throw();
    });

    it('should have error method that accepts Error objects', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });
        const error = new Error('Test error');

        expect(() => logger.error(error)).to.not.throw();
        expect(() => logger.error(error, 'context')).to.not.throw();
    });

    it('should expose the underlying channel', () => {
        logger = new ConsoleLogOutputChannel('TestChannel', { log: true });

        expect(logger.channel).to.be.an('object');
        expect(logger.channel).to.have.property('trace');
        expect(logger.channel).to.have.property('debug');
        expect(logger.channel).to.have.property('info');
        expect(logger.channel).to.have.property('warn');
        expect(logger.channel).to.have.property('error');
    });
});
