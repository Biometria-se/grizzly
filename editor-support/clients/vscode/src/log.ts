/* eslint-disable @typescript-eslint/no-explicit-any */
import * as vscode from 'vscode';

/**
 * A wrapper around VS Code's LogOutputChannel that provides dual logging.
 *
 * Logs messages to both the VS Code output channel and the console, which is
 * useful for debugging and development. When VERBOSE environment variable is set,
 * trace and debug messages are also sent to the console.
 */
export class ConsoleLogOutputChannel {
    /** The VS Code output channel instance */
    public channel: vscode.LogOutputChannel;

    /**
     * Creates a new ConsoleLogOutputChannel.
     *
     * @param name - The name of the output channel
     * @param options - Channel options, must include {log: true}
     */
    public constructor(name: string, options: {log: true}) {
        this.channel = vscode.window.createOutputChannel(name, options);
        this.channel.show();
    }

    /**
     * Internal logging method that sends messages to both the output channel and console.
     *
     * @param callee - The log level method name (trace, debug, info, warn, error)
     * @param message - The message or error to log
     * @param args - Additional arguments to log
     */
    private log(callee: string, message: string | Error, ...args: any[]): void {
        // send log to output channel
        this.channel[callee].call(this.channel, message, ...args);

        if (process.env.VERBOSE) {
            switch (callee) {
                case 'trace':
                case 'debug':
                    callee = 'log';
                    break;
            }
        }

        // send log to console
        console[callee].call(console, message, ...args);
    }

    /**
     * Logs a trace message.
     *
     * @param message - The trace message
     * @param args - Additional arguments to log
     */
    public trace(message: string, ...args: any[]): void {
        this.log('trace', message, ...args);
    }

    /**
     * Logs a debug message.
     *
     * @param message - The debug message
     * @param args - Additional arguments to log
     */
    public debug(message: string, ...args: any[]): void {
        this.log('debug', message, ...args);
    }

    /**
     * Logs an informational message.
     *
     * @param message - The info message
     * @param args - Additional arguments to log
     */
    public info(message: string, ...args: any[]): void {
        this.log('info', message, ...args);
    }

    /**
     * Logs a warning message.
     *
     * @param message - The warning message
     * @param args - Additional arguments to log
     */
    public warn(message: string, ...args: any[]): void {
        this.log('warn', message, ...args);
    }

    /**
     * Logs an error message or error object.
     *
     * @param error - The error message or Error object
     * @param args - Additional arguments to log
     */
    public error(error: string | Error, ...args: any[]): void {
        this.log('error', error, ...args);
    }
}
