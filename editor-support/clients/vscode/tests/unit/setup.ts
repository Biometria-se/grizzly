/**
 * Setup file for unit tests - provides vscode module mock
 */
/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/no-explicit-any */
import * as Module from 'module';

class Disposable {
    constructor(private callback?: () => void) {}
    dispose() {
        if (this.callback) {
            this.callback();
        }
    }
}

class EventEmitter<T> {
    private listeners: Array<(e: T) => any> = [];

    get event() {
        return (listener: (e: T) => any) => {
            this.listeners.push(listener);
            return new Disposable(() => {
                const index = this.listeners.indexOf(listener);
                if (index > -1) {
                    this.listeners.splice(index, 1);
                }
            });
        };
    }

    fire(data: T) {
        this.listeners.forEach(listener => listener(data));
    }

    dispose() {
        this.listeners = [];
    }
}

class Position {
    constructor(public line: number, public character: number) {}
}

class Range {
    constructor(public start: Position, public end: Position) {}
}

class Uri {
    constructor(
        public scheme: string,
        public authority: string,
        public path: string,
        public query: string,
        public fragment: string
    ) {}

    static file(path: string): Uri {
        return new Uri('file', '', path, '', '');
    }

    static parse(value: string): Uri {
        return new Uri('file', '', value, '', '');
    }

    get fsPath(): string {
        return this.path;
    }

    toString(): string {
        return `${this.scheme}://${this.path}`;
    }

    toJSON() {
        return {
            scheme: this.scheme,
            authority: this.authority,
            path: this.path,
            query: this.query,
            fragment: this.fragment
        };
    }

    with(change: any): Uri {
        return new Uri(
            change.scheme ?? this.scheme,
            change.authority ?? this.authority,
            change.path ?? this.path,
            change.query ?? this.query,
            change.fragment ?? this.fragment
        );
    }
}

const mockChannels = new Map<string, any>();

const vscode = {
    Disposable,
    EventEmitter,
    Position,
    Range,
    Uri,

    window: {
        createOutputChannel: (name: string, options?: { log: boolean }) => {
            if (mockChannels.has(name)) {
                return mockChannels.get(name);
            }

            const channel = {
                name,
                append: (value: string) => {},
                appendLine: (value: string) => {},
                clear: () => {},
                show: () => {},
                hide: () => {},
                dispose: () => { mockChannels.delete(name); },
                trace: (message: string, ...args: any[]) => {},
                debug: (message: string, ...args: any[]) => {},
                info: (message: string, ...args: any[]) => {},
                warn: (message: string, ...args: any[]) => {},
                error: (error: string | Error, ...args: any[]) => {}
            };

            mockChannels.set(name, channel);
            return channel;
        },
        showInformationMessage: async (message: string) => message,
        showWarningMessage: async (message: string) => message,
        showErrorMessage: async (message: string) => message,
        activeTextEditor: undefined,
        activeColorTheme: { kind: 1 },
        createWebviewPanel: (viewType: string, title: string, showOptions: any, options?: any) => {
            const panel = {
                webview: {
                    html: '',
                    options: options || {},
                    asWebviewUri: (uri: any) => uri,
                    postMessage: async (message: any) => true,
                    onDidReceiveMessage: new EventEmitter<any>().event
                },
                title,
                viewType,
                iconPath: undefined,
                options,
                visible: true,
                active: true,
                viewColumn: showOptions.viewColumn || 1,
                reveal: (viewColumn?: number, preserveFocus?: boolean) => {
                    panel.visible = true;
                    panel.active = !preserveFocus;
                },
                dispose: () => {
                    panel.visible = false;
                    onDidDisposeEmitter.fire();
                },
                onDidDispose: new EventEmitter<void>().event,
                onDidChangeViewState: new EventEmitter<any>().event
            };

            const onDidDisposeEmitter = new EventEmitter<void>();
            panel.onDidDispose = onDidDisposeEmitter.event;

            return panel;
        }
    },

    workspace: {
        getConfiguration: (section?: string, scope?: any) => ({
            get: <T>(key: string, defaultValue?: T): T | undefined => defaultValue,
            has: (key: string) => false,
            inspect: (key: string) => undefined,
            update: async () => {}
        }),
        onDidChangeConfiguration: () => new Disposable(),
        onDidOpenTextDocument: () => new Disposable(),
        onDidCloseTextDocument: () => new Disposable(),
        onDidSaveTextDocument: () => new Disposable(),
        onDidChangeTextDocument: () => new Disposable(),
        textDocuments: [],
        applyEdit: async () => true
    },

    commands: {
        registerCommand: (command: string, callback: (...args: any[]) => any) => new Disposable(),
        executeCommand: async (command: string, ...args: any[]) => {}
    },

    ColorThemeKind: {
        Light: 1,
        Dark: 2,
        HighContrast: 3,
        HighContrastLight: 4
    },

    ViewColumn: {
        Active: -1,
        Beside: -2,
        One: 1,
        Two: 2,
        Three: 3
    }
};

// Mock the vscode module
const originalRequire = Module.prototype.require;
Module.prototype.require = function (id: string, ...args: any[]) {
    if (id === 'vscode') {
        return vscode;
    }
    return originalRequire.apply(this, [id, ...args]);
} as any;

// Export for tests to use
export { vscode, mockChannels };
export default vscode;
