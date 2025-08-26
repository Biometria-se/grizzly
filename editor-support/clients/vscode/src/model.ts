/**
 * Interface for tracking the activation status of the VS Code extension.
 * Provides methods to check and update the extension's activation state.
 */
export interface ExtensionStatus {
    /** Checks if the extension is currently activated. */
    isActivated: () => boolean;
    /** Sets the activation status of the extension. */
    setActivated: (status?: boolean) => void;
}

/**
 * Configuration for stdio-based language server communication.
 * Defines the executable and arguments needed to start the language server.
 */
interface SettingsStdio {
    /** Path to the language server executable. */
    executable: string;
    /** Command-line arguments to pass to the language server. */
    args: string[];
}

/**
 * Configuration for socket-based language server communication.
 * Defines the network endpoint for connecting to the language server.
 */
interface SettingsSocket {
    /** Hostname or IP address of the language server. */
    host: string;
    /** Port number where the language server is listening. */
    port: number;
}

/**
 * Type representing the available language server connection methods.
 * Can be either 'socket' for network-based communication or 'stdio' for process-based communication.
 */
type SettingsServerConnection = 'socket' | 'stdio';

/**
 * Configuration for language server connection settings.
 * Specifies which connection method should be used.
 */
interface SettingsServer {
    /** The type of connection to use for the language server. */
    connection: SettingsServerConnection;
}

/**
 * Complete configuration settings for the Grizzly language server extension.
 * Combines server connection settings, runtime configuration, and feature flags.
 */
export interface Settings {
    /** Language server connection configuration. */
    server: SettingsServer;
    /** Stdio connection settings (used when server.connection is 'stdio'). */
    stdio: SettingsStdio;
    /** Socket connection settings (used when server.connection is 'socket'). */
    socket: SettingsSocket;
    /** Patterns for identifying variables in Grizzly feature files. */
    variable_pattern: string[];
    /** Extra index URL for pip package installation. */
    pip_extra_index_url: string;
    /** Whether to use virtual environment for Python execution. */
    use_virtual_environment: boolean;
    /** When true, diagnostics are only run on file save rather than on every change. */
    diagnostics_on_save_only: boolean;
}
