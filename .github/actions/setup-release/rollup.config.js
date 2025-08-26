import commonjs from '@rollup/plugin-commonjs';
import { nodeResolve } from '@rollup/plugin-node-resolve';

export default {
    input: 'src/index.js',
    output: {
        esModule: true,
        file: 'dist/index.js',
        format: 'es',
        sourcemap: false,
    },
    plugins: [
        commonjs(),
        nodeResolve({ preferBuiltins: true }),
    ],
    onwarn(warning, warn) {
        // suppress eval warnings
        if (warning.code === 'EVAL') return
        if (warning.code === 'CIRCULAR_DEPENDENCY' && warning.message.includes('@actions/core')) return
        if (warning.code === 'CIRCULAR_DEPENDENCY' && warning.message.includes('/semver/')) return
        warn(warning)
    }
};
