import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import json from '@rollup/plugin-json';

export default {
  input: 'src/index.js',
  output: {
    file: 'dist/index.js',
    format: 'es'
  },
  plugins: [
    json(),
    resolve(),
    commonjs()
  ],
  onwarn(warning, warn) {
    // suppress eval warnings
    if (warning.code === 'EVAL') return
    if (warning.code === 'CIRCULAR_DEPENDENCY' && warning.message.includes('@actions/core')) return
    warn(warning)
  }
};
