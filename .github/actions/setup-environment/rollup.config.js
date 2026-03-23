import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default {
  input: 'src/index.js',
  output: {
    file: 'dist/index.js',
    format: 'es'
  },
  plugins: [
    resolve(),
    commonjs()
  ],
  onwarn(warning, warn) {
    // suppress eval warnings
    if (warning.code === 'CIRCULAR_DEPENDENCY' && warning.message.includes('@actions/core')) return
    // suppress this is undefined warning from @actions/github
    if (warning.code === 'THIS_IS_UNDEFINED') return
    warn(warning)
  }
};
