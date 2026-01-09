import * as path from 'path';
import * as Mocha from 'mocha';
import { globSync } from 'glob';

export function run(): Promise<void> {
    // Create the mocha test
    const mocha = new Mocha({
        ui: 'tdd',
        color: true,
    });

    switch (process.platform) {
        case 'darwin':
            mocha.timeout(420000);
            break;
        default:
            mocha.timeout(300000); // 5 minutes
            break;
    }

    const testsRoot = __dirname;

    return new Promise((resolve, reject) => {
        const tests = process.env['TESTS']?.split(',').map((test) => path.parse(test.replace('.ts', '.js')).base);
        const files = globSync('**.test.js', { cwd: testsRoot });

        // Add files to the test suite
        files.forEach((f: string) => {
            if (tests === undefined || tests.includes(f)) {
                mocha.addFile(path.resolve(testsRoot, f));
            }
        });

        mocha.slow(300);

        try {
            // Run the mocha test
            mocha.run((failures) => {
                if (failures > 0) {
                    reject(new Error(`${failures} tests failed.`));
                } else {
                    resolve();
                }
            });
        } catch (err) {
            console.error(err);
            reject(err);
        }
    });
}
