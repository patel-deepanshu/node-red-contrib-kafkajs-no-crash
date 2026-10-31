'use strict';

const js = require('@eslint/js');
const globals = require('globals');

module.exports = [
    {
        ignores: ['node_modules/**', 'coverage/**', '.kafka-local/**', 'test/.tmp/**']
    },
    js.configs.recommended,
    {
        files: ['**/*.js'],
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: 'commonjs',
            globals: { ...globals.node }
        },
        rules: {
            'no-unused-vars': ['error', { args: 'none', caughtErrors: 'none' }],
            'no-var': 'error',
            'prefer-const': 'error',
            eqeqeq: ['error', 'always', { null: 'ignore' }],
            'no-console': 'error'
        }
    },
    {
        files: ['test/**/*.js'],
        languageOptions: {
            globals: { ...globals.node, ...globals.mocha }
        },
        rules: {
            'no-console': 'off'
        }
    }
];
