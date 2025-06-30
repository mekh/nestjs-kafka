import globals from 'globals';
import tsEslint from 'typescript-eslint';
import eslintPluginImportX from 'eslint-plugin-import-x';
import stylisticTs from '@stylistic/eslint-plugin-ts';
import tsParser from '@typescript-eslint/parser';

export default [
  eslintPluginImportX.flatConfigs.recommended,
  eslintPluginImportX.flatConfigs.typescript,
  ...tsEslint.configs.strictTypeChecked,
  ...tsEslint.configs.stylisticTypeChecked,
  {
    files: ['**/*.{js,ts}'],
    languageOptions: {
      parserOptions: {
        tsconfigRootDir: import.meta.dirname,
        project: './tsconfig.lint.json',
      },
      parser: tsParser,
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: globals.node,
    },
    plugins: {
      '@stylistic/ts': stylisticTs,
    },
    rules: {
      quotes: ['error', 'single', { avoidEscape: true }],
      semi: ['error', 'always'],
      'no-return-assign': 'error',
      'no-trailing-spaces': 'error',
      'no-multiple-empty-lines': ['error', { max: 1 }],
      'comma-dangle': ['error', {
        arrays: 'always-multiline',
        objects: 'always-multiline',
        imports: 'always-multiline',
        exports: 'always-multiline',
        functions: 'only-multiline',
      }],
      'eol-last': ['error', 'always'],
      'max-len': [
        'error',
        {
          code: 80,
          ignorePattern: 'import\\s.+\\sfrom\\s.+;$',
          ignoreUrls: true,
        },
      ],
      '@stylistic/ts/lines-between-class-members': [
        'error',
        'always',
        { exceptAfterOverload: true },
      ],
      '@stylistic/ts/member-delimiter-style': [
        'error',
        {
          overrides: {
            interface: {
              multiline: {
                delimiter: 'semi',
                requireLast: true,
              },
            },
          },
        },
      ],
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          ignoreRestSiblings: true,
        },
      ],
      '@typescript-eslint/no-unnecessary-type-parameters': 'off',
      '@typescript-eslint/no-confusing-void-expression': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-floating-promises': 'off',
      '@typescript-eslint/require-await': 'off',
      '@typescript-eslint/no-useless-constructor': 'off',
      '@typescript-eslint/restrict-template-expressions': 'off',
      '@typescript-eslint/no-extraneous-class': 'off',
      '@typescript-eslint/interface-name-prefix': 'off',
      '@typescript-eslint/explicit-module-boundary-types': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-unsafe-function-type': 'off',
      '@typescript-eslint/explicit-function-return-type': 'error',
      '@typescript-eslint/ban-ts-comment': 'error',
      '@typescript-eslint/use-unknown-in-catch-callback-variable': 'off',
      'object-curly-newline': 'off',
      'class-methods-use-this': 'off',
      'arrow-parens': 'off',
      'no-unused-vars': 'off',
      'import/prefer-default-export': 'off',
      'import-x/no-cycle': 'error',
      'import-x/no-dynamic-require': 'warn',
      'import-x/no-nodejs-modules': 'off',
    },
  }
];
