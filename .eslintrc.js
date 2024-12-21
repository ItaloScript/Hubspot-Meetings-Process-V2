const config = {
  env: {
    browser: true,
    commonjs: true,
    es6: true,
    node: true,
    mocha: true
  },
  extends: 'standard',
  globals: {
    Atomics: 'readonly',
    SharedArrayBuffer: 'readonly',
    _logger: 'readonly'
  },
  parser: '@babel/eslint-parser',
  parserOptions: {
    ecmaVersion: 11,
    requireConfigFile: false
  },
  plugins: ['eslint-plugin-prefer-arrow'],
  rules: {
    semi: [2, 'always'],
    'no-warning-comments': [0, { terms: ['todo', 'fixme', 'xxx', 'debug'], location: 'start' }],
    'prefer-arrow/prefer-arrow-functions': [2, { singleReturnOnly: true, disallowPrototype: true }],
    'object-curly-newline': ['error', { multiline: true }],
    'arrow-parens': [2, 'as-needed'],
    'arrow-body-style': [2, 'as-needed'],
    'operator-linebreak': [2, 'after'],
    indent: ['error', 2, { ignoredNodes: ['TemplateLiteral > *'], SwitchCase: 1 }],
    'no-unused-expressions': 0
  }
};


export default config;