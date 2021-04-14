/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
  Config for running and building the app
*/
const typescript = require('neutrinojs-typescript');
const typescriptLint = require('neutrinojs-typescript-eslint');
const react = require('@neutrinojs/react');
const jest = require('@neutrinojs/jest');
const eslint = require('@neutrinojs/eslint');
const { resolve } = require('path');
const copy = require('@neutrinojs/copy');

module.exports = {
  options: {
    root: __dirname,
  },
  use: [
    (neutrino) => {
      // Aliases for internal modules
      neutrino.config.resolve.alias.set('root', resolve(__dirname));
      neutrino.config.resolve.alias.set('src', resolve(__dirname, 'src'));
    },
    typescript(),
    // Modify typescript config in .tsconfig.json
    typescriptLint(),
    eslint({
      eslint: {
        // Modify eslint config in .eslintrc.js config instead
        useEslintrc: true,
      },
    }),
    jest({
      moduleDirectories: ['node_modules', 'src'],
    }),
    react({
      html: {
        title: 'Apache Airflow',
      }
    }),
    copy({
      patterns: [
        { from: 'src/static/favicon.ico', to: '.' },
      ],
    }),
  ],
};
