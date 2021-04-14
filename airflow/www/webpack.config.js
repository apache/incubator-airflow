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

const webpack = require('webpack');
const path = require('path');
const ManifestPlugin = require('webpack-manifest-plugin');
const cwplg = require('clean-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const MomentLocalesPlugin = require('moment-locales-webpack-plugin');
const OptimizeCSSAssetsPlugin = require('optimize-css-assets-webpack-plugin');

// Input Directory (airflow/www)
// noinspection JSUnresolvedVariable
const CSS_DIR = path.resolve(__dirname, './static/css');
const JS_DIR = path.resolve(__dirname, './static/js');

// Output Directory (airflow/www/static/dist)
// noinspection JSUnresolvedVariable
const BUILD_DIR = path.resolve(__dirname, './static/dist');

const config = {
  entry: {
    airflowDefaultTheme: `${CSS_DIR}/bootstrap-theme.css`,
    connectionForm: `${JS_DIR}/connection_form.js`,
    dagCode: `${JS_DIR}/dag_code.js`,
    dags: [`${CSS_DIR}/dags.css`, `${JS_DIR}/dags.js`],
    flash: `${CSS_DIR}/flash.css`,
    gantt: [`${CSS_DIR}/gantt.css`, `${JS_DIR}/gantt.js`],
    graph: `${CSS_DIR}/graph.css`,
    ie: `${JS_DIR}/ie.js`,
    loadingDots: `${CSS_DIR}/loading-dots.css`,
    main: [`${CSS_DIR}/main.css`, `${JS_DIR}/main.js`],
    materialIcons: `${CSS_DIR}/material-icons.css`,
    moment: 'moment-timezone',
    switch: `${CSS_DIR}/switch.css`,
    taskInstances: `${JS_DIR}/task_instances.js`,
    taskInstance: `${JS_DIR}/task_instance.js`,
    tree: [`${CSS_DIR}/tree.css`, `${JS_DIR}/tree.js`],
    circles: `${JS_DIR}/circles.js`,
    durationChart: `${JS_DIR}/duration_chart.js`,
    trigger: `${JS_DIR}/trigger.js`,
    variableEdit: `${JS_DIR}/variable_edit.js`,
  },
  output: {
    path: BUILD_DIR,
    filename: '[name].[chunkhash].js',
    chunkFilename: '[name].[chunkhash].js',
    library: ['Airflow', '[name]'],
    libraryTarget: 'umd',
  },
  resolve: {
    extensions: [
      '.js',
      '.jsx',
      '.css',
    ],
  },
  module: {
    rules: [
      {
        test: /datatables\.net.*/,
        loader: 'imports-loader?define=>false',
      },
      {
        test: /\.jsx?$/,
        exclude: /node_modules/,
        loader: 'babel-loader',
      },
      // Extract css files
      {
        test: /\.css$/,
        include: CSS_DIR,
        use: [
          {
            loader: MiniCssExtractPlugin.loader,
            options: {
              esModule: true,
            },
          },
          'css-loader',
        ],
      },
      /* for css linking images */
      {
        test: /\.(png|jpg|gif)$/i,
        use: [
          {
            loader: 'url-loader',
            options: {
              limit: 100000,
            },
          },
        ],
      },
      /* for fonts */
      {
        test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        use: [
          {
            loader: 'url-loader',
            options: {
              limit: 100000,
              mimetype: 'application/font-woff',
            },
          },
        ],
      },
      {
        test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        loader: 'file-loader',
      },
    ],
  },
  plugins: [
    new ManifestPlugin(),
    new cwplg.CleanWebpackPlugin({
      verbose: true,
    }),
    new MiniCssExtractPlugin({
      filename: '[name].[chunkhash].css',
    }),

    // MomentJS loads all the locale, making it a huge JS file.
    // This will ignore the locales from momentJS
    new MomentLocalesPlugin(),

    new webpack.DefinePlugin({
      'process.env': {
        NODE_ENV: JSON.stringify(process.env.NODE_ENV),
      },
    }),
    // Since we have all the dependencies separated from hard-coded JS within HTML,
    // this seems like an efficient solution for now. Will update that once
    // we'll have the dependencies imported within the custom JS
    new CopyWebpackPlugin({
      patterns: [
        {
          from: 'node_modules/nvd3/build/*.min.*',
          flatten: true,
        },
        // Update this when upgrade d3 package, as the path in new D3 is different
        {
          from: 'node_modules/d3/d3.min.*',
          flatten: true,
        },
        {
          from: 'node_modules/dagre-d3/dist/*.min.*',
          flatten: true,
        },
        {
          from: 'node_modules/d3-tip/dist/index.js',
          to: 'd3-tip.js',
          flatten: true,
        },
        {
          from: 'node_modules/bootstrap-3-typeahead/*min.*',
          flatten: true,
        },
        {
          from: 'node_modules/datatables.net/**/**.min.*',
          flatten: true,
        },
        {
          from: 'node_modules/datatables.net-bs/**/**.min.*',
          flatten: true,
        },
        {
          from: 'node_modules/eonasdan-bootstrap-datetimepicker/build/css/bootstrap-datetimepicker.min.css',
          flatten: true,
        },
        {
          from: 'node_modules/eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min.js',
          flatten: true,
        },
        {
          from: 'node_modules/redoc/bundles/redoc.standalone.*',
          flatten: true,
        },
        {
          from: 'node_modules/codemirror/lib/codemirror.*',
          flatten: true,
        },
        {
          from: 'node_modules/codemirror/addon/lint/**.*',
          flatten: true,
        },
        {
          from: 'node_modules/codemirror/mode/javascript/javascript.js',
          flatten: true,
        },
        {
          from: 'node_modules/jshint/dist/jshint.js',
          flatten: true,
        },
      ],
    }),
  ],
  optimization: {
    minimizer: [new OptimizeCSSAssetsPlugin({})],
  },
};

module.exports = config;
