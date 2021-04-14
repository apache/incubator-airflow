<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Contributing to the UI

## Learn

If you're new to modern frontend development or parts of our stack, you may want to check out these resources to understand our codebase:

- Typescript is an extension of javascript to add type-checking to our app. Files ending in `.ts` or `.tsx` will be type-checked. Check out the [handbook](https://www.typescriptlang.org/docs/handbook/typescript-in-5-minutes-func.html) for an introduction or feel free to keep this [cheatsheet](https://github.com/typescript-cheatsheets/react) open while developing.

- React powers our entire app so it would be valuable to learn JSX, the html-in-js templates React utilizes. Files that contain JSX will end in `.tsx` instead of `.ts`. Check out their official [tutorial](https://reactjs.org/tutorial/tutorial.html#overview) for a basic overview.

- Chakra-UI is our component library and theming system. You'll notice we have no traditional css nor html tags. This is all handled in Chakra with importing standard components like `<Box>` or `<Text>` that are styled globally in `src/theme.ts` file and then by passing styles as component props. Check out their [docs](https://chakra-ui.com/docs/getting-started) to see all the included components and hooks.

- Testing is done with React Testing Library. We follow their idea of "The more your tests resemble the way your software is used,
the more confidence they can give you." Keep their [cheatsheet](https://testing-library.com/docs/react-testing-library/cheatsheet) open when writing tests

- Neutrino handles our App's configuration and Webpack build. Check out their [docs](https://neutrinojs.org/api/) if you need to customize it.

## Project Structure

- `src/index.tsx` is the entry point of the app. Here you will find all the root level Providers that expose functionality to the rest of the app like the Chakra component library, routing, authentication or API queries.
- `.neutrinorc.js` is the main config file. Although some custom typescript or linting may need to be changed in `tsconfig.json` or `.eslintrc.js`, respectively

## Find open issues

Take a look at our [project board](https://github.com/apache/airflow/projects/9) for unassigned issues in the `Next Up` column. If you're interested in one, leave a comment saying you'd like it to be assigned to you.
