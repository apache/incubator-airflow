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

import React, { ReactElement } from 'react';

const PinwheelLogo = ({
  width = '40px',
  height = '40px',
  ...otherProps
}: React.SVGProps<SVGSVGElement>): ReactElement => (
  <svg width={width} height={height} viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" {...otherProps}>
    <path d="M0.861099 35.3873L17.7729 18.0515C17.8788 17.9429 17.8991 17.775 17.8108 17.6516C16.782 16.2156 14.8848 15.9667 14.1814 15.002C12.0981 12.1441 11.5695 10.5266 10.6743 10.6269C10.6117 10.6339 10.556 10.6676 10.512 10.7126L4.4026 16.9752C0.887961 20.5779 0.383943 28.5103 0.291509 35.1524C0.287333 35.4525 0.651482 35.6021 0.861099 35.3873Z" fill="#017cee" />
    <path d="M35.4734 34.9588L18.1375 18.047C18.0289 17.941 17.861 17.9207 17.7377 18.0091C16.3017 19.0378 16.0528 20.9351 15.088 21.6384C12.2302 23.7217 10.6126 24.2504 10.7129 25.1456C10.72 25.2082 10.7536 25.2639 10.7987 25.3077L17.0613 31.4172C20.664 34.9319 28.5964 35.4359 35.2385 35.5282C35.5386 35.5326 35.6882 35.1684 35.4734 34.9588Z" fill="#00ad46" />
    <path fillRule="evenodd" clipRule="evenodd" d="M17.0612 31.4173C15.0932 29.4975 14.1801 25.6994 17.953 17.8671C11.8213 20.6074 9.67257 24.2094 10.7296 25.2407L17.0612 31.4173Z" fill="#04d659" />
    <path d="M35.0445 0.346896L18.1327 17.6827C18.0268 17.7913 18.0065 17.9592 18.0948 18.0825C19.1236 19.5186 21.0209 19.7674 21.724 20.7322C23.8075 23.59 24.3362 25.2075 25.2313 25.1074C25.2938 25.1004 25.3496 25.0666 25.3936 25.0216L31.5029 18.759C35.0177 15.1562 35.5217 7.22392 35.6141 0.58175C35.6182 0.281597 35.2541 0.132024 35.0445 0.346896Z" fill="#00c7d4" />
    <path fillRule="evenodd" clipRule="evenodd" d="M31.5031 18.759C29.5832 20.7269 25.7851 21.6401 17.9528 17.8671C20.693 23.9988 24.2951 26.1477 25.3263 25.0905L31.5031 18.759Z" fill="#11e1ee" />
    <path d="M0.432658 0.775339L17.7685 17.6871C17.8771 17.793 18.045 17.8134 18.1683 17.725C19.6043 16.6963 19.8532 14.799 20.8179 14.0957C23.6759 12.0123 25.2934 11.4837 25.193 10.5885C25.186 10.526 25.1523 10.4702 25.1074 10.4263L18.8447 4.31685C15.242 0.802203 7.30967 0.298184 0.667512 0.205751C0.367359 0.201573 0.217786 0.565722 0.432658 0.775339Z" fill="#e43921" />
    <path fillRule="evenodd" clipRule="evenodd" d="M18.8446 4.31675C20.8125 6.23662 21.7257 10.0346 17.9528 17.8669C24.0844 15.1267 26.2333 11.5246 25.1761 10.4934L18.8446 4.31675Z" fill="#ff7557" />
    <path fillRule="evenodd" clipRule="evenodd" d="M4.4028 16.9752C6.32267 15.0072 10.1207 14.0942 17.953 17.867C15.2128 11.7354 11.6107 9.58661 10.5795 10.6437L4.4028 16.9752Z" fill="#0cb6ff" />
    <path d="M17.9649 18.6209C18.3825 18.6157 18.7169 18.273 18.7117 17.8553C18.7065 17.4377 18.3638 17.1034 17.9462 17.1085C17.5285 17.1137 17.1942 17.4564 17.1994 17.8741C17.2045 18.2917 17.5473 18.626 17.9649 18.6209Z" fill="#4a4848" />
  </svg>
);

export default PinwheelLogo;
