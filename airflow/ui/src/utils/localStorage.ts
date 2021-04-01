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

export function set(key: string, value:string): Record<string, unknown> {
  localStorage[key] = value;

  // Set session expiry (24hrs)
  if (key === 'token') {
    const date = new Date();
    localStorage[`${key}-expire`] = new Date(date.getTime() + 86400000);
  }

  return localStorage[key];
}

export function get(key: string, defaultValue = undefined): string {
  const value = localStorage[key] || defaultValue;
  return value;
}

export function clear(): void {
  return localStorage.clear();
}

export function remove(key: string): void {
  return localStorage.removeItem(key);
}

export function checkExpire(key: string): boolean {
  const sessExpire = get(`${key}-expire`);
  const sess = get(key);
  if (!sessExpire || !sess) return true;
  return new Date() > new Date(sessExpire);
}

export function clearAuth(): void {
  localStorage.removeItem('token');
  localStorage.removeItem('token-expire');
}
