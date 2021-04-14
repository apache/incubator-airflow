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
import d3 from 'd3';

const height = 700;
const width = document.getElementById('div_svg').offsetWidth;
const points = 20;
const matrix = [];
const duration = 2000;
let i = 0;
let flip = 0;
const colors = [
  '#FF5A5F', '#007A87', '#7B0051', '#00D1C1', '#8CE071', '#FFB400',
  '#FFAA91', '#B4A76C', '#9CA299', '#565A5C',
];

function choose(choices) {
  const index = Math.floor(Math.random() * choices.length);
  return choices[index];
}
// Making a matrix
for (let x = 0; x < points; x += 1) {
  for (let y = 0; y < points; y += 1) {
    matrix[i] = {
      x,
      y,
    };
    i += 1;
  }
}
const sclx = d3.scale.linear().domain([-1, points]).range([0, width]);
const scly = d3.scale.linear().domain([-1, points]).range([0, height]);

const circles = d3.select('#circles-svg')
  .attr('width', '100%')

  .attr('height', height)
  .selectAll('circle')
  .data(matrix)
  .enter()
  .append('circle')
  .attr('stroke', 'black')
  .attr('fill', 'none')
  .attr('cx', (d) => sclx(d.x))
  .attr('cy', (d) => scly(d.y))
  .attr('r', () => 0);

function toggle() {
  const size = 50 + (200 * Math.random());
  let yDelay = 0;
  let randomDelay = 0;
  if (Math.random() > 0.7) {
    yDelay = 50 + (Math.random() * 50);
  }
  let xDelay = 0;
  if (Math.random() > 0.7) {
    xDelay = 50 + (Math.random() * 50);
  }
  if (Math.random() > 0.7) {
    randomDelay = 1;
  }

  const randomX = Math.random() * width;
  const randomY = Math.random() * height;
  let col;
  if (Math.random() > 0.5) {
    col = choose(colors);
  } else {
    col = 'black';
  }
  if (Math.random() > 0.8) {
    col = choose(colors);
  }

  if (flip === 0) {
    flip = 1;

    circles.transition()
      .duration(duration)
      .attr('cx', (d) => sclx(d.x))
      .attr('cy', (d) => scly(d.y))
      .attr('stroke', col)
      .delay((d) => (randomDelay * Math.random() * 1000) + (d.x * xDelay) + (d.y * yDelay))
      .attr('r', () => size);
  } else {
    flip = 0;

    if (Math.random() > 0.6) {
      circles.transition()
        .duration(duration)
        .attr('r', () => 0)
        .attr('cx', () => randomX)
        .attr('stroke', col)
        .delay((d, j) => (j / 2) * Math.random() * 5)
        .attr('cy', () => randomY);
    } else {
      circles.transition()
        .duration(duration)
        .attr('cx', (d) => sclx(d.x))
        .attr('cy', (d) => scly(d.y))
        .attr('stroke', col)
        .delay((d) => (randomDelay * Math.random() * 1000) + (d.x * xDelay) + (d.y * yDelay))
        .attr('r', () => 0);
    }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  setInterval(toggle, duration * 3);
  toggle();
});
