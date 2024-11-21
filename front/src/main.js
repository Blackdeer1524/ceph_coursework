import './style.css'
import { Rect, Line } from 'fabric';
import { mapCanvas } from './mapCanvas';

import { Node } from './connection';

const rect = new Rect({
  top: 100,
  left: 100,
  width: 60,
  height: 70,
  fill: 'red',
});
mapCanvas.add(rect);
mapCanvas.add(new Line([50, 100, 200, 200], {
  left: 170,
  top: 150,
  stroke: 'red'
}));

new Node(110, 0, mapCanvas)


// https://stackoverflow.com/a/35453052
mapCanvas.renderAll();
mapCanvas.forEachObject(function (object) {
  object.selectable = false;
});
// END
