import { Circle, Textbox, Group } from "fabric";
import { PrimaryRegistry } from "./connection";

class Blob {
  static radius = 10;

  constructor(objId, left, top) {
    let c = new Circle({
      left: left - Blob.radius,
      top: top - Blob.radius,
      radius: Blob.radius,
      fill: "blue",
    });
    let txt = new Textbox(`${objId}`, {
      left: left - Blob.radius,
      top: top - Blob.radius,
      width: Blob.radius * 2,
      fontSize: (Blob.radius * 3) / 2,
      fontWeight: "bold",
      fill: "white",
      textAlign: "center",
    });

    this.g = new Group([c, txt], {});
  }
}

/**
 *
 * @param {number} objId
 * @param {Line[]} path
 * @param {Canvas} canvas
 */
function animatePath(objId, path, canvas, callback) {
  function draw(path, i) {
    if (i >= path.length) {
      callback();
      return;
    }
    let l = path[i];
    const startX = l.x1;
    const startY = l.y1;
    const endX = l.x2;
    const endY = l.y2;
    let lineLength = ((endX - startX) ** 2 + (endY - startY) ** 2) ** (1 / 2);

    let blob = new Blob(objId, startX, startY);
    canvas.add(blob.g);

    let done = false;
    blob.g.animate(
      { left: endX - Blob.radius, top: endY - Blob.radius },
      {
        duration: Math.max(lineLength * 5, 300),
        onChange: canvas.renderAll.bind(canvas),
        onComplete: () => {
          if (done) {
            return; // onComplete is somehow called multiple times.
          }
          done = true;
          canvas.remove(blob.g);
          draw(path, i + 1);
        },
      },
    );
  }
  draw(path, 0);
}

/**
 * @param {Bucket} b
 */
function animateBucketPath(objId, b, finalCallback) {
  if (b === null || b.parent === null) {
    return;
  }
  let s = [];
  while (b.parent !== null) {
    let path = b.parent.connectors.get(b.name);
    s.push(path);
    b = b.parent;
  }

  let cur = s.length - 1;
  function callback() {
    --cur;
    if (cur < 0) {
      finalCallback();
      return;
    }
    animatePath(objId, s[cur], b.canvas, callback);
  }

  animatePath(objId, s[s.length - 1], b.canvas, callback);
}

/**
 *
 * @param {number} objId
 * @param {number} pgId
 * @param {PrimaryRegistry} registry
 */
export function animateSendItem(objId, pgId, registry) {
  let pg = registry.get(pgId);
  let b = pg.osd.bucket;
  animateBucketPath(objId, b, () => {
    animatePath(objId, pg.pathToBucket, pg.canvas, () => {});
  });
}

/**
 *
 * @param {number} objId
 * @param {number} pgId
 * @param {string} osd
 * @param {PrimaryRegistry} registry
 */
export function animateSendSuccess(objId, pgId, osdName, registry) {}
