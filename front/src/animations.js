import { Circle, Textbox, Group } from "fabric";
import { OSD, PrimaryRegistry } from "./connection";

class Blob {
  static radius = 10;
  static status2color = {
    sending: "blue",
    failRecv: "red",
    successRecv: "green",
  };

  /**
   *
   * @param {number} objId
   * @param {number} centerX
   * @param {number} centerY
   * @param {"sending" | "failRecv", | "successRecv"} status
   */
  constructor(objId, centerX, centerY, status) {
    let c = new Circle({
      left: centerX - Blob.radius,
      top: centerY - Blob.radius,
      radius: Blob.radius,
      fill: Blob.status2color[status],

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
    let txt = new Textbox(`${objId}`, {
      left: centerX - Blob.radius,
      top: centerY - Blob.radius,
      width: Blob.radius * 2,
      fontSize: (Blob.radius * 3) / 2,
      fontWeight: "bold",
      fill: "white",
      textAlign: "center",
      
      
      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
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

    let blob = new Blob(objId, startX, startY, "sending");
    canvas.add(blob.g);

    let done = false;
    blob.g.animate(
      { left: endX - Blob.radius, top: endY - Blob.radius },
      {
        duration: Math.max(lineLength * 2, 300),
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
 * @param {Any} hierarchyRoot
 * @param {function} callback
 */
export function animateSendFailure(objId, hierarchyRoot, callback) {
  animateBlobFading(objId, hierarchyRoot, "failRecv", callback);
}

/**
 *
 * @param {number} objId
 * @param {number} pgId
 * @param {PrimaryRegistry} registry
 */
export function animateSendItem(objId, pgId, registry, callback) {
  let pg = registry.get(pgId);
  let b = pg.osd.bucket;
  animateBucketPath(objId, b, () => {
    animatePath(objId, pg.pathToBucket, pg.canvas, callback);
  });
}

/**
 *
 * @param {number} objId
 * @param {number} pgId
 * @param {PrimaryRegistry} registry
 */
export function animateSendToReplicas(objId, pgId, registry, callback) {
  let primary = registry.get(pgId);
  primary.connectors.forEach((path) =>
    animatePath(objId, path, primary.canvas, callback),
  );
}

/**
 *
 * @param {number} objId
 * @param {number} pgId
 * @param {string} osd
 * @param {Map<string, OSD>} name2osd
 * @param {"successRecv" | "failRecv"} status
 */
export function animateSendStatus(objId, pgId, osdName, name2osd, status) {
  let osd = name2osd.get(osdName);
  let target = osd.pgs.get(pgId);

  animateBlobFading(objId, target, status, () => {});
}

function animateBlobFading(objId, target, status, callback) {
  let b = new Blob(
    objId,
    target.drawnObj.left + target.drawnObj.width / 2,
    target.drawnObj.top + target.drawnObj.height / 2,
    status,
  );
  let canvas = target.canvas;
  canvas.add(b.g);

  b.g.animate(
    { opacity: 0 },
    {
      duration: 700,
      onChange: canvas.renderAll.bind(canvas),
      onComplete: function () {
        canvas.remove(b.g);
        callback();
      },
    },
  );
}
