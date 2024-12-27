import { Circle, Textbox, Group } from "fabric";
import { OSD, PrimaryRegistry, RCPath } from "./connection";

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
    if (i >= path.points.length) {
      callback();
      return;
    }
    const startX = path.points[i - 1].x;
    const startY = path.points[i - 1].y;
    const endX = path.points[i].x;
    const endY = path.points[i].y;
    let lineLength = ((endX - startX) ** 2 + (endY - startY) ** 2) ** (1 / 2);

    let blob = new Blob(objId, startX, startY, "sending");
    canvas.add(blob.g);

    blob.g.animate(
      startX !== endX
        ? { left: endX - Blob.radius }
        : { top: endY - Blob.radius },
      {
        duration: lineLength * 3,
        onChange: canvas.renderAll.bind(canvas),
        onComplete: () => {
          canvas.remove(blob.g);
          draw(path, i + 1);
        },
      },
    );
  }
  draw(path, 1);
}

/**
 * @param {number} objId
 * @param {Bucket} b
 * @param {function():null} finalCallback
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
 * @param {number} objId
 * @param {number} pgId
 * @param {OSD} osd
 */
export function animateSendToPrimary(objId, pgId, osd, callback) {
  let mapPrimaryPG = osd.pgs.get(pgId);
  mapPrimaryPG.connectToBucket();
  animateBucketPath(objId, osd.bucket, () => {
    animatePath(objId, mapPrimaryPG.pathToBucket.path, osd.canvas, () => {
      callback();
      mapPrimaryPG.releaseBucketConnect();
    });
  });
}

/**
 * @param {number} objId
 * @param {number} pgId
 * @param {string[]} newMap
 * @param {Map<string, OSD>} name2osd
 * @param {Any} lock
 */
export function animateSendToReplicas(
  objId,
  pgId,
  newMap,
  name2osd,
  lock,
  callback,
) {
  if (newMap.length === 1) {
    callback();
    return;
  }
  let primaryOSD = name2osd.get(newMap[0]);
  for (let i = 1; i < newMap.length; ++i) {
    let secondaryOSD = name2osd.get(newMap[i]);
    let path = primaryOSD.connectTmp(secondaryOSD, pgId);
    lock.lock(`${objId} locked for sending`);
    animatePath(objId, path.path, primaryOSD.canvas, () => {
      lock.unlock(`${objId} was sent to replica`);
      path.down();
    });
  }
  callback();
}

/**
 * @param {number} objId
 * @param {number} pgId
 * @param {OSD} osd
 * @param {"successRecv" | "failRecv"} status
 */
export function animateSendStatus(objId, pgId, osd, status) {
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
