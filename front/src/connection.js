import { Canvas, Line, Rect } from "fabric";

export class Bucket {
  static width = 100;
  static height = 60;

  /**
   * @param {number} posX
   * @param {number} posY
   * @param {Canvas} canvas
   */
  constructor(posX, posY, canvas) {
    this.posX = posX;
    this.posY = posY;
    console.log("bucket", [posX, posY]);
    this.canvas = canvas;

    this.children = [];

    this.drawnObj = new Rect({
      top: posY,
      left: posX,
      width: Bucket.width,
      height: Bucket.height,
      fill: "green",
    });
    this.canvas.add(this.drawnObj);
  }

  /**
   * @param {Bucket} other
   */
  connectParentBucket(other) {
    this.children.push(other);
    const myMidpoint = this.posX + Bucket.width / 2;
    const otherMidpoint = other.posX + Bucket.width / 2;
    const spaceBetween = this.posY - (other.posY + Bucket.height);

    const start = new Line(
      [myMidpoint, this.posY, myMidpoint, this.posY - spaceBetween / 2],
      {
        stroke: "green",
      }
    );

    const middle = new Line(
      [
        myMidpoint,
        this.posY - spaceBetween / 2,
        otherMidpoint,
        this.posY - spaceBetween / 2,
      ],
      {
        stroke: "green",
      }
    );

    const end = new Line(
      [
        otherMidpoint,
        this.posY - spaceBetween / 2,
        otherMidpoint,
        this.posY - spaceBetween,
      ],
      {
        stroke: "green",
      }
    );

    this.canvas.add(start);
    this.canvas.add(middle);
    this.canvas.add(end);

    // this.childConnections.push(start);
    // this.childConnections.push(middle);
    // this.childConnections.push(end);
  }
}

const PGBoxHeight = 20;
const PGBoxGap = 3;

class ConnectionAllocator {
  /**
   * 
   * @param {number} max 
   */
  constructor(max) {
    this.is_allocated = [];
    for (i = 0; i < max; ++i) {
      this.is_allocated.push(false);
    }
  }
  
  alloc() {

  }
}

export class OSD {
  static width = 100;
  static initHeight = 60;

  constructor(posX, posY, canvas) {
    console.log("osd", [posX, posY]);
    this.posX = posX;
    this.posY = posY;
    this.canvas = canvas;

    this.nextOSD = null;
    this.pgs = new Map();

    this.drawnObj = new Rect({
      top: posY,
      left: posX,
      width: OSD.width,
      height: OSD.initHeight,
      fill: "blue",
    });
    this.canvas.add(this.drawnObj);
  }

  connect(other, pg_id, connectAllocInfo) {}

  /**
   *
   * @param {OSD} other
   */
  addNext(other) {
    this.nextOSD = other;
  }

  /**
   * @param {number} id
   */
  addPG(id) {
    let top = this.posY + PGBoxGap;
    if (this.pgs.length > 0) {
      top = this.pgs[this.pgs.length - 1].top + PGBoxHeight + PGBoxGap;
      prin;
    }

    let newPgBox = new Rect({
      top: top,
      left: this.posX + PGBoxGap,
      width: OSD.width - 2 * PGBoxGap,
      height: PGBoxHeight,
      fill: "pink",
    });
    this.pgs.push(newPgBox);
    this.redraw(this.posY);
  }

  /**
   * @param {number} newY
   */
  redraw(newY) {
    this.canvas.remove(this.drawnObj);
    this.pgs.forEach((c) => {
      this.canvas.remove(c);
      c.top += newY - this.posY;
    });
    const newHeight = Math.max(
      OSD.initHeight,
      this.pgs.length * PGBoxHeight + (this.pgs.length + 1) * 3
    );
    this.posY = newY;
    this.drawnObj = new Rect({
      top: this.posY,
      left: this.posX,
      width: OSD.width,
      height: newHeight,
      fill: "blue",
    });
    this.canvas.add(this.drawnObj);
    this.pgs.forEach((c) => {
      this.canvas.add(c);
    });

    this.nextOSD?.redraw(this.posY + newHeight + STEP_Y_BETWEEN);
  }
}

/**
 * @typedef {Object} OSDDesc
 * @property {string} name
 * @property {"osd"} type
 */

/**
 * @typedef {Object} BucketDesc
 * @property {string} name
 * @property {"bucket"} type
 * @property {number} children_width
 * @property {BucketDesc[] | OSDDesc[]} children
 */

const SPACE_BETWEEN_OSD_COLS = 60;
const STEP_Y_BETWEEN = 40;

/**
 * @param {BucketDesc} root
 * @returns {number}
 */
function determineWidth(root) {
  root.children_width = 0;
  if (root.children[0].type == "osd") {
    root.children_width = OSD.width;
    return root.children_width;
  }
  root.children_width = 0;
  for (let child of root.children) {
    root.children_width += determineWidth(child);
    root.children_width += SPACE_BETWEEN_OSD_COLS;
  }
  root.children_width -= SPACE_BETWEEN_OSD_COLS;
  return root.children_width;
}

/**
 * @typedef {Object} HierarchyInfo
 * @property {Map<string, OSD>} name2osd
 * @property {OSD[]} lastColOSD
 */

/**
 * @param {Node | null} parent
 * @param {BucketDesc} root
 * @param {[number, number]} pos
 * @param {Canvas} canvas
 * @returns {HierarchyInfo}
 */
export function drawHierarchy(parent, root, pos, canvas) {
  determineWidth(root);

  const [rootX, rootY] = pos;

  let n = new Bucket(rootX, rootY, canvas);
  if (parent !== null) {
    n.connectParentBucket(parent);
  }

  let m = new Map();
  let heightMap = [];
  let leftBound = rootX + Bucket.width / 2 - root.children_width / 2;

  if (root.children[0].type == "osd") {
    let childY = pos[1] + Bucket.height + STEP_Y_BETWEEN;
    let prevOSD = null;

    let osd = undefined;
    for (let child of root.children) {
      osd = new OSD(rootX, childY, canvas);
      prevOSD?.addNext(osd);
      prevOSD = osd;
      m.set(child.name, osd);
      childY += OSD.initHeight + STEP_Y_BETWEEN;
    }
    return { name2osd: m, lastColOSD: [osd] };
  } else {
    const childY = pos[1] + Bucket.height + STEP_Y_BETWEEN;
    for (let child of root.children) {
      let indent = (child.children_width - Bucket.width) / 2;
      let { name2osd: subtreeM, lastColOSD: subtreeHeightMap } = drawHierarchy(
        n,
        child,
        [leftBound + indent, childY],
        canvas
      );
      subtreeM.forEach((value, key) => {
        m.set(key, value);
      });
      subtreeHeightMap.forEach((i) => {
        heightMap.push(i);
      });
      leftBound += child.children_width + SPACE_BETWEEN_OSD_COLS;
    }
    return { name2osd: m, lastColOSD: heightMap };
  }
}
