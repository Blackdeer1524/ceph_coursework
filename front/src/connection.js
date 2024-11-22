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
      },
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
      },
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
      },
    );

    this.canvas.add(start);
    this.canvas.add(middle);
    this.canvas.add(end);

    // this.childConnections.push(start);
    // this.childConnections.push(middle);
    // this.childConnections.push(end);
  }
}

const SPACE_BETWEEN_OSD_COLS = 60;
const STEP_Y_BETWEEN = 40;
const PGBoxHeight = 20;
const PGBoxGap = 3;

class ConnectorAllocator {
  /**
   * @param {number} max
   */
  constructor(max) {
    this.limit = max;
    this.is_allocated = [];
    for (let i = 0; i < max; ++i) {
      this.is_allocated.push(false);
    }
  }

  /**
   * @returns {number | null}
   */
  alloc() {
    for (let i = 0; i < this.is_allocated.length; ++i) {
      if (!this.is_allocated[i]) {
        this.is_allocated[i] = true;
        return i;
      }
    }
    return null;
  }

  /**
   * @param {number} i
   */
  free(i) {
    if (!this.is_allocated[i]) {
      throw Error(`double free: ${i}`);
    }

    this.is_allocated[i] = false;
  }
}

class PG {
  constructor(posX, posY, col, canvas) {
    this.col = col;
    this.canvas = canvas;
    this.drawnObj = new Rect({
      top: posY,
      left: posX + PGBoxGap,
      width: OSD.width - 2 * PGBoxGap,
      height: PGBoxHeight,
      fill: "pink",
    });
    this.canvas.add(this.drawnObj);

    /**
     * @type {Line[][]}
     */
    this.connects = [];
    /**
     * @type {PG[]}
     */
    this.children = [];
  }

  redraw(delta) {
    this.canvas.remove(this.drawnObj);
    this.drawnObj.top += delta;
    this.canvas.add(this.drawnObj);

    this.connects.forEach((path) => path.forEach((l) => this.canvas.rmeove(l)));
    this.children.forEach((c) => this.connect(c));
  }

  /**
   * @param {PG} child
   * @param {OSD[]} lastColOSD
   * @param {ConnectorAllocator} connectorAlloc
   */
  connect(child, lastColOSD, connectorAlloc) {
    let res = connectorAlloc.alloc();
    if (res === null) {
      throw Error("couldn't allocate a connector");
    }

    let indent = SPACE_BETWEEN_OSD_COLS / connectorAlloc.limit;
    this.children.push(child);

    if (this.col <= other.col) {
      let lastX = this.drawnObj.left + this.drawnObj.width + PGBoxGap + indent;
      let lastY = this.drawnObj.top + this.height / 2;
      let path = [
        new Line(
          [
            this.drawnObj.left + this.drawnObj.width,
            this.drawnObj.top + this.height / 2,
            lastX,
            lastY,
          ],
          { stroke: "red" },
        ),
      ];
      let n = other.col - this.col - 1;
      for (let i = 0; i < n; ++i) {
        let passOSD = lastColOSD[this.col + i + 1].drawnObj;
        path.push(new Line([lastX, lastY, lastX, passOSD.height + indent]));
        let newX = passOSD.left + passOSD.width + indent;
        let newY = passOSD.height + indent;
        path.push(new Line([lastX, newY, newX, newY]));
        lastX = newX;
        lastY = newY;
      }

      let childMidpointY = child.drawnObj.top + child.drawnObj.height / 2;
      path.push(new Line([lastX, lastY, lastX, childMidpointY]));
      path.push(
        new Line([lastX, childMidpointY, child.drawnObj.left, childMidpointY]),
      );
      path.forEach((c) => this.canvas.add(c));
      this.connect.push(path);
    } else {
      throw Error("not implemented");
    }
  }
}

export class OSD {
  static width = 100;
  static initHeight = 60;

  constructor(posX, posY, col, canvas) {
    this.col = col;
    this.canvas = canvas;

    this.nextOSD = null;
    /**
     * @type {Map<string, PG>}
     */
    this.pgs = new Map();
    this.lastPG = null;

    this.drawnObj = new Rect({
      top: posY,
      left: posX,
      width: OSD.width,
      height: OSD.initHeight,
      fill: "blue",
    });
    this.canvas.add(this.drawnObj);
  }

  /**
   *
   * @param {OSD} other
   * @param {number} pg_id
   * @param {ConnectorAllocator} connectAllocInfo
   */
  connect(other, pg_id, connectAllocInfo) {
    this.pgs;
  }

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
    let top = this.drawnObj.top + PGBoxGap;
    if (this.lastPG !== null) {
      top = this.lastPG.top + this.lastPG.height + PGBoxGap;
    }
    this.lastPG = new PG(this.drawnObj.left, top, this.col, this.canvas);
    this.pgs.push(this.lastPG);
    this.redraw(this.drawnObj.top);
  }

  /**
   * @param {number} newY
   */
  redraw(newY) {
    this.canvas.remove(this.drawnObj);
    this.pgs.forEach((c) => {
      c.redraw(newY - this.drawnObj.top);
    });
    const newHeight = Math.max(
      OSD.initHeight,
      this.pgs.length * PGBoxHeight + (this.pgs.length + 1) * 3,
    );
    this.drawnObj = new Rect({
      top: newY,
      left: this.drawnObj.left,
      width: OSD.width,
      height: newHeight,
      fill: "blue",
    });
    this.canvas.add(this.drawnObj);
    this.pgs.forEach((c) => {
      this.canvas.add(c);
    });

    this.nextOSD?.redraw(newY + newHeight + STEP_Y_BETWEEN);
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
        canvas,
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
