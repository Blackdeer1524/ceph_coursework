import { Canvas, Line, Rect, Text, Textbox } from "fabric";

export class Bucket {
  static width = 100;
  static height = 60;

  /**
   * @param {string} name
   * @param {number} posX
   * @param {number} posY
   * @param {Canvas} canvas
   * @param {ConnectorAllocator} alloc
   */
  constructor(name, posX, posY, canvas, alloc) {
    this.name = name;
    this.canvas = canvas;
    this.alloc = alloc;

    this.drawnObj = new Rect({
      top: posY,
      left: posX,
      width: Bucket.width,
      height: Bucket.height,
      fill: "green",
    });
    this.canvas.add(this.drawnObj);

    this.children = [];
    /**
     * @type {PG[]}
     */
    this.primaries = []
    this.connects = [];
  }

  /**
   * @param {Bucket} other
   */
  connectParentBucket(other) {
    this.children.push(other);
    const myMidpoint = this.drawnObj.left + Bucket.width / 2;
    const otherMidpoint = other.drawnObj.left + Bucket.width / 2;
    const spaceBetween =
      this.drawnObj.top - (other.drawnObj.top + Bucket.height);

    const start = new Line(
      [
        myMidpoint,
        this.drawnObj.top,
        myMidpoint,
        this.drawnObj.top - spaceBetween / 2,
      ],
      {
        stroke: "green",
      },
    );

    const middle = new Line(
      [
        myMidpoint,
        this.drawnObj.top - spaceBetween / 2,
        otherMidpoint,
        this.drawnObj.top - spaceBetween / 2,
      ],
      {
        stroke: "green",
      },
    );

    const end = new Line(
      [
        otherMidpoint,
        this.drawnObj.top - spaceBetween / 2,
        otherMidpoint,
        this.drawnObj.top - spaceBetween,
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

  /**
   * @param {PG} pg
   */
  connectPG(pg) {
    let res = this.alloc.alloc();
    if (res === null) {
      throw Error(
        `couldn't allocate connection from ${this.name} to ${pg.name}`,
      );
    }
    let [connectorID, color] = res;

    const indent = this.alloc.getIndent(connectorID);
    const myHeightMidpoint = this.drawnObj.top + this.drawnObj.height / 2;
    const pgHeightMidpoint = pg.drawnObj.top + pg.drawnObj.height / 2;
    let path = [
      new Line(
        [
          this.drawnObj.left,
          myHeightMidpoint,
          this.drawnObj.left - indent,
          myHeightMidpoint,
        ],
        { stroke: color },
      ),
      new Line(
        [
          this.drawnObj.left - indent,
          myHeightMidpoint,
          this.drawnObj.left - indent,
          pgHeightMidpoint,
        ],
        { stroke: color },
      ),
      new Line(
        [
          this.drawnObj.left - indent,
          pgHeightMidpoint,
          pg.drawnObj.left,
          pgHeightMidpoint,
        ],
        { stroke: color },
      ),
    ];
    this.connects.push(path);
    this.primaries.push(pg);

    path.forEach((l) => this.canvas.add(l));
  }

  redrawConnectors() {
    this.connects.forEach((p) => p.forEach((l) => this.canvas.remove(l)));
    this.connects = [];
    this.primaries.forEach((c) => this.connectPG(c));
  }
}

const SPACE_BETWEEN_OSD_COLS = 60;
const STEP_Y_BETWEEN = 40;
const PGBoxHeight = 20;
const PGBoxGap = 3;

export class ConnectorAllocator {
  static MIN_INDENT = 5;
  /**
   * @param {number} max
   */
  constructor(max) {
    this.limit = max;
    this.is_allocated = [];
    this.colors = [
      "#d53e4f",
      "#f46d43",
      "#fdae61",
      "#fee08b",
      "#e6f598",
      "#abdda4",
      "#66c2a5",
      "#3288bd",
    ];
    for (let i = 0; i < max; ++i) {
      this.is_allocated.push(false);
    }
  }

  getIndent(level) {
    let indent =
      ConnectorAllocator.MIN_INDENT +
      level *
        ((SPACE_BETWEEN_OSD_COLS - ConnectorAllocator.MIN_INDENT) /
          (this.limit + 1));
    return indent;
  }

  /**
   * @returns {[number, string] | null}
   */
  alloc() {
    for (let i = 0; i < this.is_allocated.length; ++i) {
      if (!this.is_allocated[i]) {
        this.is_allocated[i] = true;
        return [i, this.colors[i % this.colors.length]];
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
  constructor(id, posX, posY, col, canvas, lastColOSD, alloc) {
    this.id = id;
    this.col = col;
    this.canvas = canvas;
    this.lastColOSD = lastColOSD;
    this.alloc = alloc;

    this.connectorID = null;
    this.connectorColor = null;
    /**
     * @type {PG[]}
     */
    this.children = [];

    this.drawnObj = new Rect({
      top: posY,
      left: posX + PGBoxGap,
      width: OSD.width - 2 * PGBoxGap,
      height: PGBoxHeight,
      fill: "pink",
    });
    this.canvas.add(this.drawnObj);

    this.drawnText = new Textbox(`${id}`, {
      top: posY,
      left: posX + PGBoxGap,
      fontSize: PGBoxHeight,
    });
    this.canvas.add(this.drawnText);

    /**
     * @type {Line[][]}
     */
    this.connects = [];
    /**
     * @type {PG[]}
     */
  }

  redraw(delta) {
    this.canvas.remove(this.drawnObj);
    this.canvas.remove(this.drawnText);
    this.drawnText.top += delta;
    this.drawnObj.top += delta;
    this.canvas.add(this.drawnObj);
    this.canvas.add(this.drawnText);

    this.redrawConnectors();
  }

  redrawConnectors() {
    this.connects.forEach((path) => path.forEach((l) => this.canvas.remove(l)));
    this.connects = [];
    this.children.forEach((c) => this.connect(c));
  }

  /**
   * @param {PG} child
   */
  connect(child) {
    if (this.connectorID === null) {
      let res = this.alloc.alloc();
      if (res === null) {
        throw Error("couldn't allocate a connector");
      }
      [this.connectorID, this.connectorColor] = res;
    }

    let indent = this.alloc.getIndent(this.connectorID);
    this.children.push(child);

    if (this.col < child.col) {
      let lastX = this.drawnObj.left + this.drawnObj.width + PGBoxGap + indent;
      let lastY = this.drawnObj.top + this.drawnObj.height / 2;
      let path = [
        new Line(
          [
            this.drawnObj.left + this.drawnObj.width,
            this.drawnObj.top + this.drawnObj.height / 2,
            lastX,
            lastY,
          ],
          { stroke: this.connectorColor },
        ),
      ];
      let n = child.col - this.col - 1;
      for (let i = 0; i < n; ++i) {
        let passOSD = this.lastColOSD[this.col + i + 1].drawnObj;
        let newY = passOSD.top + passOSD.height + indent;
        path.push(
          new Line([lastX, lastY, lastX, newY], {
            stroke: this.connectorColor,
          }),
        );
        let newX =
          passOSD.left + passOSD.width + SPACE_BETWEEN_OSD_COLS - indent;
        path.push(
          new Line([lastX, newY, newX, newY], { stroke: this.connectorColor }),
        );
        lastX = newX;
        lastY = newY;
      }

      let childMidpointY = child.drawnObj.top + child.drawnObj.height / 2;
      path.push(
        new Line([lastX, lastY, lastX, childMidpointY], {
          stroke: this.connectorColor,
        }),
      );
      path.push(
        new Line([lastX, childMidpointY, child.drawnObj.left, childMidpointY], {
          stroke: this.connectorColor,
        }),
      );
      path.forEach((c) => this.canvas.add(c));
      this.connects.push(path);
    } else {
      let lastX = this.drawnObj.left + this.drawnObj.width + PGBoxGap + indent;
      let lastY = this.drawnObj.top + this.drawnObj.height / 2;
      let path = [
        new Line(
          [
            this.drawnObj.left + this.drawnObj.width,
            this.drawnObj.top + this.drawnObj.height / 2,
            lastX,
            lastY,
          ],
          { stroke: this.connectorColor },
        ),
      ];
      let n = this.col - child.col + 1;
      for (let i = 0; i < n - 1; ++i) {
        let passOSD = this.lastColOSD[this.col - i].drawnObj;
        let newY = passOSD.top + passOSD.height + indent;
        path.push(
          new Line([lastX, lastY, lastX, newY], {
            stroke: this.connectorColor,
          }),
        );
        let newX = passOSD.left - SPACE_BETWEEN_OSD_COLS + indent;
        path.push(
          new Line([lastX, newY, newX, newY], { stroke: this.connectorColor }),
        );
        lastX = newX;
        lastY = newY;
      }
      let passOSD = this.lastColOSD[child.col].drawnObj;
      let newY = passOSD.top + passOSD.height + indent;
      path.push(
        new Line([lastX, lastY, lastX, newY], {
          stroke: this.connectorColor,
        }),
      );
      let newX = passOSD.left - indent;
      path.push(
        new Line([lastX, newY, newX, newY], { stroke: this.connectorColor }),
      );
      lastX = newX;
      lastY = newY;

      let childMidpointY = child.drawnObj.top + child.drawnObj.height / 2;
      path.push(
        new Line([lastX, lastY, lastX, childMidpointY], {
          stroke: this.connectorColor,
        }),
      );
      path.push(
        new Line([lastX, childMidpointY, child.drawnObj.left, childMidpointY], {
          stroke: this.connectorColor,
        }),
      );
      path.forEach((c) => this.canvas.add(c));
      this.connects.push(path);
    }
  }
}

export class OSD {
  static width = 100;
  static initHeight = 60;

  /**
   * @param {Bucket} bucket
   * @param {string} name
   * @param {number} posX
   * @param {number} posY
   * @param {number} col
   * @param {Canvas} canvas
   * @param {OSD[]} lastColOSD
   */
  constructor(bucket, name, posX, posY, col, canvas, lastColOSD) {
    this.bucket = bucket;
    this.name = name;
    this.col = col;
    this.canvas = canvas;
    this.lastColOSD = lastColOSD;
    /**
     * @type {Set<OSD>}
     */
    this.parents = new Set();

    this.nextOSD = null;
    /**
     * @type {Map<number, PG>}
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
   * @param {OSD[]} lastColOSD
   */
  connect(other, pg_id) {
    let myPG = this.pgs.get(pg_id);
    if (myPG === undefined) {
      throw Error(`connect error: ${this.name} doesn't have PG ${pg_id}`);
    }
    let otherPG = other.pgs.get(pg_id);
    if (myPG === undefined) {
      throw Error(`connect error: ${other.name} doesn't have PG ${pg_id}`);
    }
    other.parents.add(this);
    this.bucket.connectPG(myPG);
    myPG.connect(otherPG);
  }

  /**
   * @param {OSD} other
   */
  addNext(other) {
    this.nextOSD = other;
  }

  /**
   * @param {number} id
   * @param {ConnectorAllocator} alloc
   */
  addPG(id, alloc) {
    let top = this.drawnObj.top + PGBoxGap;
    if (this.lastPG !== null) {
      top = this.lastPG.drawnObj.top + this.lastPG.drawnObj.height + PGBoxGap;
    }
    this.lastPG = new PG(
      id,
      this.drawnObj.left,
      top,
      this.col,
      this.canvas,
      this.lastColOSD,
      alloc,
    );
    this.pgs.set(id, this.lastPG);
    this.redraw(this.drawnObj.top, this.lastColOSD, alloc);
  }

  /**
   * @param {OSD[]} lastColOSD
   * @param {ConnectorAllocator} alloc
   */
  redrawConnectors() {
    this.pgs.forEach((pg) => pg.redrawConnectors());
  }

  /**
   * @param {number} newY
   */
  redraw(newY) {
    this.canvas.remove(this.drawnObj);
    const newHeight = Math.max(
      OSD.initHeight,
      this.pgs.size * PGBoxHeight + (this.pgs.size + 1) * 3,
    );
    let oldTop = this.drawnObj.top;
    this.drawnObj = new Rect({
      top: newY,
      left: this.drawnObj.left,
      width: OSD.width,
      height: newHeight,
      fill: "blue",
    });
    this.canvas.add(this.drawnObj);

    this.pgs.forEach((pg) => {
      pg.redraw(newY - oldTop);
    });
    this.parents.forEach((v) => v.redrawConnectors());
    if (this.nextOSD === null) {
      this.bucket.redrawConnectors();
    } else {
      this.nextOSD.redraw(newY + newHeight + STEP_Y_BETWEEN);
    }
    this.redrawConnectors();
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
 * @param {Bucket | null} parent
 * @param {BucketDesc} root
 * @param {[number, number]} pos
 * @param {number} col
 * @param {Canvas} canvas
 * @param {OSD[]} lastColOSD
 * @returns {Map<string, OSD>}
 */
export function drawHierarchy(parent, root, pos, canvas, lastColOSD) {
  determineWidth(root);

  const [rootX, rootY] = pos;

  let b = new Bucket(root.name, rootX, rootY, canvas, new ConnectorAllocator(8));
  if (parent !== null) {
    b.connectParentBucket(parent);
  }

  /**
   * @type {HierarchyInfo}
   */
  let res = new Map() ;

  if (root.children[0].type == "osd") {
    let childY = pos[1] + Bucket.height + STEP_Y_BETWEEN;
    let prevOSD = null;

    let osd = undefined;
    for (let child of root.children) {
      osd = new OSD(b, child.name, rootX, childY, lastColOSD.length, canvas, lastColOSD);
      prevOSD?.addNext(osd);
      prevOSD = osd;
      res.set(child.name, osd);
      childY += OSD.initHeight + STEP_Y_BETWEEN;
    }
    lastColOSD.push(osd);
  } else {
    const childY = pos[1] + Bucket.height + STEP_Y_BETWEEN;
    let leftBound = rootX + Bucket.width / 2 - root.children_width / 2;
    for (let child of root.children) {
      let indent = (child.children_width - Bucket.width) / 2;
      let subtreeRes = drawHierarchy(
        b,
        child,
        [leftBound + indent, childY],
        canvas,
        lastColOSD
      );
      subtreeRes.forEach((value, key) => {
        res.set(key, value);
      });
      leftBound += child.children_width + SPACE_BETWEEN_OSD_COLS;
    }
  }
  return res;
}
