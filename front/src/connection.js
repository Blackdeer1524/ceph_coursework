import { Canvas, Line, Rect, Textbox } from "fabric";

export class PrimaryRegistry {
  constructor() {
    /**
     * @type {Map<number, PG>}
     */
    this.registry = new Map();
  }

  set(pgId, pg) {
    let reg = this.registry.get(pgId);
    if (reg !== undefined) {
      if (reg === pg) {
        return;
      }
      throw Error(`${pgId} has already been registered as a primary`);
    }
    this.registry.set(pgId, pg);
  }

  remove(pgId) {
    let primary = this.registry.get(pgId);
    if (primary === undefined) {
      throw Error(`no primary found: ${pgId}`);
    }
    this.registry.delete(pgId)
    primary.removeConnectors();
  }
}

export class Bucket {
  static width = 100;
  static height = 60;

  /**
   * @param {string} name
   * @param {number} posX
   * @param {number} posY
   * @param {Canvas} canvas
   */
  constructor(name, posX, posY, canvas) {
    this.name = name;
    this.canvas = canvas;

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
     * @type {Map<number, [PG, number]>}
     */
    this.primaries = new Map();

    /**
     * @type {Map<number, Line[]>}
     */
    this.connects = new Map();
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
}

const SPACE_BETWEEN_OSD_COLS = 60;
const STEP_Y_BETWEEN = 40;
export const PGCout = 20
const PGBoxHeight = 20;
const PGBoxGap = 3;

export class ConnectorAllocator {
  static MIN_INDENT = 5;
  /**
   * @param {number} max
   */
  constructor(max, normal = true) {
    this.limit = max;
    this.is_allocated = [];
    if (normal) {
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
    } else {
      this.colors = [
        "#2ac1b0",
        "#0b92bc",
        "#02519e",
        "#011f74",
        "#0190a67",
        "#54225b",
        "#993d5a",
        "#cd7742",
      ];
    }
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
  /**
   * @param {number} id
   * @param {OSD} osd
   * @param {number} posX
   * @param {number} posY
   * @param {number} col
   * @param {Canvas} canvas
   * @param {OSD[]} lastColOSD
   * @param {ConnectorAllocator} interPgConnAlloc
   */
  constructor(
    id,
    osd,
    posX,
    posY,
    col,
    canvas,
    lastColOSD,
    bucketPgConnAlloc,
    interPgConnAlloc,
  ) {
    this.id = id;
    this.osd = osd;
    this.col = col;
    this.canvas = canvas;
    this.lastColOSD = lastColOSD;
    this.interPgConnAlloc = interPgConnAlloc;
    this.bucketPgConnAlloc = bucketPgConnAlloc;
    /**
     * @type {Line[] | null}
     */
    this.pathToBucket = null;

    this.bucketConnectorID = null;
    this.bucketConnectorColor = null;

    this.connectorID = null;
    this.connectorColor = null;
    /**
     * @type {PG[]}
     */
    this.replicas = [];

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
    this.connectors = [];
  }

  removeConnectors() {
    this.connectors.forEach((path) =>
      path.forEach((l) => this.canvas.remove(l)),
    );
    this.connectors = [];
    this.interPgConnAlloc.free(this.connectorID);
    this.connectorID = null;
    this.connectorColor = null;

    if (this.pathToBucket !== null) {
      this.pathToBucket.forEach((l) => this.canvas.remove(l));
      this.pathToBucket = null;
      this.bucketPgConnAlloc.free(this.bucketConnectorID);
      this.bucketConnectorID = null;
      this.bucketConnectorColor = null;
    }
  }

  redraw(deltaY) {
    this.canvas.remove(this.drawnObj);
    this.canvas.remove(this.drawnText);
    this.drawnText.top += deltaY;
    this.drawnObj.top += deltaY;
    this.canvas.add(this.drawnObj);
    this.canvas.add(this.drawnText);

    this.redrawConnectors();
  }

  redrawConnectors() {
    this.connectors.forEach((path) =>
      path.forEach((l) => this.canvas.remove(l)),
    );
    this.connectors = [];
    let old_replicas = this.replicas;
    this.replicas = [];
    old_replicas.forEach((c) => this.connectReplica(c));

    if (this.pathToBucket !== null) {
      this.pathToBucket.forEach((l) => this.canvas.remove(l));
      this.pathToBucket = null;
      this.connectBucket();
    }
  }

  connectBucket() {
    if (this.pathToBucket !== null) {
      return;
    }
    if (this.bucketConnectorID == null) {
      let connOpt = this.bucketPgConnAlloc.alloc();
      if (connOpt === null) {
        throw Error(
          `couldn't allocate connection from ${bucket.name} to ${this.name}`,
        );
      }
      [this.bucketConnectorID, this.bucketConnectorColor] = connOpt;
    }

    let bucket = this.osd.bucket;
    const indent = this.bucketPgConnAlloc.getIndent(this.bucketConnectorID);
    const myHeightMidpoint = bucket.drawnObj.top + bucket.drawnObj.height / 2;
    const pgHeightMidpoint = this.drawnObj.top + this.drawnObj.height / 2;
    let path = [
      new Line(
        [
          bucket.drawnObj.left,
          myHeightMidpoint,
          bucket.drawnObj.left - indent,
          myHeightMidpoint,
        ],
        { stroke: this.bucketConnectorColor },
      ),
      new Line(
        [
          bucket.drawnObj.left - indent,
          myHeightMidpoint,
          bucket.drawnObj.left - indent,
          pgHeightMidpoint,
        ],
        { stroke: this.bucketConnectorColor },
      ),
      new Line(
        [
          bucket.drawnObj.left - indent,
          pgHeightMidpoint,
          this.drawnObj.left,
          pgHeightMidpoint,
        ],
        { stroke: this.bucketConnectorColor },
      ),
    ];
    this.pathToBucket = path;

    path.forEach((l) => this.canvas.add(l));
  }

  /**
   * @param {PG} replica
   */
  connectReplica(replica) {
    if (this.connectorID === null) {
      let res = this.interPgConnAlloc.alloc();
      if (res === null) {
        throw Error("couldn't allocate a connector");
      }
      [this.connectorID, this.connectorColor] = res;
    }

    this.connectBucket();

    let indent = this.interPgConnAlloc.getIndent(this.connectorID);
    this.replicas.push(replica);

    if (this.col < replica.col) {
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
      let n = replica.col - this.col - 1;
      for (let i = 0; i < n; ++i) {
        let passOSD = this.lastColOSD[this.col + i + 1].drawnObj;
        let newY =
          passOSD.top + passOSD.height + SPACE_BETWEEN_OSD_COLS - indent;
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

      let childMidpointY = replica.drawnObj.top + replica.drawnObj.height / 2;
      path.push(
        new Line([lastX, lastY, lastX, childMidpointY], {
          stroke: this.connectorColor,
        }),
      );
      path.push(
        new Line(
          [lastX, childMidpointY, replica.drawnObj.left, childMidpointY],
          {
            stroke: this.connectorColor,
          },
        ),
      );
      path.forEach((c) => this.canvas.add(c));
      this.connectors.push(path);
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
      let n = this.col - replica.col + 1;
      for (let i = 0; i < n - 1; ++i) {
        let passOSD = this.lastColOSD[this.col - i].drawnObj;
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
      }
      let passOSD = this.lastColOSD[replica.col].drawnObj;
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

      let childMidpointY = replica.drawnObj.top + replica.drawnObj.height / 2;
      path.push(
        new Line([lastX, lastY, lastX, childMidpointY], {
          stroke: this.connectorColor,
        }),
      );
      path.push(
        new Line(
          [lastX, childMidpointY, replica.drawnObj.left, childMidpointY],
          {
            stroke: this.connectorColor,
          },
        ),
      );
      path.forEach((c) => this.canvas.add(c));
      this.connectors.push(path);
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
   * @param {PrimaryRegistry} primaryRegistry
   * @param {ConnectorAllocator} bucketPgConnAlloc
   */
  constructor(
    bucket,
    name,
    posX,
    posY,
    col,
    canvas,
    lastColOSD,
    primaryRegistry,
    bucketPgConnAlloc,
    interPgConnAlloc,
  ) {
    this.bucket = bucket;
    this.name = name;
    this.col = col;
    this.canvas = canvas;
    this.lastColOSD = lastColOSD;
    this.primaryRegistry = primaryRegistry;
    this.interPgConnAlloc = interPgConnAlloc;
    this.bucketPgConnAlloc = bucketPgConnAlloc;

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
   * @param {number} pgId
   * @param {OSD[]} lastColOSD
   */
  connect(other, pgId) {
    let myPG = this.pgs.get(pgId);
    if (myPG === undefined) {
      throw Error(`connect error: ${this.name} doesn't have PG ${pgId}`);
    }
    let otherPG = other.pgs.get(pgId);
    if (myPG === undefined) {
      throw Error(`connect error: ${other.name} doesn't have PG ${pgId}`);
    }
    this.primaryRegistry.set(pgId, myPG);
    myPG.connectReplica(otherPG);
  }

  /**
   * @param {OSD} other
   */
  addNext(other) {
    this.nextOSD = other;
  }

  /**
   * @param {number} id
   * @param {ConnectorAllocator} interPgConnAlloc
   */
  addPG(id) {
    let top = this.drawnObj.top + PGBoxGap;
    if (this.lastPG !== null) {
      top = this.lastPG.drawnObj.top + this.lastPG.drawnObj.height + PGBoxGap;
    }
    this.lastPG = new PG(
      id,
      this,
      this.drawnObj.left,
      top,
      this.col,
      this.canvas,
      this.lastColOSD,
      this.bucketPgConnAlloc,
      this.interPgConnAlloc,
    );
    this.pgs.set(id, this.lastPG);
    this.redraw(this.drawnObj.top);
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
    this.drawnObj.height = newHeight;
    this.drawnObj.top = newY;
    this.canvas.add(this.drawnObj);

    this.pgs.forEach((pg) => {
      pg.redraw(newY - oldTop);
    });

    if (this.nextOSD === null) {
      this.primaryRegistry.registry.forEach((primary) => {
        if (primary.col >= this.col) {
          primary.replicas.forEach((replica) => {
            if (replica.col <= this.col) {
              primary.redrawConnectors();
            }
          });
        } else {
          primary.replicas.forEach((replica) => {
            if (replica.col >= this.col) {
              primary.redrawConnectors();
            }
          });
        }
      });
    } else {
      this.nextOSD.redraw(newY + newHeight + STEP_Y_BETWEEN);
    }
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
 * @param {Map<number, PG>} primaryRegistry
 * @returns {Map<string, OSD>}
 */
export function drawHierarchy(
  parent,
  root,
  pos,
  canvas,
  lastColOSD,
  primaryRegistry,
  interPgConnAlloc,
) {
  determineWidth(root);

  const [rootX, rootY] = pos;

  let b = new Bucket(root.name, rootX, rootY, canvas);
  if (parent !== null) {
    b.connectParentBucket(parent);
  }

  /**
   * @type {HierarchyInfo}
   */
  let res = new Map();

  if (root.children[0].type == "osd") {
    let childY = pos[1] + Bucket.height + STEP_Y_BETWEEN;
    let prevOSD = null;

    let osd = undefined;
    let bucketPgConnAlloc = new ConnectorAllocator(PGCout, false);
    for (let child of root.children) {
      osd = new OSD(
        b,
        child.name,
        rootX,
        childY,
        lastColOSD.length,
        canvas,
        lastColOSD,
        primaryRegistry,
        bucketPgConnAlloc,
        interPgConnAlloc,
      );
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
        lastColOSD,
        primaryRegistry,
        interPgConnAlloc,
      );
      subtreeRes.forEach((value, key) => {
        res.set(key, value);
      });
      leftBound += child.children_width + SPACE_BETWEEN_OSD_COLS;
    }
  }
  return res;
}
