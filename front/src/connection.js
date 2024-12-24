import { Canvas, Line, Polyline, Rect, Textbox, util } from "fabric";

export class PrimaryRegistry {
  constructor() {
    /**
     * @type {Map<number, PG>}
     */
    this.registry = new Map();
  }

  has(pgId) {
    return this.registry.has(pgId);
  }

  /**
   *
   * @param {number} pgId
   * @returns {PG}
   */
  get(pgId) {
    let res = this.registry.get(pgId);
    if (res === undefined) {
      throw Error(`${pgId} not found in registry`);
    }
    return res;
  }

  /**
   *
   * @param {PG} pg
   * @returns
   */
  add(pg) {
    let reg = this.registry.get(pg.id);
    if (reg !== undefined) {
      if (reg.id === pg.id && reg.osd.name === pg.osd.name) {
        return;
      }
      throw Error(`${pgId} has already been registered as a primary`);
    }
    pg.drawnObj.set({stroke: "purple", strokeWidth: 2})
    this.registry.set(pg.id, pg);
    if (pg.pathToBucket === null) {
      pg.connectToBucket();
    }
  }

  remove(pgId) {
    let primary = this.registry.get(pgId);
    if (primary === undefined) {
      return;
    }
    primary.drawnObj.set({stroke: null})
    primary.removeCurrentMapConnectors();
    this.registry.delete(pgId);
  }
}

export class OSD {
  static width = 150;
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
      fill: "#e1e1e1",
      rx: 5,
      ry: 5,

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
    this.drawnText = new Textbox(this.name, {
      top: posY,
      left: posX,
      width: OSD.width,
      fontSize: PGBoxHeight,
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
    this.canvas.add(this.drawnObj);
    this.canvas.add(this.drawnText);
    this.failed = false;
  }

  /**
   * @param {OSD} other
   * @param {number} pgId
   * @returns {RCPath}
   */
  connectTmp(other, pgId) {
    let myPG = this.pgs.get(pgId);
    if (myPG === undefined) {
      throw Error(`connect error: ${this.name} doesn't have PG ${pgId}`);
    }
    let otherPG = other.pgs.get(pgId);
    if (myPG === undefined) {
      throw Error(`connect error: ${other.name} doesn't have PG ${pgId}`);
    }
    return myPG.connectReplicaTmp(otherPG);
  }

  /**
   * @param {OSD} other
   * @param {number} pgId
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
    this.primaryRegistry.add(myPG);
    myPG.connectReplica(otherPG);
  }

  /**
   * @param {OSD} other
   */
  addNext(other) {
    this.nextOSD = other;
  }

  /**
   * idempotent method. If `addPG(ID, false)` is invoked after `addPG(ID, true)`, then nothing changes.
   * If `addPG(ID, true)` is invoked after `addPG(ID, false)`, then only PG's status is changed on being primary.
   * @param {number} id
   * @param {bool} isPrimary
   */
  addPG(id, isPrimary) {
    let pg = this.pgs.get(id);
    if (pg !== undefined) {
      if (isPrimary) {
        this.primaryRegistry.add(pg);
      }
      return;
    }

    let top = this.drawnText.top + this.drawnText.height + PGBoxGap;
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
    if (isPrimary) {
      this.primaryRegistry.add(this.lastPG);
    }

    this.pgs.set(id, this.lastPG);
    this.redraw(this.drawnObj.top);
  }

  /**
   * @param {number} newY
   */
  redraw(newY) {
    this.canvas.remove(this.drawnObj);
    const newHeight = Math.max(
      OSD.initHeight,
      this.drawnText.height +
        this.pgs.size * PGBoxHeight +
        (this.pgs.size + 1) * 3,
    );
    let oldTop = this.drawnObj.top;
    this.drawnObj.height = newHeight;
    this.drawnObj.top = newY;
    this.canvas.add(this.drawnObj);

    this.canvas.remove(this.drawnText);
    this.drawnText.top = newY;
    this.canvas.add(this.drawnText);

    this.pgs.forEach((pg) => {
      pg.redraw(newY - oldTop);
    });

    if (this.nextOSD === null) {
      this.primaryRegistry.registry.forEach((primary) => {
        // todo: figure out how to redraw all PGs
        primary.redrawConnectors();
      });
    } else {
      this.nextOSD.redraw(newY + newHeight + STEP_Y_BETWEEN);
    }
  }

  fail() {
    this.failed = true;
    this.drawnObj.set({ fill: "red" });
  }

  recover() {
    this.failed = false;
    this.drawnObj.set({ fill: "#e1e1e1" });
  }
}

export class Bucket {
  static width = OSD.width;
  static height = 60;

  /**
   * @param {string} name
   * @param {number} posX
   * @param {number} posY
   * @param {Bucket | null} parent
   * @param {Canvas} canvas
   */
  constructor(name, posX, posY, parent, canvas) {
    this.name = name;
    this.canvas = canvas;

    this.drawnObj = new Rect({
      top: posY,
      left: posX,
      width: Bucket.width,
      height: Bucket.height,
      fill: "green",
      rx: 5,
      ry: 5,

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
    this.drawnText = new Textbox(this.name, {
      left: posX,
      top: posY,
      width: Bucket.width,
      fontSize: Bucket.height / 4,
      textAlign: "center",
      fill: "white",

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
    this.canvas.add(this.drawnObj);
    this.canvas.add(this.drawnText);

    this.parent = parent;
    if (parent !== null) {
      parent.connectChildBucket(this);
    }

    /**
     * @type {Map<string, Bucket>}
     */
    this.children = new Map();

    /**
     * @type {Map<string, Line[]>}
     */
    this.connectors = new Map();
  }

  /**
   * @param {Bucket} child
   */
  connectChildBucket(child) {
    this.children.set(child.name, child);
    const myMidpoint = this.drawnObj.left + Bucket.width / 2;
    const otherMidpoint = child.drawnObj.left + Bucket.width / 2;
    const spaceBetween =
      child.drawnObj.top - (this.drawnObj.top + Bucket.height);

    let path = new Polyline(
      [
        { x: myMidpoint, y: this.drawnObj.top + Bucket.height },
        {
          x: myMidpoint,
          y: this.drawnObj.top + Bucket.height + spaceBetween / 2,
        },
        {
          x: otherMidpoint,
          y: this.drawnObj.top + Bucket.height + spaceBetween / 2,
        },
        { x: otherMidpoint, y: child.drawnObj.top },
      ],
      {
        stroke: "green",
        fill: null,

        lockMovementX: true,
        lockMovementY: true,
        lockRotation: true,
        lockScalingX: true,
        lockScalingY: true,
        lockUniScaling: true,
        lockSkewingX: true,
        lockSkewingY: true,
      },
    );
    
    this.canvas.add(path);
    this.connectors.set(child.name, path);
  }
}

const SPACE_BETWEEN_OSD_COLS = 60;
const STEP_Y_BETWEEN = 40;
export const PGCount = 30;
const PGBoxHeight = 20;
const PGBoxGap = 3;

function gcd(a, b) {
  a = Math.abs(a);
  b = Math.abs(b);
  if (b > a) {
    var temp = a;
    a = b;
    b = temp;
  }
  while (true) {
    if (b == 0) return a;
    a %= b;
    if (a == 0) return b;
    b %= a;
  }
}

export class ConnectorAllocator {
  static MIN_INDENT = 5;
  /**
   * @param {number} max
   */
  constructor(max, normal = true, step = 7) {
    this.limit = max;
    this.isAllocated = [];
    this.step = step;
    if (gcd(max, this.step) != 1) {
      throw Error(
        "assertion error: allocator step and alloc size must be co-prime",
      );
    }

    if (normal) {
      this.colors = [
        "#d53e4f",
        "#f46d43",
        "#fdae61",
        "#02519e",
        "#011f74",
        "#0190a67",
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
    for (let i = 0; i < max; i++) {
      this.isAllocated.push(false);
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
    for (let i = 0; i < this.isAllocated.length; ++i) {
      let index = (this.step * i) % this.isAllocated.length;
      if (!this.isAllocated[index]) {
        this.isAllocated[index] = true;
        return [index, this.colors[index % this.colors.length]];
      }
    }
    return null;
  }

  /**
   * @param {number} i
   */
  free(i) {
    if (!this.isAllocated[i]) {
      throw Error(`double free: ${i}`);
    }

    this.isAllocated[i] = false;
  }
}

export class RCPath {
  /**
   * @param {string} name
   * @param {Polyline} path
   * @param {Canvas} canvas
   * @param {function():Polyline} redraw
   * @param {function():null} destructor
   */
  constructor(name, path, canvas, redraw, destructor) {
    canvas.add(path);
    this.name = name;
    this.path = path;
    this.canvas = canvas;
    this.count = 1;
    this.redraw_func = redraw;
    this.destructor = destructor;
  }

  redraw() {
    if (this.count == 0) {
      throw Error(`tried to redraw dealocated path "${this.paht}"`);
    }
    console.log(`${this.name} is redrawing`);
    this.canvas.remove(this.path);
    this.path = this.redraw_func();
    this.canvas.add(this.path);
  }

  up() {
    if (this.count == 0) {
      throw Error(
        `tried to up reference count on dealocated path "${this.paht}"`,
      );
    }
    ++this.count;
  }

  down() {
    --this.count;
    if (this.count == 0) {
      this.canvas.remove(this.path);
      this.path = null;
      this.destructor();
    }
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
    this.name = `${this.osd.name}.${this.id}`;
    this.col = col;
    this.canvas = canvas;
    this.lastColOSD = lastColOSD;
    this.interPgConnAlloc = interPgConnAlloc;
    this.bucketPgConnAlloc = bucketPgConnAlloc;
    /**
     * @type {RCPath | null}
     */
    this.pathToBucket = null;
    this.pathToBucketRC = 0;

    this.bucketConnectorID = null;
    this.bucketConnectorColor = null;

    this.connectorID = null;
    this.connectorColor = null;
    /**
     * @type {Map<string, PG>}
     */
    this.replicas = new Map();

    this.drawnObj = new Rect({
      top: posY,
      left: posX + PGBoxGap,
      width: OSD.width - 2 * PGBoxGap,
      height: PGBoxHeight,
      fill: "#f6f6f6",

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
    this.canvas.add(this.drawnObj);

    this.drawnText = new Textbox(`${id}`, {
      top: posY,
      left: posX + PGBoxGap,
      fontSize: PGBoxHeight,

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
    this.canvas.add(this.drawnText);

    /**
     * @type {Map<string, RCPath>}
     */
    this.connectors = new Map();
    this.peeringCount = 0;
  }

  startPeering() {
    if (this.peeringCount == 0) {
      this.drawnObj.set({ fill: "orange" });
    }
    this.peeringCount++;
  }

  endPeering() {
    this.peeringCount--;
    if (this.peeringCount == 0) {
      this.drawnObj.set({ fill: "#f6f6f6" });
    }
  }

  removeCurrentMapConnectors() {
    this.replicas.forEach((_, name) => {
      this.connectors.get(name).down();
    });
    this.replicas.clear();
    this.pathToBucket?.down();
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
    this.connectors.forEach((path) => path.redraw());
    this.pathToBucket?.redraw();
  }

  #calculatePathToBucket() {
    if (this.bucketConnectorID == null) {
      throw Error(
        `${this.name} tried to calculate path to bucket, but path wasn't allocated`,
      );
    }

    let bucket = this.osd.bucket;
    const indent = this.bucketPgConnAlloc.getIndent(this.bucketConnectorID);
    const myHeightMidpoint = bucket.drawnObj.top + bucket.drawnObj.height / 2;
    const pgHeightMidpoint = this.drawnObj.top + this.drawnObj.height / 2;

    let path = new Polyline(
      [
        { x: bucket.drawnObj.left, y: myHeightMidpoint },
        { x: bucket.drawnObj.left - indent, y: myHeightMidpoint },
        { x: bucket.drawnObj.left - indent, y: pgHeightMidpoint },
        { x: this.drawnObj.left, y: pgHeightMidpoint },
      ],
      {
        stroke: this.bucketConnectorColor,
        fill: null,

        lockMovementX: true,
        lockMovementY: true,
        lockRotation: true,
        lockScalingX: true,
        lockScalingY: true,
        lockUniScaling: true,
        lockSkewingX: true,
        lockSkewingY: true,
      },
    );
    return path;
  }

  /**
   * is **NOT** an idempotent method!!!
   * creates a path to parent bucket. This path is **reference counted**
   */
  connectToBucket() {
    if (this.pathToBucket !== null) {
      this.pathToBucket.up();
      return;
    }

    if (this.bucketConnectorID === null) {
      let connOpt = this.bucketPgConnAlloc.alloc();
      if (connOpt === null) {
        throw Error(
          `couldn't allocate connection from ${this.osd.bucket.name} to ${this.name}`,
        );
      }
      [this.bucketConnectorID, this.bucketConnectorColor] = connOpt;
    }

    this.pathToBucket = new RCPath(
      `${this.name}->${this.osd.bucket.name}`,
      this.#calculatePathToBucket(),
      this.canvas,
      () => this.#calculatePathToBucket(),
      () => {
        this.pathToBucket = null;
        this.bucketPgConnAlloc.free(this.bucketConnectorID);
        this.bucketConnectorID = null;
        this.bucketConnectorColor = null;
      },
    );
  }

  releaseBucketConnect() {
    if (this.pathToBucket === null) {
      throw Error(`${this.name}: no path to bucket exists to release`);
    }
    this.pathToBucket.down();
  }

  /**
   * Connects primary with its replica. allocates indent if needed.
   * @param {PG} replica
   * @returns {Line[]} drawn path to replica
   */
  #calculatePathToReplica(replica) {
    if (this.pathToBucket === null) {
      throw Error(
        `I tried to connect to replica, but I'm not primary: ${this.osd.name}, ${this.id} -> ${replica.osd.name}, ${replica.id}`,
      );
    }

    if (this.connectorID === null) {
      console.log("allocating connect for ", this.osd.name, this.id);
      let res = this.interPgConnAlloc.alloc();
      if (res === null) {
        throw Error("couldn't allocate a connector");
      }
      [this.connectorID, this.connectorColor] = res;
    }

    let indent = this.interPgConnAlloc.getIndent(this.connectorID);

    let path = [];
    if (this.col < replica.col) {
      path.push({
        x: this.drawnObj.left + this.drawnObj.width,
        y: this.drawnObj.top + this.drawnObj.height / 2,
      });
      let lastX = this.drawnObj.left + this.drawnObj.width + PGBoxGap + indent;
      let lastY = this.drawnObj.top + this.drawnObj.height / 2;
      path.push({ x: lastX, y: lastY });
      let n = replica.col - this.col - 1;
      for (let i = 0; i < n; ++i) {
        let passOSD = this.lastColOSD[this.col + i + 1].drawnObj;
        let newY = passOSD.top + passOSD.height + indent;
        path.push({ x: lastX, y: newY });
        let newX = passOSD.left + passOSD.width + indent;
        path.push({ x: newX, y: newY });
        lastX = newX;
        lastY = newY;
      }
      let childMidpointY = replica.drawnObj.top + replica.drawnObj.height / 2;
      path.push({ x: lastX, y: childMidpointY });
      path.push({ x: replica.drawnObj.left, y: childMidpointY });
    } else {
      let lastX = this.drawnObj.left + this.drawnObj.width + PGBoxGap + indent;
      let lastY = this.drawnObj.top + this.drawnObj.height / 2;
      path.push({
        x: this.drawnObj.left + this.drawnObj.width,
        y: this.drawnObj.top + this.drawnObj.height / 2,
      });
      path.push({ x: lastX, y: lastY });
      let n = this.col - replica.col + 1;
      for (let i = 0; i < n - 1; ++i) {
        let passOSD = this.lastColOSD[this.col - i].drawnObj;
        let newY = passOSD.top + passOSD.height + indent;
        path.push({ x: lastX, y: newY });
        let newX = passOSD.left - SPACE_BETWEEN_OSD_COLS + indent;
        path.push({ x: newX, y: newY });
        lastX = newX;
        lastY = newY;
      }
      let passOSD = this.lastColOSD[replica.col].drawnObj;
      let newY = passOSD.top + passOSD.height + indent;
      path.push({ x: lastX, y: newY });
      let newX = passOSD.left - SPACE_BETWEEN_OSD_COLS + indent;
      path.push({ x: newX, y: newY });
      lastX = newX;
      lastY = newY;

      let childMidpointY = replica.drawnObj.top + replica.drawnObj.height / 2;
      path.push({ x: lastX, y: childMidpointY });
      path.push({ x: replica.drawnObj.left, y: childMidpointY });
    }
    return new Polyline(path, {
      stroke: this.connectorColor,
      fill: null,

      lockMovementX: true,
      lockMovementY: true,
      lockRotation: true,
      lockScalingX: true,
      lockScalingY: true,
      lockUniScaling: true,
      lockSkewingX: true,
      lockSkewingY: true,
    });
  }

  /**
   * @param {PG} replica
   * @returns {RCPath} tmp path
   */
  connectReplicaTmp(replica) {
    let rcPath = this.connectors.get(replica.name);
    if (rcPath !== undefined) {
      rcPath.up();
      return rcPath;
    }
    rcPath = new RCPath(
      `${this.name}->${replica.name}`,
      this.#calculatePathToReplica(replica),
      this.canvas,
      () => this.#calculatePathToReplica(replica),
      () => {
        this.connectors.delete(replica.name);
        if (this.connectors.size === 0) {
          this.interPgConnAlloc.free(this.connectorID);
          this.connectorID = null;
          this.connectorColor = null;
        }
      },
    );
    this.connectors.set(replica.name, rcPath);
    return rcPath;
  }

  /**
   * @param {PG} replica
   * @returns {null}
   */
  connectReplica(replica) {
    this.replicas.set(replica.name, replica);
    let rcPath = this.connectors.get(replica.name);
    if (rcPath !== undefined) {
      rcPath.up();
      return null;
    }

    replica.drawnObj.set({stroke: "green", strokeWidth: 2})

    rcPath = new RCPath(
      `${this.name}->${replica.name}`,
      this.#calculatePathToReplica(replica),
      this.canvas,
      () => this.#calculatePathToReplica(replica),
      () => {
        replica.drawnObj.set({stroke: null})
        this.connectors.delete(replica.name);
        if (this.connectors.size === 0) {
          this.interPgConnAlloc.free(this.connectorID);
          this.connectorID = null;
          this.connectorColor = null;
        }
      },
    );
    this.connectors.set(replica.name, rcPath);
    return null;
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

  let b = new Bucket(root.name, rootX, rootY, parent, canvas);

  /**
   * @type {HierarchyInfo}
   */
  let res = new Map();

  if (root.children[0].type == "osd") {
    let childY = pos[1] + Bucket.height + STEP_Y_BETWEEN;
    let prevOSD = null;

    let osd = undefined;
    let bucketPgConnAlloc = new ConnectorAllocator(PGCount, false, 11);
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

/**
 * @param {number} pgId
 * @param {PrimaryRegistry} registry
 * @param {string[]} map
 * @param {Map<string, OSD>} name2osd
 */
export function setupMapping(pgId, registry, map, name2osd) {
  console.log(`setting up a map for ${pgId}: ${map}`);
  if (map.length == 0) {
    return;
  }

  if (registry.has(pgId)) {
    registry.remove(pgId);
  }

  let primaryOSD = name2osd.get(map[0]);
  if (primaryOSD === undefined) {
    return;
  }
  primaryOSD.addPG(pgId, true);
  for (let i = 1; i < map.length; ++i) {
    let secondaryOSD = name2osd.get(map[i]);
    if (secondaryOSD === undefined) {
      continue;
    }
    secondaryOSD.addPG(pgId, false);
    primaryOSD.connect(secondaryOSD, pgId);
  }
}

/**
 * @param {Map<string, OSD>} oldName2osd
 * @param {Map<number, PG>} oldRegistry
 * @param {Map<string, OSD>} newName2osd
 */
export function adjustHierarchy(oldName2osd, oldRegistry, newName2osd) {
  oldName2osd.forEach((oldOSD, osdName) => {
    let newOSD = newName2osd.get(osdName);
    if (newOSD === undefined) {
      return;
    }
    if (oldOSD.failed) {
      newOSD.fail();
    }
    oldOSD.pgs.forEach((pg) => {
      newOSD.addPG(pg.id, false);
      let newPG = newOSD.pgs.get(pg.id);
      for (let i = 0; i < pg.peeringCount; ++i) {
        newPG.startPeering();
      }
    });
  });

  oldRegistry.registry.forEach((oldPrimary, pgId) => {
    let primaryOSD = newName2osd.get(oldPrimary.osd.name);
    if (primaryOSD === undefined) {
      return;
    }
    primaryOSD.addPG(pgId, true);
    oldPrimary.replicas.forEach((oldReplica) => {
      let childOSD = newName2osd.get(oldReplica.osd.name);
      if (childOSD === undefined) {
        return;
      }
      primaryOSD.addPG(pgId, false);
      primaryOSD.connect(childOSD, pgId);
    });
  });
}
