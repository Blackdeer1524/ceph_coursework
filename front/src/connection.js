import { Canvas, Rect } from "fabric"

export class Node {
    static width = 100;
    static leftConnectionOffset = 5
    static widhtWithConnections = Node.width + Node.leftConnectionOffset
    static height = 60;

    constructor(posX, posY, canvas) {
        this.posX = posX + OSD.leftConnectionOffset
        this.posY = posY
        this.canvas = canvas

        this.drawnObj = new Rect({
            top: posY + OSD.leftConnectionOffset,
            left: posX,
            width: Node.width,
            height: Node.height,
        })
        this.canvas.add(this.drawnObj)
    }

    connect(otherNode) {
    }
}

export class OSD {
    static width = 100;
    static leftConnectionOffset = 5
    static rightConnectionOffset = 25

    static widhtWithConnections = OSD.width + Node.leftConnectionOffset + OSD.rightConnectionOffset
    static initHeight = 60;

    constructor(posX, posY, canvas) {
        this.posX = posX + OSD.leftConnectionOffset
        this.posY = posY
        this.canvas = canvas

        this.drawnObj = new Rect({
            top: posY + OSD.leftConnectionOffset,
            left: posX,
            width: OSD.width,
            height: OSD.initHeight,
        })
        this.canvas.add(this.drawnObj)
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


const STEP_X_BETWEEN = 5
const STEP_Y_BETWEEN = 5

/**
 * @param {BucketDesc} hierarchy 
 * @returns {number}
 */
function determineWidth(hierarchy) {
    hierarchy.children_width = 0
    if (hierarchy.children[0].type == "osd") {
        hierarchy.children_width = OSD.widhtWithConnections
        return OSD.widhtWithConnections
    }
    for (let child of hierarchy.children) {
        hierarchy.children_width += determineWidth(child)
        hierarchy.children_width += STEP_X_BETWEEN
    }
    hierarchy.children_width -= STEP_X_BETWEEN
    return hierarchy.children_width
}

/**
 * @param {BucketDesc | OSDDesc} hierarchy 
 * @param {[number, number]} pos
 * @param {Canvas} canvas
 */
export function drawHierarchy(hierarchy, pos, canvas) {
    determineWidth(hierarchy)
    if (hierarchy.type == "bucket") {
        let n = new Node(pos[0], pos[1], canvas)

        let spaceAvailable = hierarchy.children_width
        if (hierarchy.children[0].type == "osd") {
            spaceAvailable = 0;
        }

        hierarchy.children.forEach((c) => {
            if (c.type == "osd") {
                spaceAvailable -= OSD.widhtWithConnections
            } else {
                spaceAvailable -= Node.width
            }
        })



        childY = pos[1] + n.height + STEP_Y_BETWEEN
        for (let child of hierarchy.children) {
            drawHierarchy(child,)

        }
    }






}