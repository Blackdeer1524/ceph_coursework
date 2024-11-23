import "./style.css";
import { mapCanvas } from "./mapCanvas";
import {
  drawHierarchy,
  ConnectorAllocator,
  PrimaryRegistry,
  PGCout,
} from "./connection";

import { Bucket } from "./connection";

/** @type BucketDesc */
const h = {
  name: "root",
  type: "bucket",
  children: [
    {
      name: "rack-1",
      type: "bucket",
      children: [
        {
          name: "osd.1",
          type: "osd",
        },
        {
          name: "osd.2",
          type: "osd",
        },
      ],
    },
    {
      name: "rack-2",
      type: "bucket",
      children: [
        {
          name: "osd.3",
          type: "osd",
        },
        {
          name: "osd.4",
          type: "osd",
        },
        {
          name: "osd.5",
          type: "osd",
        },
        {
          name: "osd.10",
          type: "osd",
        },
      ],
    },
    {
      name: "rack-3",
      type: "bucket",
      children: [
        {
          name: "row-1",
          type: "bucket",
          children: [
            {
              name: "osd.6",
              type: "osd",
            },
            {
              name: "osd.7",
              type: "osd",
            },
          ],
        },
        {
          name: "row-2",
          type: "bucket",
          children: [
            {
              name: "osd.8",
              type: "osd",
            },
            {
              name: "osd.9",
              type: "osd",
            },
          ],
        },
      ],
    },
  ],
};

let initGap = (mapCanvas.getWidth() - Bucket.width) / 2;
let registry = new PrimaryRegistry();
let alloc = new ConnectorAllocator(PGCout);

let start = new Bucket("User", initGap, 30, null, mapCanvas)
let res = drawHierarchy(start, h, [initGap, 130], mapCanvas, [], registry, alloc);

res.get("osd.1").addPG(1);
res.get("osd.1").addPG(2);
res.get("osd.1").addPG(3);
res.get("osd.1").addPG(4);
res.get("osd.1").addPG(5);
res.get("osd.1").addPG(6);
res.get("osd.1").addPG(7);
res.get("osd.1").addPG(8);

res.get("osd.6").addPG(1);
res.get("osd.6").addPG(2);
res.get("osd.6").addPG(3);
res.get("osd.6").addPG(4);
res.get("osd.6").addPG(5);
res.get("osd.6").addPG(6);
res.get("osd.6").addPG(7);
res.get("osd.6").addPG(8);

res.get("osd.10").addPG(1);
res.get("osd.10").addPG(2);
res.get("osd.10").addPG(3);
res.get("osd.10").addPG(4);
res.get("osd.10").addPG(5);
res.get("osd.10").addPG(6);
res.get("osd.10").addPG(7);
res.get("osd.10").addPG(8);

res.get("osd.6").connect(res.get("osd.1"), 1);
res.get("osd.6").connect(res.get("osd.1"), 2);
res.get("osd.6").connect(res.get("osd.1"), 3);
res.get("osd.6").connect(res.get("osd.1"), 4);
res.get("osd.1").connect(res.get("osd.6"), 5);
res.get("osd.1").connect(res.get("osd.6"), 6);
res.get("osd.1").connect(res.get("osd.6"), 7);
res.get("osd.1").connect(res.get("osd.6"), 8);

res.get("osd.6").connect(res.get("osd.10"), 1);
res.get("osd.6").connect(res.get("osd.10"), 2);
res.get("osd.6").connect(res.get("osd.10"), 3);
res.get("osd.6").connect(res.get("osd.10"), 4);
res.get("osd.1").connect(res.get("osd.10"), 5);
res.get("osd.1").connect(res.get("osd.10"), 6);
res.get("osd.1").connect(res.get("osd.10"), 7);
res.get("osd.1").connect(res.get("osd.10"), 8);

registry.remove(1);
res.get("osd.10").connect(res.get("osd.6"), 1);
res.get("osd.10").connect(res.get("osd.1"), 1);

// https://stackoverflow.com/a/35453052
mapCanvas.renderAll();
mapCanvas.forEachObject(function (object) {
  object.selectable = false;
});
// END
