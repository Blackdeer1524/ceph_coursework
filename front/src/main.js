import "./style.css";
import { Rect, Line } from "fabric";
import { mapCanvas } from "./mapCanvas";
import { drawHierarchy, ConnectorAllocator } from "./connection";

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
let res = drawHierarchy(null, h, [initGap, 30], 0, mapCanvas);
let alloc = new ConnectorAllocator(8);

res.name2osd.get("osd.1").addPG(123, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(124, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(127, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(132, res.lastColOSD, alloc);

res.name2osd.get("osd.5").addPG(127, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(124, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(129, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(131, res.lastColOSD, alloc);

res.name2osd
  .get("osd.1")
  .connect(res.name2osd.get("osd.5"), 127, res.lastColOSD, alloc);
res.name2osd
  .get("osd.5")
  .connect(res.name2osd.get("osd.1"), 124, res.lastColOSD, alloc);

res.name2osd.get("osd.3").addPG(132, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(133, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(134, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(135, res.lastColOSD, alloc);

res.name2osd.get("osd.9").addPG(132);
res.name2osd.get("osd.9").addPG(133);
res.name2osd.get("osd.9").addPG(134);
res.name2osd.get("osd.9").addPG(135);

res.name2osd
  .get("osd.1")
  .connect(res.name2osd.get("osd.9"), 132, res.lastColOSD, alloc);
res.name2osd
  .get("osd.3")
  .connect(res.name2osd.get("osd.9"), 133, res.lastColOSD, alloc);

// https://stackoverflow.com/a/35453052
mapCanvas.renderAll();
mapCanvas.forEachObject(function (object) {
  object.selectable = false;
});
// END
