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

res.name2osd.get("osd.1").addPG(1, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(2, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(3, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(4, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(5, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(6, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(7, res.lastColOSD, alloc);
res.name2osd.get("osd.1").addPG(8, res.lastColOSD, alloc);


res.name2osd.get("osd.3").addPG(1, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(2, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(3, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(4, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(5, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(6, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(7, res.lastColOSD, alloc);
res.name2osd.get("osd.3").addPG(8, res.lastColOSD, alloc);

res.name2osd.get("osd.5").addPG(8, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(1, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(7, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(2, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(6, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(3, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(5, res.lastColOSD, alloc);
res.name2osd.get("osd.5").addPG(4, res.lastColOSD, alloc);

res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 1, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 2, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 3, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 4, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 5, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 6, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 7, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.1"), 8, res.lastColOSD, alloc);

res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 1, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 2, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 3, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 4, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 5, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 6, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 7, res.lastColOSD, alloc);
res.name2osd .get("osd.5") .connect(res.name2osd.get("osd.3"), 8, res.lastColOSD, alloc);

// https://stackoverflow.com/a/35453052
mapCanvas.renderAll();
mapCanvas.forEachObject(function (object) {
  object.selectable = false;
});
// END
