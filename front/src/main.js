import "./style.css";
import { Rect, Line } from "fabric";
import { mapCanvas } from "./mapCanvas";
import { drawHierarchy } from "./connection";

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

let res = drawHierarchy(null, h, [0, 0], mapCanvas);
console.log(res["name2osd"]);
res.name2osd.get("osd.1").addPG(123)
res.name2osd.get("osd.1").addPG(124)
res.name2osd.get("osd.1").addPG(125)
res.name2osd.get("osd.1").addPG(127)

res.name2osd.get("osd.5").addPG(127)
res.name2osd.get("osd.5").addPG(128)
res.name2osd.get("osd.5").addPG(129)
res.name2osd.get("osd.5").addPG(131)

res.name2osd.get("osd.3").addPG(127)
res.name2osd.get("osd.3").addPG(128)
res.name2osd.get("osd.3").addPG(129)
res.name2osd.get("osd.3").addPG(131)


// https://stackoverflow.com/a/35453052
mapCanvas.renderAll();
mapCanvas.forEachObject(function (object) {
  object.selectable = false;
});
// END
