import "./style.css";
import { mapCanvas } from "./mapCanvas";
import {
  drawHierarchy,
  ConnectorAllocator,
  PrimaryRegistry,
  PGCout,
  setupMapping,
} from "./connection";

import {
  animateSendStatus,
  animateSendItem,
  animateSendToReplicas,
} from "./animations";

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

let start = new Bucket("User", initGap, 30, null, mapCanvas);
let res = drawHierarchy(
  start,
  h,
  [initGap, 130],
  mapCanvas,
  [],
  registry,
  alloc,
);

setupMapping(1, registry, ["osd.1", "osd.2", "osd.10"], res);
setupMapping(2, registry, ["osd.2", "osd.1", "osd.10"], res);
setupMapping(3, registry, ["osd.10", "osd.1", "osd.2"], res);
setupMapping(4, registry, ["osd.2", "osd.1", "osd.10"], res);
setupMapping(5, registry, ["osd.1", "osd.2", "osd.10"], res);
