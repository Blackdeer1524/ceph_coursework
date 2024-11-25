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

// let registry = new PrimaryRegistry();
// let interPgConnAlloc = new ConnectorAllocator(PGCout);
//
// let start = new Bucket("User", INIT_GAP, 30, null, mapCanvas);
// let res = drawHierarchy(
//   start,
//   h,
//   [INIT_GAP, 130],
//   mapCanvas,
//   [],
//   registry,
//   interPgConnAlloc,
// );
//
// setupMapping(1, registry, ["osd.1", "osd.2", "osd.10"], res);
// setupMapping(2, registry, ["osd.2", "osd.1", "osd.10"], res);
// setupMapping(3, registry, ["osd.10", "osd.1", "osd.2"], res);
// setupMapping(4, registry, ["osd.2", "osd.1", "osd.10"], res);
// setupMapping(5, registry, ["osd.1", "osd.2", "osd.10"], res);
// setupMapping(1, registry, ["osd.6", "osd.7", "osd.8"], res);
//
// res.get("osd.1").startPeering();
// res.get("osd.5").fail();
// res.get("osd.5").recoverFromFailure();
//
// animateSendItem(1, 1, registry);
// animateSendToReplicas(1, 3, registry);

function main() {
  let simStart = document.getElementById("sim-start");
  var socket = new WebSocket("ws://localhost:8080");

  simStart.onclick = (e) => {
    socket.send(
      JSON.stringify({
        type: "step",
      }),
    );
  };

  socket.addEventListener("open", (event) => {
    console.log("sent a mapping");
    socket.send(
      JSON.stringify({
        type: "rule",
        message: `
device 0 osd.0 class hdd
device 1 osd.1 class hdd
device 2 osd.2 class ssd
device 3 osd.3 class ssd
device 4 osd.4 class hdd
device 5 osd.5 class hdd
device 6 osd.6 class ssd
device 7 osd.7 class ssd
device 8 osd.8 class ssd

host ceph-osd-server-1 {
    id -1
    alg uniform
    item osd.0 weight 1.00
}

host ceph-osd-server-2 {
    id -2
    hash 0
    alg uniform
    item osd.1 weight 1.00
    item osd.2 weight 1.00
    item osd.3 weight 1.00
}

host ceph-osd-server-3 {
    id -3
    hash 0
    alg uniform
    item osd.4 weight 1.00
    item osd.5 weight 1.00
    item osd.6 weight 1.00
    item osd.7 weight 1.00
}

root default{
    id -4
    alg straw2
    item ceph-osd-server-1 
    item ceph-osd-server-2 
    item ceph-osd-server-3 
}

rule cold {
    id 0
    type replicated
    min_size 2
    max_size 11
    step take default class hdd
    step chooseleaf firstn 0 type host
    step emit
}

rule hot {
    id 1
    type replicated
    min_size 2
    max_size 11
    step take default class ssd
    step chooseleaf firstn 0 type host
    step emit
}
  `,
      }),
    );
  });

  /**
   * @typedef {Object} State
   * @property {Bucket} start
   * @property {PrimaryRegistry} registry
   * @property {ConnectorAllocator} interPgConnAlloc
   * @property {Map<string, OSD>} name2osd
   */

  /**
   * @type {State | null}
   */
  let state = null;

  socket.addEventListener("message", (event) => {
    let res = JSON.parse(event.data);

    console.log(res.type);
    switch (res.type) {
      case "hierarchy":
        const INIT_GAP = (mapCanvas.getWidth() - Bucket.width) / 2;
        mapCanvas.clear();
        state = {
          start: new Bucket("User", INIT_GAP, 30, null, mapCanvas),
          registry: new PrimaryRegistry(),
          interPgConnAlloc: new ConnectorAllocator(PGCout),
        };
        state.name2osd = drawHierarchy(
          state.start,
          res.data,
          [INIT_GAP, 130],
          mapCanvas,
          [],
          state.registry,
          state.interPgConnAlloc,
        );
        break;
      case "events":
        if (state === null) {
          console.log("can't process events when state is null");
          return;
        }
        let timestamp = res.timestamp;
        let events = res.events;
        for (let e of events) {
          switch (e.type) {
            case "primary_recv_success": {
              let primary = state.registry.get(e.pg);
              animateSendStatus(
                e.objId,
                e.pg,
                primary.osd.name,
                state.registry,
                "successRecv",
              );
              animateSendToReplicas(e.objId, e.pg, state.registry);
              break;
            }
            case "primary_recv_fail": {
              let primary = state.registry.get(e.pg);
              animateSendStatus(
                e.objId,
                e.pg,
                primary.osd.name,
                state.registry,
                "failRecv",
              );
              break;
            }
            case "primary_recv_ack": {
              let primary = state.registry.get(e.pg);
              animateSendStatus(
                e.objId,
                e.pg,
                primary.osd.name,
                state.registry,
                "successRecv",
              );
              break;
            }
            case "primary_replication_fail": {
              let primary = state.registry.get(e.pg);
              animateSendStatus(
                e.objId,
                e.pg,
                primary.osd.name,
                state.registry,
                "failRecv",
              );
              break;
            }
            case "replica_recv_ack": {
              animateSendStatus(
                e.objId,
                e.pg,
                e.osd,
                state.registry,
                "successRecv",
              );
              break;
            }
            case "replica_recv_success": {
              animateSendStatus(
                e.objId,
                e.pg,
                e.osd,
                state.registry,
                "successRecv",
              );
              break;
            }
            case "replica_recv_fail": {
              animateSendStatus(
                e.objId,
                e.pg,
                e.osd,
                state.registry,
                "failRecv",
              );
              break;
            }
            case "peering_start": {
              e.osds.forEach((osdName) => {
                state.name2osd.get(osdName).
              })

              animateSendStatus(
                e.objId,
                e.pg,
                e.osd,
                state.registry,
                "failRecv",
              );
              break;
            }
          }
        }
        break;
    }
  });
}

main();
