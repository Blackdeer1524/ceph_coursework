import "./style.css";
import { mapCanvas } from "./mapCanvas";
import {
  drawHierarchy,
  ConnectorAllocator,
  PrimaryRegistry,
  PGCount,
  setupMapping,
  OSD,
  adjustHierarchy,
} from "./connection";

import {
  animateSendStatus,
  animateSendToPrimary,
  animateSendToReplicas,
  animateSendFailure,
} from "./animations";

import { Bucket } from "./connection";

const DEFAULT_CONFIG = `
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
    item osd.1 weight 1.00
}

host ceph-osd-server-2 {
    id -2
    hash 0
    alg uniform
    item osd.2 weight 1.00
    item osd.3 weight 1.00
    item osd.4 weight 1.00
}

host ceph-osd-server-3 {
    id -3
    hash 0
    alg uniform
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
`;

let timestampLabel = document.getElementById("time-label");

function main() {
  var socket = new WebSocket("ws://localhost:8080");

  let isRandomized = true;

  document.getElementById("editor").value = DEFAULT_CONFIG;
  socket.addEventListener("open", (event) => {
    let objId = 0;
    document.getElementById("sim-start").onclick = (e) => {
      socket.send(
        JSON.stringify({
          type: "step",
        }),
      );
    };

    let editor = document.getElementById("editor");
    editor.addEventListener("keydown", function (e) {
      if (e.key == "Tab") {
        e.preventDefault();
        var start = this.selectionStart;
        var end = this.selectionEnd;

        // set textarea value to: text before caret + tab + text after caret
        this.value =
          this.value.substring(0, start) + "\t" + this.value.substring(end);

        // put caret at right position again
        this.selectionStart = this.selectionEnd = start + 1;
      }
    });

    let submitButton = document.getElementById("config-submit");
    submitButton.onclick = (e) => {
      objId = 0;
      timestampLabel.innerHTML = 0;
      socket.send(
        JSON.stringify({
          type: "rule",
          message: editor.value,
        }),
      );
    };

    let adjustButton = document.getElementById("config-adjust");
    adjustButton.onclick = (e) => {
      socket.send(
        JSON.stringify({
          type: "adjust_rule",
          message: editor.value,
        }),
      );
    };

    let insertButton = document.getElementById("insert-button");
    insertButton.onclick = (e) => {
      ++objId;
      socket.send(
        JSON.stringify({
          type: "insert",
          id: objId,
        }),
      );
    };

    document.getElementById("pop-up-close").onclick = (e) => {
      document.getElementById("pop-up").style.visibility = "hidden";
    };

    document.getElementById("mode-toggle").onclick = (e) => {
      isRandomized = !isRandomized;
      let modeLabel = document.getElementById("mode-label");
      if (isRandomized) {
        modeLabel.innerHTML = "Randomized";
        socket.send(
          JSON.stringify({
            type: "mode",
            new_mode: "randomized",
          }),
        );
      } else {
        modeLabel.innerHTML = "Manual";
        socket.send(
          JSON.stringify({
            type: "mode",
            new_mode: "manual",
          }),
        );
      }
    };

    socket.send(
      JSON.stringify({
        type: "rule",
        message: editor.value,
      }),
    );
  });

  /**
   * @typedef {Object} PeeringData
   * @property {string[]} newMap
   * @property {string[]} peeringOsds
   * @property {number} pg
   */

  /**
   * @typedef {Object} State
   * @property {Map<number, PeeringData>} peeringInfo
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
    let simStart = document.getElementById("sim-start")

    switch (res.type) {
      case "hierarchy_fail": {
        console.log(res.data.replace("\n", "<br>").replaceAll(" ", "&nbsp;"));
        document.getElementById("error-message").innerHTML = res.data
          .replaceAll("\n", "<br>")
          .replaceAll(" ", "&nbsp;");
        document.getElementById("pop-up").style.visibility = "visible";
        break;
      }
      case "hierarchy_success":
        const INIT_GAP = (mapCanvas.getWidth() - Bucket.width) / 2;
        mapCanvas.forEachObject((o) => {
          mapCanvas.remove(o);
        });
        state = {
          start: new Bucket("User", INIT_GAP, 30, null, mapCanvas),
          registry: new PrimaryRegistry(),
          interPgConnAlloc: new ConnectorAllocator(PGCount, true, true),
          peeringInfo: new Map(),
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
      case "adjust_hierarchy_success": {
        const INIT_GAP = (mapCanvas.getWidth() - Bucket.width) / 2;
        mapCanvas.forEachObject((o) => {
          mapCanvas.remove(o);
        });
        let oldState = state;
        state = {
          start: new Bucket("User", INIT_GAP, 30, null, mapCanvas),
          registry: new PrimaryRegistry(),
          interPgConnAlloc: new ConnectorAllocator(PGCount, true, true),
          peeringInfo: state.peeringInfo,
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
        adjustHierarchy(oldState.name2osd, oldState.registry, state.name2osd);
        break;
      }
      case "events":
        if (state === null) {
          console.log("can't process events when state is null");
          return;
        }
        let timestamp = res.timestamp;
        timestampLabel.innerHTML = timestamp;
        let events = res.events;
        console.log("=====================");
        for (let e of events) {
          console.log(e.type);
          switch (e.type) {
            case "send_fail": {
              simStart.disabled = true;
              animateSendFailure(e.objId, state.start, () => {
                simStart.disabled = false;
              });
              break;
            }
            case "primary_recv_success": {
              simStart.disabled = true;
              animateSendToPrimary(
                e.objId,
                e.pg,
                state.name2osd.get(e.map[0]),
                () => {
                  animateSendStatus(
                    e.objId,
                    e.pg,
                    state.name2osd.get(e.map[0]),
                    "successRecv",
                  );
                  animateSendToReplicas(
                    e.objId,
                    e.pg,
                    e.map,
                    state.name2osd,
                    () => {
                      simStart.disabled = false;
                    },
                  );
                },
              );
              break;
            }
            case "primary_recv_fail": {
              let primaryOSD = state.name2osd.get(e.osd);
              simStart.disabled = true;
              animateSendToPrimary(e.objId, e.pg, primaryOSD, () => {
                animateSendStatus(e.objId, e.pg, primaryOSD, "failRecv");
                simStart.disabled = false;
              });
              break;
            }
            case "primary_recv_ack": {
              animateSendStatus(
                e.objId,
                e.pg,
                state.name2osd.get(e.osd),
                "successRecv",
              );
              break;
            }
            case "primary_replication_fail": {
              animateSendStatus(
                e.objId,
                e.pg,
                state.name2osd.get(e.osd),
                "failRecv",
              );
              break;
            }
            case "replica_recv_ack": {
              animateSendStatus(
                e.objId,
                e.pg,
                state.name2osd.get(e.osd),
                "successRecv",
              );
              break;
            }
            case "replica_recv_success": {
              animateSendStatus(
                e.objId,
                e.pg,
                state.name2osd.get(e.osd),
                "successRecv",
              );
              break;
            }
            case "replica_recv_fail": {
              animateSendStatus(
                e.objId,
                e.pg,
                state.name2osd.get(e.osd),
                "failRecv",
              );
              break;
            }
            case "peering_start": {
              e.osds.forEach((osdName) => {
                state.name2osd.get(osdName).pgs.get(e.pg).startPeering();
              });
              state.peeringInfo.set(e.peering_id, {
                newMap: e.new_map_candidate,
                peeringOsds: e.osds,
                pg: e.pg,
              });
              break;
            }
            case "peering_success": {
              let info = state.peeringInfo.get(e.peering_id);
              console.log("peering success: ", info);
              setupMapping(
                info.pg,
                state.registry,
                info.newMap,
                state.name2osd,
              );
              info.peeringOsds.forEach((osdName) => {
                state.name2osd.get(osdName).pgs.get(info.pg).endPeering();
              });
              state.peeringInfo.delete(e.peering_id);
              break;
            }
            case "peering_fail": {
              let info = state.peeringInfo.get(e.peering_id);
              info.peeringOsds.forEach((osdName) => {
                state.name2osd.get(osdName)?.pgs.get(info.pg).endPeering();
              });
              state.peeringInfo.delete(e.peering_id);
              break;
            }
            case "osd_failed": {
              state.name2osd.get(e.osd)?.fail();
              break;
            }
            case "osd_recovered": {
              state.name2osd.get(e.osd)?.recover();
              break;
            }
          }
        }
        break;
    }
  });
}

main();
