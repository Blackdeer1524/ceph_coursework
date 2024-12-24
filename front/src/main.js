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
device 0 osd.0
device 1 osd.1
device 2 osd.2
device 3 osd.3
device 4 osd.4
device 5 osd.5
device 6 osd.6
device 7 osd.7
device 8 osd.8

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

rule choice {
    id 0
    step take default 
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

  class LockButton {
    constructor(button) {
      this.button = button;
      this.c = 0;
    }

    lock() {
      if (this.c == 0) {
        this.button.disabled = true;
      }
      ++this.c;
    }

    unlock(meta) {
      --this.c;
      if (this.c == 0) {
        this.button.disabled = false;
      }
      console.log(meta, this.c)
    }
  }

  let simStart = new LockButton(document.getElementById("sim-start"));
  socket.addEventListener("message", (event) => {
    let res = JSON.parse(event.data);

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
          interPgConnAlloc: new ConnectorAllocator(PGCount, true, 7),
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
          interPgConnAlloc: new ConnectorAllocator(PGCount, true, 7),
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
        timestampLabel.innerHTML = res.timestamp;
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
        console.log(`${timestamp}=====================`);
        for (let e of events) {
          switch (e.type) {
            case "send_fail": {
              console.log(`${e.objId} failed. reason: ${e.reason}`)
              animateSendFailure(e.objId, state.start, () => null);
              break;
            }
            case "primary_recv_success": {
              simStart.lock();
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
                    simStart,
                    () => simStart.unlock(`${e.objId} sent to replicas`),
                  );
                },
              );
              break;
            }
            case "primary_recv_fail": {
              let primaryOSD = state.name2osd.get(e.osd);
              simStart.lock();
              animateSendToPrimary(e.objId, e.pg, primaryOSD, () => {
                animateSendStatus(e.objId, e.pg, primaryOSD, "failRecv");
                simStart.unlock(`primary failure for ${e.objId}`);
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
