import { msg_template } from "./msg_template.js";

async function sleep(ms) {
  /* ISODate resulotion is 1ms
   * meanwhile, clusteredIndex must be unique
   */
  return new Promise((r) => setTimeout(r, ms));
}

export async function genMsgsPerWorker(nMsgsPerWorker, timestamp) {
  console.log("gen msgs per worker", new Date().toISOString());
  let msgs = [];
  let msg;
  let newts;
  while (nMsgsPerWorker > 0) {
    newts = timestamp();
    msg = {
      ...msg_template,
      _id: new Date(newts),
      updateDate: new Date(newts),
    };
    msgs.push(msg);
    nMsgsPerWorker--;
  }
  return msgs;
}

export async function runWorker(wi, co, docs) {
  const timestart = new Date();
  console.log(`wkr[${wi}] start inserting @ ${timestart.toISOString()}`);
  const res = co.insertMany(docs, {
    writeConcert: { w: "majority" },
    ordered: true,
  });
  return res;
}
