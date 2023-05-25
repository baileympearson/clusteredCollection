import { msg_template } from "./msg_template.js";

export async function genMsgsPerWorker(nMsgsPerWorker) {
  let msgs = [];
  let msg;
  let timestamp = new Date();
  while (nMsgsPerWorker > 0) {
    timestamp = nMsgsPerWorker % 2500 == 0 ? new Date() : timestamp;
    msg = {
      ...msg_template,
      createDate: timestamp,
      updateDate: timestamp,
    };
    msgs.push(msg);
    nMsgsPerWorker--;
  }
  process.stdout.write(".");
  return msgs;
}

export async function runWorker(wi, co, docs) {
  const timestart = new Date();
  process.stdout.write(`${wi}`);
  const res = co.insertMany(docs, {
    writeConcert: { w: "majority" },
    ordered: true,
  });
  return res;
}

export async function updateSamples(co, timegt, timelte) {
  const nBase = await co
    .find({
      createDate: {
        $gt: timegt,
        $lte: timelte,
      },
    })
    .count();
  let nSamples = Math.floor(Math.random() * 100);
  nSamples = Math.max(5, nSamples);
  nSamples = Math.min(25, nSamples);
  nSamples = Math.floor((nSamples * nBase) / 100);
  console.log(`nSamples = ${nSamples}/${nBase}`);
  console.log(`update <${timegt.toISOString()}, ${timelte.toISOString()}]`);
}
