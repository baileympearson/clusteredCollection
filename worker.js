import { msg_template } from "./msg_template.js";
import { CO } from "./mongocli.js";

export function progress(tickChar) {
  let prog = 1;
  const id = setInterval(() => {
    const out = prog++ % 10 == 0 ? tickChar : ".";
    process.stdout.write(out);
  }, 1000);
  return id;
}

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
  const starttm = new Date().getTime();
  const nBase = await co
    .find({
      createDate: {
        $gt: timegt,
        $lte: timelte,
      },
    })
    .count();
  let nSamples = Math.round(Math.random() * 25);
  nSamples = Math.max(5, nSamples);
  nSamples = Math.floor((nSamples * nBase) / 100);
  console.log(
    `update [${nSamples}/${nBase}] btn ${timegt.toISOString()} & ${timelte.toISOString()}`,
  );

  const pipeline = [
    {
      $match: {
        createDate: {
          $gt: timegt,
          $lte: timelte,
        },
      },
    },
    {
      $sample: {
        size: nSamples,
      },
    },
    {
      $set: {
        isOpen: true,
      },
    },
    {
      $merge: {
        into: CO,
        on: "_id",
        whenMatched: "replace",
      },
    },
  ];
  const progid = progress("U");
  await co.aggregate(pipeline).toArray();
  const nUpdated = await co
    .find({
      createDate: {
        $gt: timegt,
        $lte: timelte,
      },
      isOpen: true,
    })
    .count();
  clearInterval(progid);
  console.log();
  const endtm = new Date().getTime();
  console.log("> done nUpdated=", nUpdated, "/ elapsed:", endtm - starttm);
}
