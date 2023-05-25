import nopt from "nopt";
import { cli_init } from "./mongocli.js";
import { genMsgsPerWorker, runWorker } from "./worker.js";

function initts(starttm) {
  let tms = starttm.getTime();
  return function () {
    return new Date(++tms);
  };
}

async function run(args) {
  const bClean = args["clean"] ?? false;
  const nDays = 3;
  const nMsgsPerDay = 3 * 1000 * 1000;
  const nMsgsPerWorker = 100 * 1000;
  let workers = [];

  const co = await cli_init(bClean);

  const timeStart = new Date();

  const timestamp = initts(timeStart);
  let wi = 1,
    nTotal = nMsgsPerDay;
  while (nTotal > 0) {
    const msgs = await genMsgsPerWorker(nMsgsPerWorker, timestamp);
    const worker = runWorker(wi, co, msgs);
    workers.push(worker);
    nTotal -= nMsgsPerWorker;
    wi++;
  }

  const results = await Promise.all(workers);
  const timeEnd = new Date();

  console.log(">");
  for (const i in results) {
    if (!results[i].acknowledged) {
      console.log(
        `wkr[${i + 1}]: ack=${results[i].acknowledged}, nIns=${
          results[i].insertedCount
        }`,
      );
    }
  }

  console.log();
  console.log("> total processing...");
  console.log(`> start time   : ${timeStart.toISOString()}`);
  console.log(`> end time     : ${timeEnd.toISOString()}`);
  console.log("> elapsed secs :", timeEnd.getTime() - timeStart.getTime());
  process.exit(0);
}

const knownOpts = {
    clean: Boolean,
  },
  shortHands = {
    c: ["--clean"],
  };

await run(nopt(knownOpts, shortHands, process.argv, 1));
