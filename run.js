import nopt from "nopt";
import { cli_init, DB, CO, DayInSecs } from "./mongocli.js";
import {
  genMsgsPerWorker,
  runWorker,
  updateSamples,
  progress,
} from "./worker.js";

async function runDailyJob(co, dayno) {
  const nMsgsPerDay = 3 * 1000 * 1000;
  const nMsgsPerWorker = 100 * 1000;
  let workers = [];

  const timeStart = new Date();
  console.log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
  console.log(`start day ${dayno} job @ ${timeStart.toISOString()}`);

  let wi = 1,
    nTotal = nMsgsPerDay;
  while (nTotal > 0) {
    let msgs = await genMsgsPerWorker(nMsgsPerWorker);
    const worker = runWorker(wi, co, msgs);
    workers.push(worker);
    nTotal -= nMsgsPerWorker;
    wi++;
    msgs = null;
  }
  console.log();
  const progressid = progress("I");

  const results = await Promise.all(workers);
  workers = null;

  clearInterval(progressid);
  const timeEnd = new Date();

  console.log();
  console.log(`> day ${dayno} upload done :`);
  for (const i in results) {
    if (!results[i].acknowledged) {
      console.log(
        `wkr[${i + 1}]: ack=${results[i].acknowledged}, nIns=${
          results[i].insertedCount
        }`,
      );
    }
  }
  console.log(
    `> end time : ${timeEnd.toISOString()}`,
    ` / elapse: ${timeEnd.getTime() - timeStart.getTime()}`,
  );
}

async function sleepUntil(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function run(args) {
  const bClean = args["clean"] ?? false;
  const nDays = 14;
  const DayInMs = DayInSecs * 1000;

  let cli = await cli_init(bClean);

  let co;
  let prevLast = new Date();
  for (let d = 1; d <= nDays; d++) {
    cli.close();
    cli = await cli_init(false);
    co = cli.db(DB).collection(CO);
    const starttm = new Date();
    await runDailyJob(co, d);

    let progressid = progress("U");
    await sleepUntil(10 * 1000); // update after 10 sec
    clearInterval(progressid);
    console.log();
    const curLast = await co.findOne(
      {},
      {
        sort: { createDate: -1 },
        projection: { _id: 0, createDate: 1 },
      },
    );
    await updateSamples(co, prevLast, curLast.createDate);
    prevLast = curLast.createDate;

    progressid = progress("D");
    const MsUntilNextDay = DayInMs - (new Date().getTime() - starttm.getTime());
    await sleepUntil(MsUntilNextDay);
    clearInterval(progressid);
    console.log();
  }

  console.log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
  process.exit(0);
}

const knownOpts = {
    clean: Boolean,
  },
  shortHands = {
    c: ["--clean"],
  };

await run(nopt(knownOpts, shortHands, process.argv, 1));
