import { MongoClient } from "mongodb";
import dotenv from "dotenv";

const DayInSecs = 4 * 60;
const ExpireInSecs = DayInSecs * 10;

const DB = "musinsa";
const CO = "msgcol";

async function cli_init(bClean) {
  dotenv.config();
  const connURI = process.env.ATLAS_CONN_URI ?? "";
  let msgcol;

  const cli = new MongoClient(connURI);
  await cli.connect();
  const db = cli.db(DB);

  const cols = await db.collections();
  let bExist = cols.some((col) => col.s.namespace.collection === CO);
  if (bExist && bClean) {
    console.log(`+++ reset collection: "${DB}.${CO}"`);
    await db.collection(CO).drop();
    bExist = false;
  }

  if (!bExist) {
    console.log(`+++ create collection: "${DB}.${CO}"`);
    const col = await cli.db(DB).createCollection(CO);
    msgcol = col;

    await col.createIndex(
      { createDate: 1 },
      { expireAfterSeconds: ExpireInSecs },
    );
  }

  return cli;
}

export { cli_init, DB, CO, DayInSecs };
