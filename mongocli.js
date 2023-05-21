import { MongoClient } from "mongodb";
import dotenv from "dotenv";

async function cli_init(bClean) {
  dotenv.config();
  const connURI = proecess.env.ATLAS_CONN_URI ?? "";
  const DB = "musinsa";
  const CO = "msgcol";
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

  if (bExist) {
    console.log(`+++ collection exists: "${DB}.${CO}"`);
    msgcol = db.collection(CO);
  } else {
    console.log(`+++ create collection: "${DB}.${CO}"`);
    const col = await cli.db(DB).createCollection(CO, {
      clusteredIndex: {
        key: { createDate: 1 },
        unique: true,
        name: "msg create timestamp & TTL",
      },
    });
    msgcol = col;
  }

  return msgcol;
}

export { cli_init };
