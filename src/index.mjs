import { chromium } from "playwright";
import fs from "fs";
import { DuckDBConnection } from "@duckdb/node-api";
import { parse } from "node-html-parser";

const userDataDir = "./.userData";

async function main(asNumber) {
  // Setup
  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: true,
  });
  const page = await context.newPage();

  const { isp, peers } = await getPeers(page, asNumber);
  console.log(isp);

  await fs.promises.mkdir("data", { recursive: true });
  await fs.promises.writeFile(
    peersFile(asNumber),
    JSON.stringify(peers, null, 2),
  );
  await analyze(asNumber);

  // Teardown
  // await page.pause();
  await context.close();
}

function getURL(asNumber) {
  return `https://bgp.he.net/AS${asNumber}#_peers`;
}

function peersFile(asNumber) {
  return `data/peers-${asNumber}.json`;
}

const suppressImage = (route) => {
  // Abort requests for images
  if (route.request().resourceType() === "image") {
    return route.abort();
  }
  // Continue for all other requests
  return route.continue();
};

async function getPeers(page, asNumber) {
  const url = getURL(asNumber);
  await page.route("**/*", suppressImage);
  await page.goto(url);
  const isp = await page.locator("#header h1").textContent();
  const trsString = await page.locator("#peers table").innerHTML();
  const root = parse(trsString);
  const trs = root.querySelectorAll("tbody tr");

  const peers = [];
  for (const tr of trs) {
    const rank = tr.querySelector("td:nth-child(1)").textContent;
    const name = tr.querySelector("td:nth-child(2)").textContent;
    const img = tr.querySelector("td:nth-child(2) img");
    const country = img == null ? null : img.getAttribute("alt");
    const ipv6 = tr.querySelector("td:nth-child(3)").textContent;
    const peer = tr.querySelector("td:nth-child(4)").textContent;

    if (country == null) {
      console.log(`Rank ${rank}: country is null`);
    }

    peers.push({ rank: parseInt(rank), name, country, ipv6, peer });
  }

  return { isp, peers };
}

async function getPeer(tr) {
  return await tr.evaluate((el) => {
    const rank = el.querySelector("td:nth-child(1)").textContent;
    const name = el.querySelector("td:nth-child(2)").textContent;
    const country = el.querySelector("td:nth-child(2) img").getAttribute("alt");
    const ipv6 = el.querySelector("td:nth-child(3)").textContent;
    const peer = el.querySelector("td:nth-child(4)").textContent;

    return { rank: parseInt(rank), name, country, ipv6, peer };
  });
}

async function analyze(asNumber) {
  const connection = await DuckDBConnection.create();
  const peersCount = await countPeers(asNumber, connection);
  const countriesCount = await countCountries(asNumber, connection);

  connection.closeSync();

  console.log(`==> ${peersCount} peers`);
  console.log(`==> ${countriesCount} countries`);
}

async function countPeers(asNumber, conn) {
  const query = `SELECT COUNT(*) AS jml FROM read_json('${peersFile(asNumber)}')`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects[0].jml;
}

async function countCountries(asNumber, conn) {
  const query = `SELECT COUNT(DISTINCT country) AS jml FROM read_json('${peersFile(asNumber)}')`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects[0].jml;
}

// Telkom
await main(7713);

// Indosat
await main(4761);

// SIMS
await main(23671);

/*
tailand
ceko
nigeria
etiopia
sudan
tajikistan
pakistan
timor leste
malaysia
*/
