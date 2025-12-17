import { chromium } from "playwright";
import fs from "fs";
import { DuckDBConnection } from "@duckdb/node-api";
import { parse } from "node-html-parser";

const userDataDir = "./.userData";
const targetDir = "data";

async function fetchPeers(asNumbers) {
  // Setup
  await fs.promises.mkdir(targetDir, { recursive: true });
  const context = await chromium.launchPersistentContext(userDataDir, {
    headless: true,
  });

  const proms = [];
  for (const asNumber of asNumbers) {
    const page = await context.newPage();
    proms.push(getPeers(page, asNumber));
  }

  const isps = await Promise.all(proms);

  await fs.promises.writeFile(ispFile(), JSON.stringify(isps, null, 2));

  // await doAnalyze(asNumber);

  // Teardown
  // await page.pause();
  await context.close();
}

async function analyze(asNumbers, preferredCountries) {
  const conn = await DuckDBConnection.create();
  const proms = [];
  for (const asNumber of asNumbers) {
    proms.push(doAnalyze(conn, asNumber, preferredCountries));
  }
  await Promise.all(proms);
  conn.closeSync();
}

function getURL(asNumber) {
  return `https://bgp.he.net/AS${asNumber}#_peers`;
}

function peersFile(asNumber) {
  return `${targetDir}/peers-${asNumber}.json`;
}

function ispFile() {
  return `${targetDir}/isp.json`;
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

  await fs.promises.writeFile(
    peersFile(asNumber),
    JSON.stringify(peers, null, 2),
  );

  return { asNumber, name: isp, timestamp: Date.now() };
}

async function doAnalyze(conn, asNumber, preferredCountries) {
  const isp = await ispName(conn, asNumber);
  const peersCount = await countPeers(conn, asNumber);
  const countriesCount = await countCountries(conn, asNumber);
  const preferredCountriesCount = await countPreferredCountries(
    conn,
    asNumber,
    preferredCountries,
  );
  const countries = await getCountries(conn, asNumber);

  console.log("");
  console.log(isp.name, timestampString(isp.timestamp));
  console.log(`==> ${peersCount} peers`);
  console.log(`==> ${countriesCount} countries`);
  console.log(`==> ${preferredCountriesCount} preferred countries`);
  console.log(`==> Countries`);
  for (const c of countries) {
    console.log(`    ${c.country}: ${c.jml}`);
  }
}

const timestampString = (timestamp) =>
  new Date(parseInt(timestamp)).toLocaleString("id-ID");

async function ispName(conn, asNumber) {
  const query = `SELECT name, timestamp FROM read_json('${ispFile()}') WHERE asNumber = ${asNumber}`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects[0];
}

async function countPeers(conn, asNumber) {
  const query = `SELECT COUNT(*) AS jml FROM read_json('${peersFile(asNumber)}')`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects[0].jml;
}

async function countCountries(conn, asNumber) {
  const query = `SELECT COUNT(DISTINCT country) AS jml FROM read_json('${peersFile(asNumber)}')`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects[0].jml;
}

async function countPreferredCountries(conn, asNumber, preferredCountries) {
  const query = `SELECT COUNT(DISTINCT country) AS jml FROM read_json('${peersFile(asNumber)}') WHERE country IN (${preferredCountries.map((country) => `'${country}'`).join(", ")})`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects[0].jml;
}

async function getCountries(conn, asNumber) {
  const query = `SELECT country, COUNT(*) AS jml FROM read_json('${peersFile(asNumber)}') GROUP BY country ORDER BY jml DESC`;
  const result = await conn.runAndReadAll(query);
  const rowObjects = result.getRowObjectsJson();
  return rowObjects;
}

const asNumbers = [
  7713, // Telkom
  4761, // Indosat
  23671, // SIMS
];

const preferredCountries = [
  "Pakistan",
  "Thailand",
  "Czech Republic",
  "Nigeria",
  "Timor-Leste",
  "Malaysia",
  "Tajikistan",
  "Ethiopia",
  "Sudan",
];

// await fetchPeers(asNumbers);
await analyze(asNumbers, preferredCountries);
