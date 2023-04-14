import { readFileSync, unlinkSync, existsSync } from 'fs';
import Database from 'better-sqlite3';
import request, { gql } from 'graphql-request';
import { json } from 'stream/consumers';
import Web3 from 'web3';
import fs from 'fs/promises';

function createDB(db: any) {
    let sql = `CREATE TABLE IF NOT EXISTS uniswapv2_pairs (
        id VARCHAR(42) NOT NULL PRIMARY KEY,
        token0_symbol VARCHAR(10) NOT NULL,
        token1_symbol VARCHAR(10) NOT NULL,
        reserve0 REAL NOT NULL,
        reserve1 REAL NOT NULL,
        reserve_usd REAL NOT NULL,
        volume_token0 REAL NOT NULL,
        volume_token1 REAL NOT NULL,
        volume_usd REAL NOT NULL,
        tx_count BIGINT NOT NULL,
        created_at_timestamp BIGINT NOT NULL
    );
    `;
    db.exec(sql);
}

async function getPairCount(block: number) {
    const query = gql`
        query PairCountAtBlock($blockNumber: Int!) {
          uniswapFactories(block: { number: $blockNumber }) {
            pairCount
          }
        }
    `;
    const variables = {
        blockNumber: block
    };
    const data = await request("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2", query, variables);
    const jsonStr = JSON.stringify(data, undefined, 2);
    const json = JSON.parse(jsonStr);
    return json.uniswapFactories[0].pairCount;

}

async function getLatestBlock() {
    const publicNodeUrl = 'https://eth.llamarpc.com';
    const web3 = new Web3(new Web3.providers.HttpProvider(publicNodeUrl));
    const blockNumber = await web3.eth.getBlockNumber();
    return blockNumber;
}

async function getPairs(block: number, first: number, lastId: string) {
    const query = gql`
        query Query($block: Int!, $first: Int!, $lastId: String!){
            pairs(block: { number: $block } first: $first where: { id_gt: $lastId }  orderBy: id, orderDirection: asc) {
              id
              token0 {
                symbol
              }
              token1 {
                symbol
              }
              reserve0
              reserve1
              reserveUSD
              volumeToken0
              volumeToken1
              volumeUSD
              txCount
              createdAtTimestamp
            }
        }
    `;
    const variables = {
        block: block,
        first: first,
        lastId: lastId
    };
    const data = await request("https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2", query, variables);
    const jsonStr = JSON.stringify(data, undefined, 2);
    const json = JSON.parse(jsonStr);
    return json.pairs;
}

//{
//  "id": "0x0000871c95bb027c90089f4926fd1ba82cdd9a8b",
//  "token0": {
//    "symbol": "HORE"
//  },
//  "token1": {
//    "symbol": "WETH"
//  },
//  "reserve0": "0.000000000000007155",
//  "reserve1": "0.000000000000000141",
//  "reserveUSD": "0.00000000000009764515173366604499968328796917891",
//  "volumeToken0": "1136.139076109248718343",
//  "volumeToken1": "23.591009907456922402",
//  "volumeUSD": "0",
//  "txCount": "37",
//  "createdAtTimestamp": "1601773155"
//},
function saveToDB(db: any, pairs: any) {
    for (const pair of pairs) {
        const stmt = db.prepare(
            `INSERT INTO uniswapv2_pairs (id, token0_symbol, token1_symbol,
                reserve0, reserve1, reserve_usd, volume_token0, volume_token1, volume_usd, tx_count,
                created_at_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        );
        stmt.run(
            pair.id,
            pair.token0.symbol,
            pair.token1.symbol,
            parseFloat(pair.reserve0),
            parseFloat(pair.reserve1),
            parseFloat(pair.reserveUSD),
            parseFloat(pair.volumeToken0),
            parseFloat(pair.volumeToken1),
            parseFloat(pair.volumeUSD),
            parseInt(pair.txCount),
            parseInt(pair.createdAtTimestamp),
        );
    }

}

async function deleteFileIfExists(filePath: string): Promise<void> {
    try {
        await fs.access(filePath);
        await fs.unlink(filePath);
        console.log('File deleted:', filePath);
    } catch (error: any) {
        if (error.code === 'ENOENT') {
            console.log('File does not exist:', filePath);
        } else {
            console.error('Error deleting file:', error);
        }
    }
}

async function fetchData(func: any, block: any, first: any, lastId: any) {
    let retries = 10;

    while (retries > 0) {
        try {
            return await func(block, first, lastId);
        } catch (error) {
            console.log('Retrying due to conflict with recovery...');
            retries -= 1;
            await new Promise((resolve) => setTimeout(resolve, 10000)); // Wait for 3 seconds before retrying
        }
    }

    throw new Error('Failed to fetch data after retries');
}

async function main() {
    const block = await getLatestBlock() - 5;
    console.log(block);

    deleteFileIfExists(`db.${block}`)
    const db = new Database(`db.${block}`, { verbose: console.log });
    createDB(db);

    const pairCount = await getPairCount(block);
    console.log(pairCount);

    let lastId = ""
    const first = 1000;
    let progress = 0;
    while (true) {
        const pairs = await fetchData(getPairs, block, first, lastId);
        if (pairs.length === 0) {
            break;
        }
        progress += first;
        lastId = pairs[pairs.length - 1].id;
        console.log(`progress: ${progress}/${pairCount}`);
        saveToDB(db, pairs);
    }
}

main()
    .then(data => console.log(data))
    .catch(error => console.error(error));
