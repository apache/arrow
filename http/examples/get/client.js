const Arrow = require('apache-arrow');

const url = 'http://localhost:8000';

async function runExample(url) {
  const startTime = new Date();
  
  const table = await Arrow.tableFromIPC(fetch(url));
  
  const duration = (new Date() - startTime) / 1000;
  console.log(`${table.batches.length} record batches received`);
  console.log(`${duration.toFixed(2)} seconds elapsed`);
}

runExample(url);
