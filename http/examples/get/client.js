const Arrow = require('apache-arrow');

const url = 'http://localhost:8000';

async function getArrowData(url) {
  try {
    const response = await fetch(url);
    const table = await Arrow.tableFromIPC(response);
    return table;
  } catch (error) {
    console.error('Error:', error.message);
    throw error;
  }
}

async function runExample(url) {
  const startTime = new Date();
  try {
    const table = await getArrowData(url);
    const endTime = new Date();
    const duration = (endTime - startTime) / 1000;
    console.log(`${table.batches.length} record batches received`);
    console.log(`${duration.toFixed(2)} seconds elapsed`);
  } catch (error) {
    console.error('Error:', error.message);
  }
}

runExample(url);
