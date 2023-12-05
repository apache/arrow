const Arrow = require('apache-arrow');

const url = 'http://localhost:8000';

async function getArrowData(url) {
  try {
    const table = await Arrow.tableFromIPC(fetch(url))
    return table;
  } catch (error) {
    console.error('Error:', error.message);
  }
}

const startTime = new Date();

getArrowData(url)
  .then(table => {
    const endTime = new Date();
    const duration = (endTime - startTime) / 1000;
    console.log(`${table.batches.length} record batches received`);
    console.log(`${duration.toFixed(2)} seconds elapsed`);
  })
  .catch(error => {
    console.error('Error:', error.message);
  });
