const fetch = require('node-fetch');
const { PubSub } = require('@google-cloud/pubsub');

const pubsub = new PubSub();
const topicName = 'PS_to_BQ';

exports.invokeApiAndPublishToPubSub = async (req, res) => {
  try {
    const structuredDataList = [];
    const techsymbols = req.body.symbols;

    for (const symbol of techsymbols) {
      const apiUrl = `https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=${symbol}&interval=5min&apikey=LIKUB79HU5XJSYC4`;
      const response = await fetch(apiUrl);
      const rawData = await response.json();

      // Extract symbol from the API response
      const symbolFromResponse = rawData['Meta Data']['2. Symbol'];

      // Transform data
      const transformedDataList = transformJsonToStructured(rawData, symbolFromResponse);
      structuredDataList.push(...transformedDataList);
    }

    // Publish structured data to Pub/Sub topic
    const topic = pubsub.topic(topicName);
    for (const structuredData of structuredDataList) {
      const dataBuffer = Buffer.from(JSON.stringify(structuredData));
      await topic.publish(dataBuffer);
    }

    res.status(200).send('Data processed and published to Pub/Sub.');
  } catch (error) {
    console.error('Error:', error);
    res.status(500).send('Internal Server Error');
  }
};

const transformJsonToStructured = (json_data, symbol) => {
  const timeSeriesData = json_data["Time Series (5min)"];
  const structuredDataList = [];

  for (const [timestamp, values] of Object.entries(timeSeriesData)) {
    // Convert timestamp to a standard format
    const timestampFormatted = new Date(timestamp);

    // Transformation logic to match BigQuery schema
    const structuredData = {
      event_timestamp: timestampFormatted.toISOString(),
      symbol: symbol,
      open_price: parseFloat(values["1. open"]),
      high_price: parseFloat(values["2. high"]),
      low_price: parseFloat(values["3. low"]),
      close_price: parseFloat(values["4. close"]),
      volume: parseInt(values["5. volume"])
    };

    structuredDataList.push(structuredData);
  }

  return structuredDataList;
};