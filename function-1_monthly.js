const fetch = require('node-fetch');
const { PubSub } = require('@google-cloud/pubsub');

const pubsub = new PubSub();
const topicName = 'PS_to_BQ_monthly';

exports.invokeApiAndPublishToPubSub = async (req, res) => {
  try {
    const structuredDataList = [];
    const techsymbols = req.body.symbols;

    for (const symbol of techsymbols) {
      const apiUrl = `https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=${symbol}&apikey=LIKUB79HU5XJSYC4`;
      const response = await fetch(apiUrl);
      const rawData = await response.json();

      // Transform data with start and end date limits
      const transformedDataList = transformJsonToStructured(rawData, symbol);
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
  const monthlyTimeSeriesData = json_data["Monthly Time Series"];
  const structuredDataList = [];

  // Check if monthlyTimeSeriesData is null or undefined
  if (!monthlyTimeSeriesData) {
    return structuredDataList;
  }

  // Convert start and end dates to Date objects
  const startDateLimit = new Date('2015-01-01');
  const endDateLimit = new Date('2023-11-30');

  for (const [timestamp, values] of Object.entries(monthlyTimeSeriesData)) {
    // Skip entry if timestamp is null or not a valid date
    if (!timestamp || timestamp.toLowerCase() === 'null') {
      continue;
    }

    // Convert timestamp to a standard format
    const timestampFormatted = new Date(timestamp);

    // Check if the timestamp is within the specified date range
    if (isNaN(timestampFormatted.getTime()) || timestampFormatted < startDateLimit || timestampFormatted > endDateLimit) {
      continue; // Skip entry if outside the date range or not a valid date
    }

    // Transformation logic
    const structuredData = {
      date: timestamp,
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