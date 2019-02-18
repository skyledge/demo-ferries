const request = require('request');
const GtfsRealtimeBindings = require('gtfs-realtime-bindings');
const _ = require('lodash');
const config = require('./config');

function loop() {
  retrieveTransitData();
}

function retrieveTransitData() {
  request(transitDataRequest(), function(error, response, body) {
    if (!error && response.statusCode == 200) {
      const parsedData = GtfsRealtimeBindings.FeedMessage.decode(body);      
      const vehicleDataArray = parseTransitData(parsedData.entity);
      vehicleDataArray.forEach(vehicle => postToSkyledge(vehicle));      
    } else {
      console.log('Error retrieving data');
    }
  });
}

function postToSkyledge(vehicleData) {
  request(skyLedgeRequest(
    formatRequestJson(vehicleData)
  ), function(error, response, body) {
    if (error || response.statusCode !== 202) {
      console.log('failed to post', response ? response.statusCode: '');
    }
  });
}

function formatRequestJson(vehicleData) {
  return {
    // the location of the ferry, in geojson format
    location: {
      type: 'Point',
      coordinates: [
        vehicleData.getPosition().getLongitude().toFixed(5),
        vehicleData.getPosition().getLatitude().toFixed(5) 
      ]
    },
    // the dynamic attributes defined in the Event Type
    dynamicAttributes: {
      name: vehicleData.getVehicle().getLabel(),
      id: vehicleData.getVehicle().getId(),
      speed: vehicleData.getPosition().getSpeed().toFixed(2)
    }
  }
}

function skyLedgeRequest(jsonBody) {
  return {
    method: 'POST',
    url: config.eventEndpoint,
    headers: {
      'X-Authorization': config.skyLedgeApiKey
    },
    json: jsonBody
  };
}

// this function parses the messages received by the API. As we may receive multiple messages
// per vehicle, we take only the last message received for each vehicle.
function parseTransitData(entityArray) {
  return _(entityArray)
  .filter(e => e.getVehicle())     // ignore any messages without vehicle data
  .map(e => e.getVehicle())        // retrieve only the vehicle message
  .sort((v1, v2) => 
    v1.timestamp.toNumber() - v2.timestamp.toNumber())  // sort by timestamp, ascending
  .groupBy(v => v.getVehicle().id)  // we get multiple messages for each vehicle, so group by vehicle id
  .map(group => group.pop())        // take only the latest message for each vehicle
  .value();
}

function transitDataRequest() {
  return {
    method: 'GET',
    // encoding: null forces the return type to be a buffer, required by the gtfs module
    encoding: null,
    headers: {
      "Authorization" : `apikey ${config.transportApiKey}`
    },
    url: 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/ferries'
  };
}

// calls the function once immediately, then polls every 5 minutes
loop();
setInterval(loop, 300000);