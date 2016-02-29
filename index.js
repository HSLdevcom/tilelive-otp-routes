"use strict"
const geojsonVt = require('geojson-vt');
const vtPbf = require('vt-pbf');
const request = require('requestretry');
const zlib = require('zlib');
const groupBy = require('lodash.groupby');
const zip = require('lodash.zip');


const query = `
  query trips{
    trips{
      gtfsId
      shapeId
      route{
        type
        shortName
        gtfsId
      }
    }
  }`;


const tripQuery = id => `
  query shape{
    trip(id: "${id}") {
      geometry
    }
  }`;

const tripRequiest = (uri, gtfsId) => ({
  url: uri,
  body: tripQuery(gtfsId),
  method: 'POST',
  headers: {
    'Content-Type': 'application/graphql'
  },
  fullResponse: false
})

const toFeature = bundle => {
 return({
  type: "Feature",
  geometry: {type: "LineString", coordinates: JSON.parse(bundle[2]).data.trip.geometry},
  properties: Object.assign({}, {id: bundle[0]}, bundle[1])
})}

class GeoJSONSource {
  constructor(uri, callback){
    uri.protocol = "http:"
    request({
      url: uri,
      body: query,
      maxAttempts: 20,
      retryDelay: 30000,
      method: 'POST',
      headers: {
        'Content-Type': 'application/graphql'
      },
      fullResponse: false
    })
    .then((body) => {
      const shapes = groupBy(JSON.parse(body).data.trips, trip => trip.shapeId)
      const shapeIds = Object.keys(shapes)
      const shapePromises = shapeIds.map(shapeId => request(tripRequiest(uri, shapes[shapeId][0].gtfsId)))

      Promise.all(shapePromises).then(geometries => {
        const shapeBundles = zip(shapeIds, shapeIds.map(shapeId => shapes[shapeId][0].route), geometries)
        const geoJSON = {type: "FeatureCollection", features: shapeBundles.map(toFeature)}
        this.tileIndex = geojsonVt(geoJSON, {maxZoom: 20, buffer: 512}); //TODO: this should be configurable
        console.log("all ready")
        callback(null, this)
      }).catch((err) => {
        console.log(err)
        callback(err);
      })
    })
    .catch((err) => {
      console.log(err)
      callback(err);
    })
  };

  getTile(z, x, y, callback){
    let tile = this.tileIndex.getTile(z, x, y)

    if (tile === null){
      tile = {features: []}
    }

    zlib.gzip(vtPbf.fromGeojsonVt({routes: tile}), function (err, buffer) {
      if (err){
        callback(err);
        return;
      }

      callback(null, buffer, {"content-encoding": "gzip"})
    })
  }

  getInfo(callback){
    callback(null, {
      format: "pbf",
      maxzoom: 20,
      minzoom: 0,
      scheme: "tms",
      vector_layers: [{
        description: "",
        id: "routes"
      }]
    })
  }
}

module.exports = GeoJSONSource

module.exports.registerProtocols = (tilelive) => {
  tilelive.protocols['otproutes:'] = GeoJSONSource
}
