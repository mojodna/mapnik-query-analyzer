"use strict";

var fs = require("fs"),
    path = require("path"),
    util = require("util");

var async = require("async"),
    escape = require("pg-escape"),
    holdtime = require("holdtime"),
    mapnik = require("mapnik"),
    mercator = new (require("sphericalmercator"))(),
    pg = require("pg"),
    ProgressBar = require("progress"),
    wkx = require("wkx");

// TODO Usage:
// TODO allow an individual layer to be specified
var argv = process.argv.slice(2),
    filename = argv.shift();

mapnik.register_default_input_plugins();

var COUNT_VERTICES = false;

var SUPPORTED_TYPES = [
  "postgis",
  "pgraster" // NOTE: overviews aren't supported
];


var tiles = [
  [20, 49, 7]
  // [655, 1583, 12]
  // [1310, 3166, 13]
];

var processLayer = function(map, tile, layer, callback) {
  var ds = layer.datasource,
      params = ds.parameters(),
      description = layer.describe(),
      active = description.status === undefined ? true : description.status,
      minZoom = description.minzoom === undefined ? 0 : description.minzoom,
      maxZoom = description.maxzoom === undefined ? Infinity : description.maxzoom,
      scaleDenominator = map.scaleDenominator();

  if (active &&
      SUPPORTED_TYPES.indexOf(params.type) >= 0 &&
      scaleDenominator >= minZoom &&
      scaleDenominator <= maxZoom) {

    var bufferedExtent = map.bufferedExtent,
        geometryColumn = params.geometry_field || params.raster_field,
        simplify = params.simplify_geometries === "true",
        bboxToken = util.format("ST_SetSRID('BOX3D(%d %d 0, %d %d 0)'::box3d, 3857)", bufferedExtent[0], bufferedExtent[1], bufferedExtent[2], bufferedExtent[3]),
        subQuery = params.table
          .replace(/!bbox!/ig, bboxToken)
          .replace(/!scale_denominator!/ig, scaleDenominator)
          .replace(/!pixel_width!/ig, map.scale()) // TODO is this right?
          .replace(/!pixel_height!/ig, map.scale()),
        simplifyBegin = "",
        simplifyEnd = "";

    if (simplify) {
      simplifyBegin = "ST_Simplify(";
      // see https://github.com/mapnik/mapnik/issues/1639
      simplifyEnd = escape(", %s)", map.scale() / 20);
    }

    // TODO fetch property names from matching styles
    // (this needs to be done before tracking data transfer is worthwhile)
    var query = escape("SELECT *, ST_AsBinary(%s%I%s) AS geom FROM %s", simplifyBegin, geometryColumn, simplifyEnd, subQuery);

    if (!/!bbox!/.test(params.table)) {
      query += escape(" WHERE %I && %s", geometryColumn, bboxToken);
    }

    // TODO support row_limit

    // TODO check if styles match (via min/max scale denominator)

    return pg.connect({
      user: params.user,
      database: params.dbname,
      password: params.password,
      port: params.port,
      host: params.host
    }, function(err, client, done) {
      if (err) {
        return callback(err);
      }

      return client.query(query, holdtime(function(err, result, elapsedMS) {
        done();

        if (err) {
          return callback(err);
        }

        var vertexCount = 0;

        if (COUNT_VERTICES && result.rowCount > 0 && params.type === "postgis") {
          result.rows.forEach(function(row) {
            try {
              var wkb = row.geom,
                  geometry = wkx.Geometry.parse(wkb),
                  wkt = geometry.toWkt();

              vertexCount += wkt.split(",").length; // cheap way to count vertices
            } catch (err) {
              console.warn(err);
              // console.warn(params.table);
            }
          });
        }

        // TODO track data transferred using the pg connection (before doing
        // this make sure that we're limiting columns selected)

        // TODO render as a series of bar charts using node-canvas

        return callback(null, {
          name: layer.name,
          table: params.table,
          rows: result.rowCount,
          vertexCount: vertexCount,
          avgVertices: Math.round(vertexCount / result.rowCount),
          time: elapsedMS,
          query: query
        });
      }));
    });

    // TODO respect layer-specific buffer (not exposed via node-mapnik)
  }

  // TODO check for layer-specific buffer size (unused by us to-date)

  return callback();
};

var processTile = function(map, tile, callback) {
  // var z = tile[2];

  var bbox = mercator.bbox(tile[0], tile[1], tile[2], false, "900913");

  // for manually determining buffers
  // var minPx = mercator.px([bbox[0], bbox[1]], z),
  //     maxPx = mercator.px([bbox[2], bbox[3]], z);

  // console.log("sw px:", minPx);
  // console.log("ne px:", maxPx);

  map.zoomToBox(bbox);

  var layers = map.layers(),
      bar = new ProgressBar("[:bar] :percent :etas", {
        complete: "=",
        incomplete: " ",
        total: layers.length
      });

  return async.mapSeries(layers, function(layer, next) {
    return processLayer(map, tile, layer, function() {
      bar.tick();

      return next.apply(null, arguments);
    });
  }, function(err, results) {
    if (err) {
      return callback(err);
    }

    results = results.filter(function(x) {
      return !!x;
    });

    results
      .sort(function(a, b) {
        return a.time - b.time;
      })
      .reverse()
      .slice(0, 5)
      .forEach(function(layer, idx) {
        console.log("%d. ", idx + 1, layer.name);
        console.log("    Time: %dms", layer.time.toFixed(2));
        console.log("    Rows:", layer.rows);

        if (COUNT_VERTICES) {
          console.log("    Vertices:", layer.vertexCount);
          console.log("    Average vertices/feature:", layer.avgVertices);
        }

        // TODO if verbose
        console.log(layer.table.split("\n").map(function(x) {
          // indent
          return "    " + x;
        }).join("\n"));

        console.log(layer.query.split("\n").map(function(x) {
          // indent
          return "    " + x;
        }).join("\n"));

        console.log();
      });

    return callback(null, results);
  });
};

var style = fs.readFileSync(filename, "utf8");

var map = new mapnik.Map(256, 256);

map.fromStringSync(style, {
  strict: true,
  base: path.dirname(filename)
});

async.eachSeries(tiles, async.apply(processTile, map), function(err) {
  if (err) {
    throw err;
  }

  pg.end();
});

