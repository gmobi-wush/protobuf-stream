var fs = require('fs');
var protobuf = require('protocol-buffers');
var pbstream = require(__dirname + "/../index.js");
var es = require('event-stream');
var tempfile = require('tempfile');
var async = require('async');
var assert = require('assert');
var stream = require('stream');

describe("createWriteStream", function() {
	var path = tempfile(".pbs");
	it("Test write and read", function(done) {
		var schema = protobuf(fs.readFileSync(__dirname + "/proto/test.proto")).Test;
		var objs = [], bufs = [];
		async.series([
			function(done) {
				var output = fs.createWriteStream(path);
				fs.createReadStream(__dirname + "/ndjson/test.ndjson")
				.pipe(es.split())
				.on("error", done)
				.pipe(es.parse({error:true}))
				.on("error", done)
				.pipe(es.map(function(obj, cb) {
					console.log("Parsing: " + JSON.stringify(obj));
					objs.push(obj);
					cb(null, obj);
				}))
				.pipe(pbstream.createWriteStream(schema))
				.on("error", done)
				.pipe(es.map(function(buf, cb) {
					bufs.push(buf);
					cb(null, buf);
				}))
				.pipe(output)
				.on("error", done);
				output.on("close", done);
			},
			function(done) {
				assert(fs.lstatSync(path).size > 0);
				done();
			},
			function(done) {
				var refs = JSON.parse(JSON.stringify(objs));
				var input = fs.createReadStream(path);
				var i = 0;
				fs.createReadStream(path)
				.pipe(pbstream.createReadStream(schema))
				.on("error", done)
				.pipe(es.map(function(obj, cb) {
					try {
						var ref = refs.shift();
						assert(JSON.stringify(obj) === JSON.stringify(ref));
						cb(null, obj);
					} catch (ex) {
						cb(ex);
					}
				}))
				.on("error", done)
				.on("error", done)
				.on("end", function() {
					assert(refs.length === 0);
					done();
				});
			},
			function(done) {
				var refs = JSON.parse(JSON.stringify(objs));
				var input = fs.createReadStream(path);
				var i = 0;
				var byteSplitter = new stream.Transform();
				byteSplitter._transform = function(chunk, enc, callback) {
					if (!chunk) {
						byteSplitter.push(null);
						return callback();
					}
					for(var i = 0;i < chunk.length;i++) {
						byteSplitter.push(chunk.slice(i, i+1));
					}
					return callback();
				}
				fs.createReadStream(path)
				.pipe(byteSplitter)
				.on("error", done)
				.pipe(pbstream.createReadStream(schema))
				.on("error", done)
				.pipe(es.map(function(obj, cb) {
					try {
						var ref = refs.shift();
						assert(JSON.stringify(obj) === JSON.stringify(ref));
						cb(null, obj);
					} catch (ex) {
						cb(ex);
					}
				}))
				.on("error", done)
				.on("error", done)
				.on("end", function() {
					assert(refs.length === 0);
					done();
				});
			}
		], done);
	});
});
