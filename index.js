var protobuf = require('protocol-buffers');
var stream = require('stream');

const DELIMITER = "\n";
const DELIMITER_BUFFER = new Buffer(DELIMITER);

function createSerializer(schema) {
	return function(chunk) {
		var buf = schema.encode(chunk);
		var lengthBuf = new Buffer(4);
		lengthBuf.writeUInt32LE(buf.length, 0);
		return Buffer.concat([lengthBuf, buf, DELIMITER_BUFFER]);
	};
}

module.exports = {
	delimiter : DELIMITER,
	createReadStream : function(schema) {
		var retval = new stream.Transform({objectMode : true});
		var buf = new Buffer(0);
		var size = null;
		retval._transform = function(chunk, enc, callback) {
			try {
				if (!chunk) {
					if (buf.length > 0) {
						var err = new Error("end unexpected");
						retval.emit("error", err);
						return callback(err);
					}
					retval.push(null);
					return callback();
				}
				buf = Buffer.concat([buf, chunk]);
				while(true) {
					if (!size) {
						if (buf.length < 4) {
							return callback();
						}
						size = buf.readInt32LE(0);
					}
					if (buf.length < size + 5) {
						return callback();
					}
					var obj = schema.decode(buf, 4, 4 + size);
					buf = buf.slice(5 + size);
					size = null;
					retval.push(obj);
				}
			} catch (ex) {
				retval.emit("error", ex);
				return callback(ex);
			}
		}
		return retval;
	},
	createSerializer : createSerializer,
	createWriteStream : function(schema) {
		var serialier = createSerializer(schema);
		var retval = new stream.Transform({objectMode : true});
		retval._transform = function(chunk, enc, callback) {
			if (!chunk) {
				retval.push(null);
				return callback();
			}
			try {
				var buf = serialier(chunk);
			} catch (ex) {
				retval.emit("error", ex);
				return callback(ex);
			}
			retval.push(buf);
			return callback();
		};
		return retval;
	}
}
