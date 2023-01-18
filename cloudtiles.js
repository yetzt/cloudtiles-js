var cloudtiles = function cloudtiles(src) {
	if (!(this instanceof cloudtiles)) return new cloudtiles(src);
	var self = this;

	self.format = [ "png", "jpeg", "webp", "svg", "avif", null, null, null, null, null, null, null, "geojson", "topojson", "json", "bin", "pbf" ];
	self.compression = [ null, "gzip", "br" ];

	self.mime = {
		png: "image/png",
		jpeg: "image/jpeg",
		webp: "image/webp",
		svg: "image/svg+xml",
		avif: "image/avif",
		geojson: "application/geo+json",
		topojson: "application/topo+json",
		json: "application/json",
		bin: "application/octet-stream",
		pbf: "application/x-protobuf",
	};

	self.src = src;
	self.header = null;
	self.meta = null;
	self.index = null;
	self.zoom = null;
	self.bbox = null;
	
	return this;
};

// read from http(s)
cloudtiles.prototype.read = function(position, length, fn){
	var self = this;
		
	fetch(self.src, {
		headers: { "Range": "bytes=" + position.toString() + "-" + (position+length).toString() }
	}).then(function(resp){
		if (!resp.ok) return fn(new Error("Server replied with HTTP Status Code "+resp.status));
		resp.arrayBuffer().then(function(buf){
			fn(null, buf);
		}).catch(function(err){
			fn(err);
		});
	}).catch(function(err){
		fn(err);
	});
	return self;
};

// get header
cloudtiles.prototype.getHeader = function(fn){
	var self = this;

	// deliver if known
	if (self.header !== null) return fn(null, self.header), self;

	// FIXME: get magic bytes first, then read whole header based on version
	self.read(0, 62, function(err, data){
		if (err) return fn(err);
		var view = new DataView(data);

		try {
			self.header = {
				magic: String.fromCharCode.apply(null, new Uint8Array(data.slice(0,28))),
				tile_format: self.format[view.getUint8(28)]||"bin",
				tile_precompression: self.compression[view.getUint8(29)]||null,
				meta_offset: view.getBigUint64(30),
				meta_length: view.getBigUint64(38),
				block_index_offset: view.getBigUint64(46),
				block_index_length: view.getBigUint64(54),
			};
		} catch (err) {
			return fn(err);
		}

		fn(null, self.header);

	});

	return self;
};

// get metadata
cloudtiles.prototype.getMeta = function(fn){
	var self = this;

	// deliver if known
	if (self.meta !== null) return fn(null, self.meta), self;

	self.getHeader(function(err){
		if (err) return fn(err);

		self.read(self.header.meta_offset, self.header.meta_length, function(err, data){ // read meta buffer

			self.decompress("br", data, function(err, data){
				if (err) return fn(err);

				var blob = new Blob([ data ]);
				blob.text().then(function(text){

					try {
						self.meta = JSON.parse(text);
					} catch (err) {
						self.meta = {}; // empty
					}

					return fn(null, self.meta);

				}).catch(function(err){
					fn(err);
				});

			});
			
		});

	});

	return self;
};

// get block index
cloudtiles.prototype.getBlockIndex = function(fn){
	var self = this;

	// deliver if known
	if (self.index !== null) return fn(null, self.index), self;

	self.getHeader(function(err){
		if (err) return fn(err);

		self.read(self.header.block_index_offset, self.header.block_index_length, function(err, data){ // read block_index buffer
			if (err) return fn(err);

			self.decompress("br", data, function(err, data){
				if (err) return fn(err);


				var view = new DataView(data);
			
				// read index from buffer
				var index = [];
				for (var i = 0; i < (view.byteLength/29); i++) {
					index.push({
						level: view.getUint8(0+i*29),
						column: view.getUint32(1+i*29),
						row: view.getUint32(5+i*29),
						col_min: view.getUint8(9+i*29),
						row_min: view.getUint8(10+i*29),
						col_max: view.getUint8(11+i*29),
						row_max: view.getUint8(12+i*29),
						tile_index_offset: view.getBigUint64(13+i*29),
						tile_index_length: view.getBigUint64(21+i*29),
						tile_index: null,
					});
				}

				// filter invalid blocks and sort by z, y, x
				index = index.filter(function(b){
					return (b.col_max >= b.col_min && b.row_max >= b.row_min); // these shouldn't exist
				}).sort(function(a,b){
					if (a.level !== b.level) return (a.level - b.level);
					if (a.column !== b.column) return (a.column - b.column);
					return (a.row - b.row);
				});

				// build hierarchy
				self.index = index.reduce(function(i,b){
					if (!i.hasOwnProperty(b.level)) i[b.level] = {};
					if (!i[b.level].hasOwnProperty(b.column)) i[b.level][b.column] = {};
					i[b.level][b.column][b.row] = b;
					return i;
				},{});

				return fn(null, self.index);

			});
		});
	});

	return self;
};

// get tile index for block
cloudtiles.prototype.getTileIndex = function(block, fn){
	var self = this;
	if (block.tile_index !== null) return fn(null, block.tile_index), self;
	self.read(block.tile_index_offset, block.tile_index_length, function(err, data){ // read tile_index buffer

		self.decompress("br", data, function(err, data){
			if (err) return fn(err);
			block.tile_index = new DataView(data); // keep as DataView, decode on demand
			return fn(null, block.tile_index);
		});

	});
	return self;
};

// get tile by zxy
cloudtiles.prototype.getTile = function(z, x, y, fn){
	const self = this;

	// ensure block index is loaded
	self.getBlockIndex(function(err){
		if (err) return fn(err);

		// tile xy (within block)
		const tx = x%256;
		const ty = y%256;

		// block xy
		const bx = ((x-tx)/256);
		const by = ((y-ty)/256);

		// check if block containing tile is within bounds
		if (!self.index.hasOwnProperty(z)) return fn(new Error("Invalid Z"));
		if (!self.index[z].hasOwnProperty(bx)) return fn(new Error("Invalid X"));
		if (!self.index[z][bx].hasOwnProperty(by)) return fn(new Error("Invalid Y"));

		const block = self.index[z][bx][by];

		// check if block contains tile
		if (tx < block.col_min || tx > block.col_max) return fn(new Error("Invalid X within Block"));
		if (ty < block.row_min || ty > block.row_max) return fn(new Error("Invalid Y within Block"));

		// calculate sequential tile number
		const j = (ty - block.row_min) * (block.col_max - block.col_min + 1) + (tx - block.col_min);

		// get tile index
		self.getTileIndex(block, function(err){
			if (err) return fn(err);

			const tile_offset = block.tile_index.getBigUint64(12*j);
			const tile_length = BigInt(block.tile_index.getUint32(12*j+8)); // convert to bigint so range request can be constructed

			self.read(tile_offset, tile_length, function(err, data){
				if (err) return fn(err);

				// decompress
				self.decompress(self.header.tile_precompression, data, function(err, data){

					var blob = new Blob([ new Uint8Array(data) ], { type: self.mime[self.header.tile_format] });

					return fn(null, blob);
				
				});
				
			});

		});

	});

	return self;
};


// get zoom levels
cloudtiles.prototype.getZoomLevels = function(fn){
	var self = this;

	// deliver if known
	if (self.zoom !== null) return fn(null, self.zoom), self;

	self.getBlockIndex(function(err){
		if (err) return fn(err);

		self.zoom = Object.keys(self.index).sort(function(a,b){
			return a.localeCompare(b, undefined, { numeric: true });
		});

		return fn(null, self.zoom);

	});

	return self;
};

// get approximate bbox for highest zoom level (lonlat; w, s, e, n)
cloudtiles.prototype.getBoundingBox = function(fn){
	var self = this;

	// deliver if known
	if (self.bbox !== null) return fn(null, self.bbox), self;

	self.getZoomLevels(function(err, zoom){
		if (err) return fn(err);

		// get max zoom level
		// assumption: highest zoom tileset delivers the most detailed bounding box
		var z = "10";// zoom.pop();

		// get min and max x
		var xr = Object.keys(self.index[z]).sort(function(a,b){
			return a.localeCompare(b, undefined, { numeric: true });
		});
		var xmin = xr[0];
		var xmax = xr[xr.length-1];

		// get min and max y
		// assumption: extent is the same on every block (tileset is "rectangular")
		var yr = Object.keys(self.index[z][xmin]).sort(function(a,b){
			return a.localeCompare(b, undefined, { numeric: true });
		});

		var ymin = yr[0];
		var ymax = yr[yr.length-1];

		// convert to tile ids;
		var txmin = ((parseInt(xmin,10)*256)+self.index[z][xmin][ymin].col_min);
		var txmax = ((parseInt(xmin,10)*256)+self.index[z][xmin][ymin].col_max)+1; // use "next" tile to include all tiles
		var tymin = ((parseInt(ymax,10)*256)+self.index[z][xmax][ymax].row_max)+1;
		var tymax = ((parseInt(ymin,10)*256)+self.index[z][xmin][ymin].row_min);

		// convert to coordinates:
		self.bbox = [
			...self._zxy_ll(parseInt(z,10), txmin, tymin),
			...self._zxy_ll(parseInt(z,10), txmax, tymax),
		];

		return fn(null, self.bbox);

	});

	return self;
};

// helper zxy → lonlat
cloudtiles.prototype._zxy_ll = function(z,x,y){
	var n = Math.PI - 2 * Math.PI * y / Math.pow(2, z);
	return [
		(x / Math.pow(2, z) * 360 - 180), // lon
		(180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)))), // lat
	];
};

// decompression
cloudtiles.prototype.decompress = async function(encoding, data, fn){
	if (!encoding) return fn(null, data);

	switch (encoding) {
		case "br":
			try {
				return fn(null, brotli(new Uint8Array(data)).buffer);
			} catch (err) {
				return fn(err);
			}
		break;
		case "gzip":
			try {
				return fn(null, fflate.gunzipSync(new Uint8Array(data)).buffer);
			} catch (err) {
				return fn(err);
			}
		break;
	};

	return this;
};

var ct = cloudtiles("http://localhost/cloudtiles/data/hitzekarte.cloudtiles").getMeta(console.log).getTile(9, 274, 170, function(err, tile){
	if (err) return console.error(err);
	document.querySelector("#tile").src = (window.URL || window.webkitURL).createObjectURL(tile);
});