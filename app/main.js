import { open } from "node:fs/promises";

main();

async function main() {
	const databaseFilePath = process.argv[2];
	const command = process.argv[3];

	const databaseFileHandler = await open(databaseFilePath, "r");

	// assume database have a single page of 4096 bytes
	const { buffer } = await databaseFileHandler.read({
		length: 4096,
		position: 0,
		buffer: Buffer.alloc(4096),
	});

	const db = new Database(buffer);

	if (command === ".dbinfo") {
		const pageSize = db.header.pageSize;
		const page = db.page;

		console.log(`database page size: ${pageSize}`);
		// if it is a leaf page
		// then its number of cells equals to its number of tables
		if (page.header.type === 13) {
			console.log(`number of tables: ${page.header.numberOfCells}`);
		}
	} else if (command === ".tables") {
		const tables = db.page.cells
			.flatMap((cell) => {
				const tableName = cell.record.body.columns[2];
				if (tableName.startsWith("sqlite_")) {
					return [];
				}
				return [tableName];
			})
			.join(" ");
		console.log(tables);
	} else {
		throw `Unknown command ${command}`;
	}
}

class Database {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer;
		this.header = new DatabaseHeader(this.buffer.subarray(0, 100));
		this.page = new Page(buffer, true);
	}
}

class DatabaseHeader {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer;
	}

	get pageSize() {
		return this.buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
	}

	get encoding() {
		return this.buffer.readUInt32BE(56);
	}
}

class Page {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer, isFirst = false) {
		this.originalBuffer = buffer;
		this.buffer = isFirst ? buffer.subarray(100) : buffer;
		this.header = new PageHeader(this.buffer.subarray(0, 8)); // assuming this is a table page (it is 12 on index page)
	}

	get cellPointerArray() {
		const result = [];

		for (let i = 0; i < this.header.numberOfCells; i++) {
			// cell pointer array starts afeter the header (offset 8) and have cell count * 2 bytes os length
			result.push(this.buffer.readUint16BE(8 + i * 2));
		}

		return result;
	}

	get cells() {
		const result = [];
		for (const pointer of this.cellPointerArray) {
			result.push(new Cell(this.originalBuffer.subarray(pointer))); // the offsets are relative to the start of the page
		}
		return result;
	}
}

/**
 * The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages. All multibyte values in the page header are big-endian. The b-tree page header is composed of the following fields:
 * The one-byte flag at offset 0 indicating the b-tree page type.
 * A value of 2 (0x02) means the page is an interior index b-tree page.
 * A value of 5 (0x05) means the page is an interior table b-tree page.
 * A value of 10 (0x0a) means the page is a leaf index b-tree page.
 * A value of 13 (0x0d) means the page is a leaf table b-tree page.
 * Any other value for the b-tree page type is an error.
 */
class PageHeader {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer;
	}

	get type() {
		return this.buffer.readUint8(0); // page type is the first byte at offset 0
	}

	get numberOfCells() {
		return this.buffer.readUint16BE(3); // page cells count is the 2 bytes at offset 3
	}
}

class Cell {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer;
		this.reader = new BinaryReader(buffer);
		this.recordSize = this.reader.readVarint();
		this.rowid = this.reader.readVarint();
		this.record = new Record(buffer.subarray(this.reader.pos));
	}
}

class Record {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer;
		this.reader = new BinaryReader(this.buffer);
		this.header = new RecordHeader(this.reader);
		this.body = new RecordBody(this.reader, this.header);
	}
}

class RecordHeader {
	/**
	 * @param {BinaryReader} buffer
	 */
	constructor(reader) {
		this.reader = reader;
		this.size = this.reader.readVarint();
		this.serialTypes = this.readSerialTypes();
	}

	readSerialTypes = () => {
		const result = [];

		while (this.reader.pos !== this.size) {
			result.push(this.reader.readVarint());
		}

		return result;
	};
}

class RecordBody {
	/**
	 * @param {BinaryReader} buffer
	 */
	constructor(reader, header) {
		this.reader = reader;
		this.columns = this.readColumns(header);
	}

	readColumns = (header) => {
		const result = [];

		for (const serialType of header.serialTypes) {
			const size = getSerialTypeSize(serialType);

			if (size === 1) {
				result.push(this.reader.buffer.readUint8(this.reader.pos));
				this.reader.pos += 1;
			} else {
				const bytes = this.reader.buffer.subarray(
					this.reader.pos,
					this.reader.pos + size,
				);
				const decoder = new TextDecoder();
				result.push(decoder.decode(bytes));
				this.reader.skip(size);
			}
		}

		return result;
	};
}

class BinaryReader {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer;
		this.pos = 0;
	}

	/**
	 * @param {number} n
	 */
	skip = (n) => {
		this.pos += n;
	};

	/**
	 * @param {number} n
	 */
	setPos = (n) => {
		this.pos = n;
	};

	readVarint = () => {
		let byte = this.buffer.readUint8(this.pos);
		this.pos += 1;

		let result = byte & 0x7f; // remove msb

		// check if msb is NOT set
		if (!((byte & 0x80) > 0)) {
			// if not, then we are done, just return the result
			return result;
		}

		// if it is set, then next byte must be read
		byte = this.buffer.readUint8(this.pos);
		this.pos += 1;

		result = (result << 7) | (byte & 0x7f);

		// TODO: handle more than two byte varint

		return result;
	};
}

function getSerialTypeSize(serialType) {
	if (serialType <= 4) {
		return serialType;
	}

	if (serialType === 5) {
		return 6;
	}

	if (serialType === 6 || serialType === 7) {
		return 8;
	}

	if (serialType === 8 || serialType === 9) {
		return 0;
	}

	// if even
	if (serialType % 2 === 0) {
		return (serialType - 12) / 2;
	}

	return (serialType - 13) / 2;
}
