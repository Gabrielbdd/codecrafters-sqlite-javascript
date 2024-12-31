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
		this.page = new Page(buffer);
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
}

class Page {
	/**
	 * @param {Buffer} buffer
	 */
	constructor(buffer) {
		this.buffer = buffer.subarray(100); // assume this is the first page, so ignore the first 100 bytes as it is for the database header
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
			result.push(this.buffer.readUint8(pointer - this.buffer.byteOffset)); // subtract buffer offset because the offset is meant to be referenced from the start of the file
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
