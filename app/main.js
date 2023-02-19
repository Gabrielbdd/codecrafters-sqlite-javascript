import { open } from "fs/promises";

const databaseFilePath = process.argv[2];
const command = process.argv[3];

if (command === ".dbinfo") {
  const databaseFileHandler = await open(databaseFilePath, "r");

  const { buffer } = await databaseFileHandler.read({
    length: 108,
    position: 0,
    buffer: Buffer.alloc(108),
  });
  const pageSize = buffer.readUInt16BE(16); // page size is 2 bytes starting at offset 16
  const numberOfTables = buffer.readUInt16BE(103)

  /**
    * The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages. All multibyte values in the page header are big-endian. The b-tree page header is composed of the following fields:
    * The one-byte flag at offset 0 indicating the b-tree page type.
    * A value of 2 (0x02) means the page is an interior index b-tree page.
    * A value of 5 (0x05) means the page is an interior table b-tree page.
    * A value of 10 (0x0a) means the page is a leaf index b-tree page.
    * A value of 13 (0x0d) means the page is a leaf table b-tree page.
    * Any other value for the b-tree page type is an error. 
    */

  console.log(`database page size: ${pageSize}`);
  console.log(`number of tables: ${numberOfTables}`);
} else {
  throw `Unknown command ${command}`;
}
