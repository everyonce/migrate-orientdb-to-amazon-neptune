// EXAMPLE command:
//    node process.js source-data/orientdb-export-pets-db.gz pets
//         ^ this     ^ orient export json-gzipped           ^ simple name for dataset

//#region includes
const {chain}  = require('stream-chain');
const {parser} = require('stream-json');
const {pick}   = require('stream-json/filters/Pick');
const {streamArray} = require('stream-json/streamers/StreamArray');
const { once } = require('events');

const csvWriter = require('csv-write-stream');
const fs   = require('fs');
const zlib = require('zlib');
//#endregion includes

//#region constants
// CONVERSION CONSTANTS - you might need to change based on which version of OrientDB, etc.
const orientToNeptuneTypeMapping = {
  //OrientDB Type ID (number) - Neptune CSV/Gremlin Type (string)
  0: "Bool",
  1: "Integer",
  2: "Short",
  3: "Long",
  4: "Float",
  5: "Double",
  6: "Date",
  7: "String",
  19: "Date"
/*  Not implemented yet
     8	Binary	BINARY	Can contain any value as byte array	byte[]	0 2,147,483,647	String
     9	Embedded	EMBEDDED	The Record is contained inside the owner. The contained Record has no Record ID	ORecord-	ORecord
     10	Embedded list	EMBEDDEDLIST	The Records are contained inside the owner. The contained records have no Record ID's and are reachable only by navigating the owner record	List<Object>	0 41,000,000 items	String
     11	Embedded set	EMBEDDEDSET	The Records are contained inside the owner. The contained Records have no Record ID and are reachable only by navigating the owner record	Set<Object>	0 41,000,000 items	String
     12	Embedded map	EMBEDDEDMAP	The Records are contained inside the owner as values of the entries, while the keys can only be Strings. The contained ords e no Record IDs and are reachable only by navigating the owner Record	Map<String, ORecord>	0 41,000,000 items	Collection<? extends ORecord<?>>, String
     13	Link	LINK	Link to another Record. It's a common one-to-one relationship	ORID, <? extends ORecord>	1:-1 32767:2^63-1	String
     14	Link list	LINKLIST	Links to other Records. It's a common one-to-many relationship where only the Record IDs are stored	List<? extends ORecord	0 41,000,000 items	String
     15	Link set	LINKSET	Links to other Records. It's a common one-to-many relationship	Set<? extends ORecord>	0 41,000,000 items	Collection<? extends ORecord>, String
     16	Link map	LINKMAP	Links to other Records as value of the entries, while keys can only be Strings. It's a common One-to-Many Relationship. Only the Record IDs are stored	Map<String,    ? extends Record>	0 41,000,000 items	String
     17	Byte	BYTE	Single byte. Useful to store small 8-bit signed integers	java.lang.Byte or byte	-128 +127	Any Number, String
     18	Transient	TRANSIENT	Any value not stored on database			 
     20	Custom	CUSTOM	used to store a custom type providing the marshall and unmarshall methods	OSerializableStream	0 X	-
     21	Decimal	DECIMAL	Decimal numbers without rounding	java.math.BigDecimal	? ?	Any Number, String
     22	LinkBag	LINKBAG	List of Record IDs as spec RidBag	ORidBag	? ?	-
     23	Any	ANY	Not determinated type, used to specify Collections of mixed type, and null	- 
     */
  };
// Ignore these classes to not output files
const classesToIgnore=['ORole','OUser','Blockinground-0','_studio'];
const fileName = process.argv[2];
const userClassName = process.argv[3]?process.argv[3]:fileName.split(".")[0];
//#endregion constants

//#region processing arrays
let sourceClasses=[];                 // class definitions from source file
let outputClassSummary={};            // counts for classes
let dataCount=0;                      // absolute record count from input file
let csvWriters = [];                  // array for output CSV writers
let writerPromises = [];              // promises for output streams array
let writerFiles = [];                 // output stream array
let readyPromiseResolve = () => {};   // once writer promises are ready
let readyPromise = new Promise((resolve) => {
  readyPromiseResolve = resolve;
});
let classMapping = [];                // class name to writer array index
//#endregion processing arrays


let main = () => {return new Promise((resolve, reject) => {
  const dataPipeline = chain([
    fs.createReadStream(fileName),
    zlib.createGunzip(),
    parser(),
    pick({filter: 'records'}),
    streamArray()
  ]);

  //
  // ON DATA
  //
  dataPipeline.on('data', (x) => {
    ++dataCount;
    if (dataCount ==2) {
      // the second record has our core class definitions/schemas
      setupOutputClasses(x);
      resolve();  //main
    }
    thisType = (x.value["@class"]||x.value["name"])
    if (thisType && !classesToIgnore.includes(thisType)) {
      newClassName=getNewClassName(x);
      if (dataCount%10000==0)
          //console.log("Processed: "+discoveryCounter);
          process.stdout.write('.');
      if (outputClassSummary[newClassName]) {
          outputClassSummary[newClassName]++; }
      else {
          outputClassSummary[newClassName]=1;
      }
      csvWriters[classMapping[thisType]].write( getRow(x.value), err => { if (err) { console.error(err); } });  
      
      //evalRowHeaders(x);
    }
  })

  //
  // ON END
  //
  dataPipeline.on('end', async () => {
    process.stdout.write("\n");
    console.log(`looping through ${csvWriters.length} output file streams`);
    console.log(`Found these classes within a total ${dataCount} records: ${JSON.stringify(outputClassSummary, null, 5)}`);
    csvWriters.forEach((x) => {   
      console.log(` >> ${JSON.stringify(x["_writableState"]["finished"])}`)
      x.end();
    });

  });

});
};

/* Promise order:
     1 - main, once we've discovered output classes
     2 - readyPromise, once the output class writers have been initialized
     3 - writerPromises, once the writers have completed (at the end) */
main()
    .then(async () => {
      await readyPromise;
      console.log("ready promise done");
    })
    .then(async () => {
      console.log(`Waiting for writer promises (${writerPromises.length})`);
      await Promise.all(writerPromises);
      console.log("writer promises all done");
    })
    .then(() => {
      console.log("Time to exit program!");
        process.exit(0);
    })
    .catch(err => {
        console.log(err); // Writes to stderr
        process.exit(1);
    });

//#region helper functions
  
/* Neptune CSV Looks like:

VERTEX EXAMPLE CSV
~id, name:String, age:Int, lang:String, interests:String[], ~label
v1, "marko", 29, , "sailing;graphs", person
v2, "lop", , "java", , software

EDGE EXAMPLE CSV
~id, ~from, ~to, ~label, weight:Double
e1, v1, v2, created, 0.4
*/

function getRowHeader(className) {
  //console.log(`Getting row headers for ${className}`);
  thisClass = sourceClasses[className];
  //console.log(JSON.stringify(thisClass));
  let header=["~label", "~id"];
  if (thisClass["superClass"] && thisClass["superClass"]=="E") 
    header.push("~from", "~to");
  thisClass["properties"].forEach((p) => {
    header.push(p.name + ":" + orientToNeptuneTypeMapping[p["type"]]);
  });
  //console.log(`HEADER for ${className}: ${JSON.stringify(header)}`);
  return header;
}
function getRow(rowObject) {
  let thisClassName=rowObject["@class"] || rowObject["name"];
  let thisClass = sourceClasses[thisClassName];
  let rowArray=[thisClassName, formatID(rowObject["@rid"])];
  if (thisClass["superClass"] && thisClass["superClass"]=="E") 
    rowArray.push(formatID(rowObject["out"]), formatID(rowObject["in"]));
  sourceClasses[thisClassName]["properties"].forEach((p) => {
    let thisValue = rowObject[p.name]??"";
    if ((p.type==6 || p.type==19) && thisValue != "") {    // DATES require conversion     
      try {
        var d = new Date(thisValue);
        thisValue = d.toISOString();
      } catch (e) {
        try {
          var d = new Date(thisValue/1000);
          thisValue = d.toISOString();
        } catch (e) {
          try {
            var d = new Date(thisValue/1000000);
            thisValue = d.toISOString();
          } catch (e) {
            process.stdout.write("\n");
            console.log("Problem Decoding Date:", d, " -> ISO String.  Value in JSON: ", thisValue, " - ", p.name, ":", p.type, " field.");
          }
        }
      }
    }
    rowArray.push(thisValue);
  });
  return rowArray;
}
function formatID(id) {
  return `${userClassName}-${id??""}`.replaceAll('#','');
}
const isEdge = (x) => {return x.value["in"] && x.value["out"]?'E':'V';}
const getNewClassName=(x)=> {return `${x.value["@class"]||x.value["name"]}-${isEdge(x)}`;}

const setupOutputClasses = (x) => {
  x.value.classes.forEach( (c) => {
    console.log(`Checking class: ${c["name"] || JSON.stringify(c)}`);
    if (c["superClass"]) {
      console.log(`Defining class: ${c["name"] || JSON.stringify(c)}`);
      let thisClass = {
        name: c["name"],
        superClass: c["superClass"],
        properties: []
      };
      c["properties"].forEach((p) => {
        if ((!p["type"] && p["type"]!==0) || !orientToNeptuneTypeMapping[p["type"]]) 
          console.log("WARNING: field ", p["name"], " has unsupported type ", p["type"]);
        thisClass.properties.push(
          {
            name: p["name"],
            type: p["type"]
          }
        )
      });
      sourceClasses[c["name"]]=thisClass;

      if (!classesToIgnore.includes(c["name"])) {
        // CREATE OUTPUT WRITER FOR THIS CLASS
        let csvIndex= csvWriters.length;
        classMapping[c["name"]]=csvIndex;
        console.log(`Creating output file for class: ${c["name"] || JSON.stringify(c)}  [csvIndex=${csvIndex}]`);
        csvWriters.push(csvWriter({ headers: getRowHeader(c["name"])}));
        writerFiles[csvIndex] = fs.createWriteStream(`neptune-${userClassName}-${c["name"]}.csv.gz`);
        csvWriters[csvIndex]
          .pipe(zlib.createGzip())
          .pipe(writerFiles[csvIndex]);
        writerPromises.push(once(writerFiles[csvIndex], 'finish').then(()=>{console.log(`csvWriter has finished: ${c["name"]}`)}));
      }
    }
  });
  readyPromiseResolve();
}

//#endregion helper functions