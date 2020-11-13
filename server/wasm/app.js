'use strict'

const Sourmash = require('sourmash/sourmash.js')

var FileReadStream = require('filestream/read')
var FASTQStream = require('fastqstream').FASTQStream
var Fasta = require('fasta-parser')

var zlib = require('zlib')
var peek = require('peek-stream')
const through = require('through2')
const pumpify = require('pumpify')

const $dragContainer = document.querySelector('#drag-container')
const $progressBar = document.querySelector('#progress-bar')
const $downloadButton = document.querySelector('#download_btn')
const $resultsContainer = document.querySelector('#results-container')

let fileSize = 0
let fileName
let loadedFile = 0

/* ===========================================================================
   Files handling
   =========================================================================== */

const resetProgress = () => {
  $downloadButton.disabled = true
  $progressBar.style.transform = 'translateX(-100%)'
}

/* Drag & Drop
   =========================================================================== */

const onDragEnter = () => $dragContainer.classList.add('dragging')

const onDragLeave = () => $dragContainer.classList.remove('dragging')

function isFASTA (data) {
  return data.toString().charAt(0) === '>'
}

function isFASTQ (data) {
  return data.toString().charAt(0) === '@'
}

function isGzip (data) {
  return (data[0] === 31) && (data[1] === 139)
}

function GzipParser () {
  return peek(function (data, swap) {
    if (isGzip(data)) return swap(null, new zlib.Unzip())
    else return swap(null, through())
  })
}

function FASTParser () {
  return peek(function (data, swap) {
    if (isFASTA(data)) return swap(null, pumpify.obj(Fasta(), jsParse()))
    if (isFASTQ(data)) return swap(null, new FASTQStream())

    // we do not know - bail
    swap(new Error('No parser available'))
  })
}

function onDrop (event) {
  onDragLeave()
  event.preventDefault()
  resetProgress()

  const dt = event.dataTransfer
  const filesDropped = dt.files

  var file = filesDropped[0]

  var reader = new FileReadStream(file)

  fileSize = file.size
  fileName = file.name

  reader.reader.onprogress = (data) => {
    loadedFile += data.loaded
    let percent = 100 - ((loadedFile / fileSize) * 100)

    $progressBar.style.transform = `translateX(${-percent}%)`
  }

  //var mh = new Sourmash.KmerMinHash(0, 21, false, false, false, 42, 2000, false)
  var params = new Sourmash.ComputeParameters();
  params.scaled = BigInt(2000);
  var sig = new Sourmash.Signature(params);

  var seqparser = new FASTParser()
  var compressedparser = new GzipParser()

  seqparser
    .on('data', function (data) {
      sig.add_sequence_js(data.seq)
    })
    .on('end', function (data) {
      const jsonData = sig.to_json()
      const file = new window.Blob([jsonData], { type: 'application/octet-binary' })
      const url = window.URL.createObjectURL(file)

      const link = document.createElement('a')
      link.setAttribute('href', url)
      link.setAttribute('download', fileName + '.sig')

      document.querySelectorAll('#download_btn a').forEach(e => e.parentNode.removeChild(e))

      $downloadButton.appendChild(link)
      $downloadButton.addEventListener('click', () => { link.click() })
      $downloadButton.disabled = false

      $progressBar.style.transform = `translateX(0%)`

      fetch("/gather", {
          method: 'POST',
          body: jsonData,
          headers: {
            'Content-Type': 'application/json'
          }
        })
        .then(response => response.json())
        .then(data => {
            const table = document.createElement("table")

            let head = table.createTHead();
            for (const cname of ["overlap", "p_query", "p_match", "name"]) {
              let columnName = document.createElement("th");
              let newText = document.createTextNode(cname);
              columnName.appendChild(newText);
              head.appendChild(columnName);
            }
            
            const baseURL = "https://www.ncbi.nlm.nih.gov/assembly/";
            const fmt = function(n, decimals) {
               return n.toFixed(decimals).replace(/\.?0*$/, ""); 
            };
            
            const bp_fmt = function(bp) {
              if (bp < 500) {
                  return fmt(bp, 0) + ' bp'
              } else if (bp <= 500e3) {
                  return fmt(bp / 1e3, 1) + ' Kbp'
              } else if (bp < 500e6) {
                  return fmt(bp / 1e6, 1) + ' Mbp'
              } else if (bp < 500e9) {
                  return fmt(bp / 1e9, 1) + ' Gbp'
              }
              return '???'
            };

            // TODO: save data for CSV formatting before this loop,
            // will change data from now on for screen formatting
            for (const rmatch of data) {
              let newRow = table.insertRow(-1);

              // TODO: format overlap (Kbp, Mbp)
              rmatch['intersect_bp'] = bp_fmt(rmatch['intersect_bp'])

              rmatch['f_orig_query'] = fmt(rmatch['f_orig_query'] * 100, 1) + "%";
              rmatch['f_match'] = fmt(rmatch['f_match'] * 100, 1) + "%";
              rmatch['average_abund'] = fmt(rmatch['average_abund'], 1)

              let newCell;
              let newText;
              for (const cname of ["intersect_bp", "f_orig_query", "f_match"]) {
								newCell = newRow.insertCell(-1);
								newText = document.createTextNode(rmatch[cname]);
								newCell.appendChild(newText);
              }

              newCell = newRow.insertCell(-1);
              let acc = new String(rmatch['filename']).substring(rmatch["filename"].lastIndexOf("/") + 1);
              acc = acc.substring(0, acc.length - 4);
              const link = document.createElement('a');
              link.setAttribute('href', baseURL + acc);
              newText = document.createTextNode(rmatch['name']);
              link.appendChild(newText);
              newCell.appendChild(link);
            }

            while ($resultsContainer.firstChild) {
                $resultsContainer.removeChild($resultsContainer.firstChild);
            }

            $resultsContainer.appendChild(table);
        });
    })

  switch (file.type) {
    case 'application/gzip':
      reader.pipe(new zlib.Unzip()).pipe(seqparser)
      break
    default:
      reader.pipe(compressedparser).pipe(seqparser)
      break
  }
}

function jsParse () {
  var stream = through.obj(transform, flush)
  return stream
  function transform (obj, enc, next) {
    if (Buffer.isBuffer(obj)) { obj = obj.toString() }
    JSON.parse(obj)
    this.push(JSON.parse(obj))
    next()
  }
  function flush () { this.push(null) }
}

/* ===========================================================================
   Boot the app
   =========================================================================== */

const startApplication = () => {
  // Setup event listeners
  $dragContainer.addEventListener('dragenter', onDragEnter)
  $dragContainer.addEventListener('dragover', onDragEnter)
  $dragContainer.addEventListener('drop', onDrop)
  $dragContainer.addEventListener('dragleave', onDragLeave)
}

startApplication()
