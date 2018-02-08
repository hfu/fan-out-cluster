const cluster = require('cluster')
const Database = require('better-sqlite3')
const zlib = require('zlib')
const fs = require('fs-extra')

const params = {
  dry: false, verbose: true,
  n_threads: 4, report_steps: 10,
  minzoom: 13, maxzoom: 14, qcon: q => {return (q >= 15)},
  mbtiles: '/export/italy.mbtiles', 
  dir: '/home/unmap/xyz/ita1801'
}

const v2q = v => {
  for(let i = 0; true; i++) {
    if(v / (2 ** i) < 1) return i - 1
  }
}

const report = (count) => {
  console.log(count)
}

if (cluster.isMaster) {
  let count = 0
  let wrote = 0
  let skip = 0
  console.log(`master ${process.pid} is running`)
  for(let i = 0; i < params.n_threads; i++) cluster.fork({mod: i})
  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} exited.`)
  })
  for (const i in cluster.workers) {
    cluster.workers[i].on('message', msg => {
      count++
      wrote += msg.wrote
      skip += msg.skip
      if(count % params.report_steps == 0) {
        console.log(`${wrote} of ${count} from ${skip + count} written: ${msg.dst}`)
      }
    })
  }
} else {
  console.log(`worker ${process.pid} ${process.env.mod} started.`)
  const mod = Number(process.env.mod)
  const db = new Database(params.mbtiles, {readonly: true})
  for (let z = params.minzoom; z <= params.maxzoom; z++) {
    let x_last = -1
    for (const r of db.prepare(
      `SELECT * FROM tiles WHERE zoom_level == ${z}`).iterate()) {
      let skip = 0
      const x = r.tile_column
      if(x % params.n_threads === mod) {
        const y = (1 << z) - r.tile_row - 1
        let buf = r.tile_data
        const size_gz = buf.length
        buf = zlib.unzipSync(buf)
        const size_raw = buf.length
        const q = v2q(size_raw)
        if(params.qcon(q)) {
          let wrote = 0
          if(x !== x_last && !params.dry) fs.mkdirsSync(`${params.dir}/${z}/${x}`)
          const dst = `${params.dir}/${z}/${x}/${y}.pbf`
          if(!fs.existsSync(dst)&& !params.dry) {
            fs.writeFileSync(dst, buf)
            wrote = 1
          }
          process.send({
            mod: mod, 
            dst: `${params.dir}/${z}/${x}/${y}.pbf`,
            wrote: wrote, skip: skip,
            size_qz: size_gz, size_raw: size_raw
          })
          x_last = x
          skip = 0
        } else {
          skip += 1
        }
      } else {
        // data for other thread
      }
    }
  }
  process.exit(0)
}
