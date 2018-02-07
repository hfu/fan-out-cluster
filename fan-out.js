const cluster = require('cluster')
const Database = require('better-sqlite3')
const zlib = require('zlib')
const fs = require('fs-extra')

const params = {
  dry: false, verbose: true,
  n_threads: 4, report_steps: 10,
  minzoom: 0, maxzoom: 14, minq: 10,
  mbtiles: './linestrings.mbtiles', 
  dir: '/home/unmap/xyz/linestrings'
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
  console.log(`master ${process.pid} is running`)
  for(let i = 0; i < params.n_threads; i++) cluster.fork({mod: i})
  cluster.on('exit', (worker, code, signal) => {
    console.log(`worker ${worker.process.pid} exited.`)
  })
  for (const i in cluster.workers) {
    cluster.workers[i].on('message', message => {
      count++
      if(count % params.report_steps == 0) {
        console.log(`${count}: ${message}`)
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
      const x = r.tile_column
      if(x % params.n_threads === mod) {
        const y = (1 << z) - r.tile_row - 1
        let buf = r.tile_data
        const size_gz = buf.length
        buf = zlib.unzipSync(buf)
        const size_raw = buf.length
        const q = v2q(size_raw)
        if(q >= params.minq) {
          if(x !== x_last && !params.dry) fs.mkdirsSync(`${params.dir}/${z}/${x}`)
          const dst = `${params.dir}/${z}/${x}/${y}.pbf`
          if(!fs.existsSync(dst)&& !params.dry) {
            fs.writeFileSync(dst, buf)
          }
          process.send(`[${mod}] ${params.dir}/${z}/${x}/${y} ${size_gz} => ${size_raw}`)
          x_last = x
        }
      } else {
      }
    }
  }
  process.exit(0)
}
