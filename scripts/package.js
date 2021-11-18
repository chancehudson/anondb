const fs = require('fs')
const path = require('path')

const _package = require('../package.json')
_package.scripts = {}

const outpath = path.join(__dirname, '../dist/package.json')
fs.writeFileSync(outpath, JSON.stringify(_package, null, 2))

const lockpath = path.join(__dirname, '../package-lock.json')
const newlockpath = path.join(__dirname, '../dist/package-lock.json')
fs.copyFileSync(lockpath, newlockpath)


