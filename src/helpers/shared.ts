import { SchemaTable, Schema, Relation, validTypes } from '../types'

export function checkSchema(schema: Schema) {
  const tableNames = {} as any
  for (const [key, val] of Object.entries(schema)) {
    if (tableNames[key]) {
      throw new Error(`Duplicate table name: "${key}"`)
    }
    tableNames[key] = true
    const rowNames = {} as any
    for (const row of val?.rows ?? []) {
      // check unique row name
      if (rowNames[row.name]) {
        throw new Error(`Duplicate row in table "${key}": "${row.name}"`)
      }
      rowNames[row.name] = true
      // check that type is valid
      if (validTypes.indexOf(row.type) === -1) {
        throw new Error(`Invalid type for row "${row.name}" in table "${key}": "${row.type}"`)
      }
      // check that default value is valid
      if (typeof row.default === 'function') {
        if (typeof row.default() !== row.type) {
          throw new Error(`Default function for row "${row.name}" in table "${key}" returns wrong type`)
        }
      } else if (typeof row.default !== 'undefined' && typeof row.default !== row.type) {
        throw new Error(`Default value for row "${row.name}" in table "${key}" does not match row type (got "${typeof row.default}" expected "${row.type}"`)
      }
    }
  }
}

async function loadIncludedModels(
  models: any[],
  relation: Relation & { name: string },
  findMany: Function,
  include?: any,
) {
  const values = models.map(model => model[relation.localField])
  // load relevant submodels
  const submodels = await findMany(relation.foreignTable, {
    where: {
      [relation.foreignField]: values,
    },
    include: include as any, // load subrelations if needed
  })
  // key the submodels by their relation field
  const keyedSubmodels = {}
  for (const submodel of submodels) {
    // assign to the models
    keyedSubmodels[submodel[relation.foreignField]] = submodel
  }
  // Assign submodel onto model
  for (const model of models) {
    const submodel = keyedSubmodels[model[relation.localField]]
    Object.assign(model, {
      [relation.name]: submodel || null,
    })
  }
}

export async function loadIncluded(
  collection: string,
  options: {
    models: any[]
    include?: any
    findMany: Function
    table: SchemaTable
  },
) {
  const { models, include, table, findMany } = options
  if (!include || !models || !models.length) return
  if (!table) throw new Error(`Unable to find table ${collection} in schema`)
  for (const key of Object.keys(include)) {
    const relation = table.relations[key]
    if (!relation) {
      throw new Error(`Unable to find relation ${key} in ${collection}`)
    }
    if (include[key]) {
      await loadIncludedModels(
        models,
        relation,
        findMany,
        typeof include[key] === 'object' ? include[key] : undefined,
      )
    }
  }
}
