import { TableData } from '../src'

export default [
  {
    name: 'TableOne',
    primaryKey: 'id',
    rows: [
      {
        name: 'id',
        type: 'string',
        default: () => Math.floor(Math.random() * 1000000).toString(),
      },
      ['uniqueField', 'string', { unique: true }],
      ['uniqueAndOptionalField', 'string', { unique: true, optional: true }],
      ['optionalField', 'string', { optional: true }],
      ['regularField', 'string'],
    ],
  },
  {
    name: 'TableTwo',
    primaryKey: 'id',
    rows: [
      {
        name: 'id',
        type: 'string',
        // so this can be meaningfully lexographically sorted
        default: () => `${+new Date()}${Math.random()}`,
      },
      ['counterField', 'number', { unique: true }],
    ],
  },
  {
    name: 'TableThree',
    primaryKey: 'id',
    rows: [
      {
        name: 'id',
        type: 'string',
      },
      {
        name: 'optionalField',
        type: 'string',
        optional: true,
      },
    ],
  },
  {
    name: 'TableFour',
    primaryKey: 'id',
    rows: [
      ['id', 'string'],
      ['relation1Id', 'string', { optional: true }],
      {
        name: 'relation1',
        type: 'string',
        relation: {
          foreignField: 'id',
          localField: 'relation1Id',
          foreignTable: 'Relation1',
        },
      },
    ],
  },
  {
    name: 'Relation1',
    primaryKey: 'id',
    rows: [
      ['id', 'string'],
      ['relation2Id', 'string'],
      {
        name: 'relation2',
        type: 'string',
        relation: {
          foreignField: 'id',
          localField: 'relation2Id',
          foreignTable: 'Relation2',
        },
      },
    ],
  },
  {
    name: 'Relation2',
    primaryKey: 'id',
    rows: [['id', 'string']],
  },
  {
    name: 'Table5',
    primaryKey: 'id',
    rows: [
      ['id', 'number'],
      ['optionalField', 'boolean', { optional: true }],
    ],
  },
  {
    name: 'Table6',
    primaryKey: 'id',
    rows: [
      ['id', 'number'],
      ['boolField', 'boolean'],
      ['stringField', 'string'],
    ],
  },
  {
    name: 'Table7',
    primaryKey: 'id',
    rows: [
      ['id', 'number'],
      ['boolField', 'boolean'],
      ['stringField', 'string'],
      ['bigintField', 'bigint'],
      ['optionalField', 'string', { optional: true }],
    ],
  },
  {
    name: 'bigint',
    rows: [
      ['id', 'bigint']
    ]
  },
  {
    name: 'IndexTable',
    primaryKey: 'id',
    rows: [
      ['id', 'number'],
      ['id2', 'number'],
    ],
  },
] as TableData[]
